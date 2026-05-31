'use strict';

var crypto       = require('crypto');
var util         = require('util');
var utils        = require('./utils');
var BaseConsumer = require('./base_consumer');
var Kafka        = require('./index');
var promiseUtils = require('./promise-utils');

// KIP-848: next-gen consumer group. Membership is driven by a single
// ConsumerGroupHeartbeat RPC (apiKey 68) with broker-side ("server") assignment.
// The client only reconciles (revoke-before-acquire) the target assignment the
// coordinator hands back; it never computes assignments itself. Consumption,
// offset commit and offset fetch all reuse the inherited BaseConsumer machinery.

function ConsumerGroup(options) {
    this.options = utils.defaultsDeep(options || {}, {
        groupId: 'no-kafka-consumer-group',
        sessionTimeout: 45000,   // sent to broker as rebalanceTimeoutMs
        assignor: 'uniform',     // server-side assignor: 'uniform' or 'range'
        startingOffset: Kafka.LATEST_OFFSET
    });

    BaseConsumer.call(this, this.options);

    if (!this.client.validateId(this.options.groupId)) {
        throw new Error('Invalid groupId. Kafka IDs may not contain the following characters: ?:,"');
    }

    // KIP-848 v1 (KIP-1082): the client generates its own UUID member id
    this.memberId = crypto.randomUUID();
    this.memberEpoch = 0;
    this.heartbeatIntervalMs = 5000;

    this._topics = [];
    this._handler = null;
    this._onPartitionsRevoked = null;
    this._onPartitionsAssigned = null;

    // currently owned partitions: map "topic:partition" -> true
    this.owned = {};
    // whether the owned set must be reported in the next heartbeat
    this._ownedChanged = false;

    this._closed = false;
    this._heartbeatTimeout = null;
    this._heartbeatPromise = Promise.resolve();
}

module.exports = ConsumerGroup;

util.inherits(ConsumerGroup, BaseConsumer);

/**
 * Initialize ConsumerGroup
 *
 * @param  {Object} spec  { topics: [string], handler: function(messageSet, topic, partition, highwaterMarkOffset),
 *                          onPartitionsRevoked: function([{topic, partition}]),
 *                          onPartitionsAssigned: function([{topic, partition}]) }
 * @return {Promise}
 */
ConsumerGroup.prototype.init = function (spec) {
    var self = this;

    if (!spec || typeof spec.handler !== 'function') {
        return Promise.reject(new Error('ConsumerGroup requires a handler function'));
    }
    if (!spec.topics || spec.topics.length === 0) {
        return Promise.reject(new Error('ConsumerGroup requires at least one topic'));
    }

    self._topics = Array.isArray(spec.topics) ? spec.topics : [spec.topics];
    self._handler = spec.handler;
    if (typeof spec.onPartitionsRevoked === 'function') {
        self._onPartitionsRevoked = spec.onPartitionsRevoked;
    }
    if (typeof spec.onPartitionsAssigned === 'function') {
        self._onPartitionsAssigned = spec.onPartitionsAssigned;
    }

    return BaseConsumer.prototype.init.call(self).then(function () {
        // load metadata so topicId <-> name maps are populated (the wire uses topic IDs)
        return self.client.updateMetadata(self._topics);
    }).then(function () {
        return self._join();
    });
};

/**
 * Join the group: find the coordinator, send the initial heartbeat, start the loop
 */
ConsumerGroup.prototype._join = function () {
    var self = this;

    return (function _tryJoin(attempt) {
        attempt = attempt || 0;

        return self.client.updateGroupCoordinator(self.options.groupId).then(function () {
            // initial heartbeat: memberEpoch=0, declare topics + assignor, empty owned set
            return self.client.consumerGroupHeartbeatRequest(
                self.options.groupId,
                self.memberId,
                0,
                self._topics,
                self.options.assignor,
                [],
                self.options.sessionTimeout
            );
        })
        .then(function (response) {
            self.memberId = response.memberId || self.memberId;
            self.memberEpoch = response.memberEpoch;
            self.heartbeatIntervalMs = response.heartbeatIntervalMs;
            self.client.log('Joined consumer group', self.options.groupId, 'as', self.memberId, 'epoch', self.memberEpoch);

            return self._maybeReconcile(response.assignment).then(function () {
                self._heartbeatPromise = self._heartbeat();
            });
        })
        .catch(function (err) {
            var maxAttempts, warnAfter, retryDelay;
            if (self._closed) { return null; }
            maxAttempts = self.options.maxJoinAttempts || Infinity;
            warnAfter = 10;
            if (attempt >= maxAttempts) {
                self.client.error('Consumer group join exceeded max attempts (' + maxAttempts + '), giving up');
                throw err;
            }
            if (attempt === warnAfter) {
                self.client.warn('Consumer group join has failed ' + warnAfter + ' times, still retrying...');
            }
            retryDelay = Math.min(1000 * Math.pow(2, attempt), 30000);
            self.client.error('Consumer group join failed (attempt ' + (attempt + 1) + '):', err);
            return promiseUtils.delay(retryDelay).then(function () {
                if (self._closed) { return null; }
                return _tryJoin(attempt + 1);
            });
        });
    }());
};

/**
 * Heartbeat loop: sends ConsumerGroupHeartbeat at the interval returned by the coordinator
 */
ConsumerGroup.prototype._heartbeat = function () {
    var self = this;
    var reportOwned = self._ownedChanged ? self._buildOwnedTopicPartitions() : null;

    return self.client.consumerGroupHeartbeatRequest(
        self.options.groupId,
        self.memberId,
        self.memberEpoch,
        null, // subscribedTopicNames unchanged
        null, // serverAssignor unchanged
        reportOwned
    )
    .then(function (response) {
        if (self._closed) { return undefined; }

        self.memberEpoch = response.memberEpoch;
        self.heartbeatIntervalMs = response.heartbeatIntervalMs;
        if (reportOwned !== null) {
            self._ownedChanged = false; // acknowledged our current owned set
        }

        return self._maybeReconcile(response.assignment).then(function () {
            self._scheduleHeartbeat();
        });
    })
    .catch(function (err) {
        if (self._closed) { return undefined; }

        // KIP-848: the member must abandon its partitions and rejoin from scratch
        if (err && (err.code === 'UnknownMemberId' || err.code === 'FencedMemberEpoch')) {
            self.client.warn('Consumer group heartbeat rejected (' + err.code + '), rejoining');
            if (err.code === 'UnknownMemberId') {
                self.memberId = crypto.randomUUID();
            }
            self.memberEpoch = 0;
            self._revokeAll();
            return self._join();
        }

        self.client.error('Consumer group heartbeat failed:', err);
        self._scheduleHeartbeat();
        return undefined;
    });
};

ConsumerGroup.prototype._scheduleHeartbeat = function () {
    var self = this;
    if (self._closed) { return; }
    self._heartbeatTimeout = setTimeout(function () {
        if (self._closed) { return; }
        self._heartbeatPromise = self._heartbeat();
    }, self.heartbeatIntervalMs);
};

/**
 * Reconcile toward the target assignment from the coordinator (revoke-before-acquire).
 * `assignment` is the nullable struct { _present, topicPartitions }; null means "unchanged".
 */
ConsumerGroup.prototype._maybeReconcile = function (assignment) {
    var self = this;

    if (!assignment) {
        return Promise.resolve(); // no assignment change in this heartbeat
    }

    return self._reconcile(assignment.topicPartitions || []);
};

ConsumerGroup.prototype._reconcile = function (targetTopicPartitions) {
    var self = this, targetSet = {}, revoked = [], added = [], offsetRequests = [], i;

    // translate target (topic IDs) -> "topic:partition" set
    targetTopicPartitions.forEach(function (tp) {
        var topic = self.client.topicNames[tp.topicId];
        if (!topic) { return; } // unknown topicId (not one we subscribed to)
        tp.partitions.forEach(function (p) {
            targetSet[topic + ':' + p] = topic;
        });
    });

    // revoked = owned - target
    Object.keys(self.owned).forEach(function (k) {
        var parts;
        if (!targetSet[k]) {
            parts = k.split(':');
            revoked.push({ topic: parts[0], partition: Number(parts[1]) });
        }
    });

    // added = target - owned
    Object.keys(targetSet).forEach(function (k) {
        var idx;
        if (!self.owned[k]) {
            idx = k.lastIndexOf(':');
            added.push({ topic: k.substring(0, idx), partition: Number(k.substring(idx + 1)) });
        }
    });

    if (revoked.length === 0 && added.length === 0) {
        return Promise.resolve();
    }

    // 1. Revoke first, for safe handoff
    for (i = 0; i < revoked.length; i++) {
        self.unsubscribe(revoked[i].topic, revoked[i].partition);
        delete self.owned[revoked[i].topic + ':' + revoked[i].partition];
    }
    if (revoked.length > 0) {
        self.client.log('Consumer group: revoked', revoked.length, 'partitions');
        if (self._onPartitionsRevoked) {
            self._onPartitionsRevoked(revoked);
        }
    }

    self._ownedChanged = true;

    if (added.length === 0) {
        return Promise.resolve();
    }

    // 2. Acquire: resume from committed offsets, then subscribe.
    //    owned is updated only AFTER subscribe succeeds, so a concurrent heartbeat
    //    never reports a partition as acquired before it is actually being consumed
    //    (preserves the revoke-before-acquire safety guarantee).
    for (i = 0; i < added.length; i++) {
        offsetRequests.push({ topic: added[i].topic, partition: added[i].partition });
    }

    return self.client.updateMetadata(self._topics).then(function () {
        return self.fetchOffset(offsetRequests).then(function (results) {
            return Promise.all(results.map(function (p) {
                var options = { offset: p.offset };
                if (p.error || p.offset < 0) {
                    self.client.warn('No committed offset available, using startingOffset for', p.topic + ':' + p.partition);
                    options = { time: self.options.startingOffset };
                }
                return self.subscribe(p.topic, p.partition, options, self._handler).then(function () {
                    self.owned[p.topic + ':' + p.partition] = true;
                }).catch(function (err) {
                    self.client.error('Failed to subscribe to', p.topic + ':' + p.partition, err);
                });
            }));
        });
    }).then(function () {
        self._ownedChanged = true; // report the post-acquire owned set in the next heartbeat
        self.client.log('Consumer group: assigned', added.length, 'partitions');
        if (self._onPartitionsAssigned) {
            self._onPartitionsAssigned(added);
        }
    });
};

/**
 * Locally drop all owned partitions (used when fenced — partitions are abandoned before rejoin)
 */
ConsumerGroup.prototype._revokeAll = function () {
    var self = this, revoked = [];
    Object.keys(self.owned).forEach(function (k) {
        var parts = k.split(':');
        revoked.push({ topic: parts[0], partition: Number(parts[1]) });
        self.unsubscribe(parts[0], Number(parts[1]));
    });
    self.owned = {};
    self._ownedChanged = false;
    if (revoked.length > 0 && self._onPartitionsRevoked) {
        self._onPartitionsRevoked(revoked);
    }
};

/**
 * Build the owned set in wire form: [{topicId, partitions}]
 */
ConsumerGroup.prototype._buildOwnedTopicPartitions = function () {
    var self = this, byTopic = {}, result = [];

    Object.keys(self.owned).forEach(function (k) {
        var idx = k.lastIndexOf(':');
        var topic = k.substring(0, idx);
        var partition = Number(k.substring(idx + 1));
        if (!byTopic[topic]) { byTopic[topic] = []; }
        byTopic[topic].push(partition);
    });

    Object.keys(byTopic).forEach(function (topic) {
        result.push({
            topicId: self.client.topicIds[topic] || null,
            partitions: byTopic[topic]
        });
    });

    return result;
};

ConsumerGroup.prototype._prepareOffsetRequest = function (type, commits) {
    var grouped;
    if (!Array.isArray(commits)) {
        commits = [commits];
    }

    grouped = utils.groupBy(commits, function (c) { return c.topic; });
    return Object.keys(grouped).map(function (k) {
        var v = grouped[k];
        return {
            topicName: k,
            partitions: type === 'fetch' ? v.map(function (p) { return p.partition; }) : v.map(function (p) {
                return {
                    partition: p.partition,
                    offset: p.offset + 1, // commit the next offset to consume
                    metadata: p.metadata
                };
            })
        };
    });
};

/**
 * Commit processed offsets. KIP-848 carries the member epoch in the generation slot.
 *
 * @param  {Object|Array} commits [{topic, partition, offset, metadata}]
 * @return {Promise}
 */
ConsumerGroup.prototype.commitOffset = function (commits) {
    var self = this;
    return self.client.offsetCommitRequestV2(self.options.groupId, self.memberId, self.memberEpoch,
        self._prepareOffsetRequest('commit', commits));
};

/**
 * Fetch committed offsets [{topic, partition}]
 *
 * @param  {Object|Array} commits
 * @return {Promise}
 */
ConsumerGroup.prototype.fetchOffset = function (commits) {
    var self = this;
    return self.client.offsetFetchRequestV1(self.options.groupId, self._prepareOffsetRequest('fetch', commits));
};

/**
 * Leave the group and close all connections
 *
 * @return {Promise}
 */
ConsumerGroup.prototype.end = function () {
    var self = this;
    var leaveEpoch = self.options.groupInstanceId ? -2 : -1; // -2: static leave (keep assignment), -1: dynamic leave
    var leavePromise;

    self._closed = true;
    clearTimeout(self._heartbeatTimeout);

    // only send a leave heartbeat if we actually joined (broker assigned an epoch)
    if (self.memberEpoch > 0) {
        leavePromise = self.client.consumerGroupHeartbeatRequest(
            self.options.groupId,
            self.memberId,
            leaveEpoch,
            null,
            null,
            null
        ).catch(function (err) {
            self.client.warn('Failed to send consumer group leave heartbeat:', err);
        });
    } else {
        leavePromise = Promise.resolve();
    }

    return leavePromise.then(function () {
        return BaseConsumer.prototype.end.call(self);
    });
};
