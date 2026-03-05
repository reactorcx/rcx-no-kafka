'use strict';

var crypto       = require('crypto');
var utils        = require('./utils');
var Client       = require('./client');
var errors       = require('./errors');
var promiseUtils = require('./promise-utils');

// Acknowledgement type constants (wire values from KIP-932)
var ACKNOWLEDGE_TYPE = {
    GAP: 0,
    ACCEPT: 1,
    RELEASE: 2,
    REJECT: 3
};

function ShareConsumer(options) {
    this.options = utils.defaultsDeep(options || {}, {
        groupId: 'no-kafka-share-group',
        maxWaitTime: 100,
        idleTimeout: 1000,
        minBytes: 1,
        maxBytes: 1024 * 1024,
        handlerConcurrency: 10
    });

    this.client = new Client(this.options);

    if (!this.client.validateId(this.options.groupId)) {
        throw new Error('Invalid groupId. Kafka IDs may not contain the following characters: ?:,"');
    }

    // Membership state — client generates its own UUID (required by ShareGroupHeartbeat)
    this.memberId = crypto.randomUUID();
    this.memberEpoch = 0;
    this.heartbeatIntervalMs = 5000;

    // Assignment: topicId -> [partitionIndex]
    this.assignedPartitions = {}; // { 'topicId': [0, 1, 2] }

    // Share session epoch (0 = open new session, increments after each fetch)
    this.shareSessionEpoch = 0;

    // Pending acknowledgements: keyed by "topicName:partition"
    // Each entry: { topicName, partition, batches: [{firstOffset, lastOffset, acknowledgeTypes}] }
    this._pendingAcks = {};

    // User handler and topics
    this._handler = null;
    this._topics = [];

    this._closed = false;
    this._heartbeatTimeout = null;
    this._fetchTimeout = null;
}

module.exports = ShareConsumer;

/**
 * Initialize ShareConsumer
 *
 * @param  {Object} options  { topics: [string], handler: function(messageSet, topic, partition, acquiredRecords) }
 * @return {Promise}
 */
ShareConsumer.prototype.init = function (options) {
    var self = this;

    if (!options || typeof options.handler !== 'function') {
        return Promise.reject(new Error('ShareConsumer requires a handler function'));
    }
    if (!options.topics || options.topics.length === 0) {
        return Promise.reject(new Error('ShareConsumer requires at least one topic'));
    }

    self._handler = function () {
        try {
            return Promise.resolve(options.handler.apply(this, arguments));
        } catch (e) {
            return Promise.reject(e);
        }
    };
    self._topics = options.topics;

    return self.client.init().then(function () {
        // Ensure metadata is loaded for our topics so topicIds are populated
        return self.client.updateMetadata(self._topics);
    }).then(function () {
        return self._join();
    });
};

/**
 * Join the share group: find coordinator, send initial heartbeat, start loops
 */
ShareConsumer.prototype._join = function () {
    var self = this;

    return (function _tryJoin(attempt) {
        attempt = attempt || 0;

        return self.client.updateGroupCoordinator(self.options.groupId).then(function () {
            // Initial heartbeat: memberEpoch=0 to join
            return self.client.shareGroupHeartbeatRequest(
                self.options.groupId,
                self.memberId,
                0,
                null,
                self._topics
            );
        })
        .then(function (response) {
            self.memberId = response.memberId;
            self.memberEpoch = response.memberEpoch;
            self.heartbeatIntervalMs = response.heartbeatIntervalMs;
            self.client.log('Joined share group', self.options.groupId, 'as', self.memberId, 'epoch', self.memberEpoch);

            if (response.assignment) {
                self._applyAssignment(response.assignment);
            }

            // Start heartbeat and fetch loops
            self._heartbeatPromise = self._heartbeat();
            self._fetchPromise = self._fetch();
        })
        .catch(function (err) {
            if (self._closed) { return null; }
            var maxAttempts = self.options.maxJoinAttempts || 100;
            if (attempt >= maxAttempts) {
                self.client.error('Share group join exceeded max attempts (' + maxAttempts + '), giving up');
                throw err;
            }
            var retryDelay = Math.min(1000 * Math.pow(2, attempt), 30000);
            self.client.error('Share group join failed (attempt ' + (attempt + 1) + '):', err);
            return promiseUtils.delay(retryDelay).then(function () {
                if (self._closed) { return null; }
                return _tryJoin(attempt + 1);
            });
        });
    }());
};

/**
 * Apply a new partition assignment from the coordinator
 */
ShareConsumer.prototype._applyAssignment = function (assignment) {
    var self = this, newAssigned = {}, topicPartitions;

    // assignment is a struct { _present, topicPartitions, tagCount }
    // _present < 0 means null assignment
    if (!assignment || assignment._present < 0) {
        return;
    }

    topicPartitions = assignment.topicPartitions || [];
    topicPartitions.forEach(function (tp) {
        newAssigned[tp.topicId] = tp.partitions;
    });

    self.assignedPartitions = newAssigned;
    // Reset share session since assignment changed
    self.shareSessionEpoch = 0;

    var topicNames = Object.keys(newAssigned).map(function (id) {
        return self.client.topicNames[id] || id;
    });
    self.client.log('Share group assignment:', topicNames.length > 0
        ? topicNames.join(', ')
        : '(none)');
};

/**
 * Heartbeat loop: sends ShareGroupHeartbeat at the interval returned by the coordinator
 */
ShareConsumer.prototype._heartbeat = function () {
    var self = this;

    return self.client.shareGroupHeartbeatRequest(
        self.options.groupId,
        self.memberId,
        self.memberEpoch,
        null,
        null // only send topics on initial join
    )
    .then(function (response) {
        if (self._closed) { return; }

        self.memberEpoch = response.memberEpoch;
        self.heartbeatIntervalMs = response.heartbeatIntervalMs;

        if (response.assignment) {
            self._applyAssignment(response.assignment);
        }

        self._heartbeatTimeout = setTimeout(function () {
            if (self._closed) { return; }
            self._heartbeatPromise = self._heartbeat();
        }, self.heartbeatIntervalMs);
    })
    .catch(function (err) {
        if (self._closed) { return; }

        if (err && err.code === 'UnknownMemberId') {
            self.client.warn('Unknown member, rejoining share group');
            self.memberId = crypto.randomUUID();
            self.memberEpoch = 0;
            return self._join();
        }

        self.client.error('Share group heartbeat failed:', err);
        // Retry after interval
        self._heartbeatTimeout = setTimeout(function () {
            if (self._closed) { return; }
            self._heartbeatPromise = self._heartbeat();
        }, self.heartbeatIntervalMs);
    });
};

/**
 * Fetch loop: sends ShareFetch to partition leaders, delivers records to handler
 */
ShareConsumer.prototype._fetch = function () {
    var self = this;

    return Promise.resolve().then(function () {
        var requests = self._buildFetchRequests();

        if (Object.keys(requests).length === 0) {
            return null;
        }

        // Collect and clear pending acks to piggyback on this fetch
        var acks = self._drainPendingAcks();

        return self.client.shareFetchRequest(
            self.options.groupId,
            self.memberId,
            self.shareSessionEpoch,
            requests,
            null,
            acks
        ).then(function (results) {
            // Increment session epoch after successful fetch
            if (self.shareSessionEpoch === 0) {
                self.shareSessionEpoch = 1;
            } else {
                self.shareSessionEpoch++;
            }

            return promiseUtils.mapConcurrent(results, function (p) {
                if (p.error) {
                    return self._partitionError(p.error, p.topic, p.partition);
                }

                if (p.messageSet && p.messageSet.length > 0) {
                    return self._handler(p.messageSet, p.topic, p.partition, p.acquiredRecords)
                    .catch(function (err) {
                        self.client.warn('Handler for', p.topic + ':' + p.partition, 'failed with', err);
                        // Release acquired records so they can be redelivered
                        if (p.acquiredRecords && p.acquiredRecords.length > 0) {
                            return self.acknowledge(
                                p.acquiredRecords.map(function (ar) {
                                    return { topic: p.topic, partition: p.partition, firstOffset: ar.firstOffset, lastOffset: ar.lastOffset };
                                }),
                                ACKNOWLEDGE_TYPE.RELEASE
                            );
                        }
                    });
                }
                return null;
            }, self.options.handlerConcurrency);
        });
    })
    .catch(function (err) {
        self.client.error(err);
    })
    .then(function () {
        if (self._closed) { return; }
        self._fetchTimeout = setTimeout(function () {
            if (self._closed) { return; }
            self._fetchPromise = self._fetch();
        }, self.options.idleTimeout);
    });
};

/**
 * Build fetch requests grouped by partition leader
 * Returns { leaderId: [{topicName, partitions: [{partition}]}] }
 */
ShareConsumer.prototype._buildFetchRequests = function () {
    var self = this, requests = {}, topicIds, i, topicId, topicName, partitions, j, leader;

    topicIds = Object.keys(self.assignedPartitions);
    for (i = 0; i < topicIds.length; i++) {
        topicId = topicIds[i];
        topicName = self.client.topicNames[topicId];
        if (!topicName || !self.client.topicMetadata[topicName]) { continue; }

        partitions = self.assignedPartitions[topicId];
        for (j = 0; j < partitions.length; j++) {
            leader = self.client.topicMetadata[topicName][partitions[j]];
            if (!leader) { continue; }
            leader = leader.leader;

            if (!requests[leader]) { requests[leader] = {}; }
            if (!requests[leader][topicName]) {
                requests[leader][topicName] = { topicName: topicName, partitions: [] };
            }
            requests[leader][topicName].partitions.push({ partition: partitions[j] });
        }
    }

    // Convert inner objects to arrays
    var result = {}, leaders = Object.keys(requests);
    for (i = 0; i < leaders.length; i++) {
        result[leaders[i]] = Object.values(requests[leaders[i]]);
    }
    return result;
};

/**
 * Handle partition-level errors from ShareFetch
 */
ShareConsumer.prototype._partitionError = function (err, topic, partition) {
    var self = this;

    if (/NotLeaderForPartition|LeaderNotAvailable|FencedLeaderEpoch/.test(err.code)) {
        self.client.debug('Received', err.code, 'for', topic + ':' + partition);
        return self.client.updateMetadata([topic]);
    }

    if (err instanceof errors.NoKafkaConnectionError) {
        self.client.debug('Received', err.toString(), 'for', topic + ':' + partition);
        return self.client.updateMetadata([topic]);
    }

    self.client.warn('Received', err.code || err, 'for', topic + ':' + partition);
    return null;
};

/**
 * Acknowledge records. Queues acks to be piggybacked on the next ShareFetch request.
 *
 * @param {Array} records  [{topic, partition, offset}] or [{topic, partition, firstOffset, lastOffset}]
 * @param {Number} type    ACKNOWLEDGE_TYPE constant (ACCEPT, RELEASE, REJECT)
 * @return {Promise}
 */
ShareConsumer.prototype.acknowledge = function (records, type) {
    var self = this, i, r, key;

    if (type === undefined) {
        type = ACKNOWLEDGE_TYPE.ACCEPT;
    }

    if (!Array.isArray(records)) {
        records = [records];
    }

    for (i = 0; i < records.length; i++) {
        r = records[i];
        key = r.topic + ':' + r.partition;

        if (!self._pendingAcks[key]) {
            self._pendingAcks[key] = {
                topicName: r.topic,
                partition: r.partition,
                batches: []
            };
        }
        self._pendingAcks[key].batches.push({
            firstOffset: r.firstOffset !== undefined ? r.firstOffset : r.offset,
            lastOffset: r.lastOffset !== undefined ? r.lastOffset : r.offset,
            acknowledgeTypes: [type]
        });
    }

    return Promise.resolve();
};

/**
 * Drain pending acks into the format expected by shareFetchRequest
 * Returns { topicName: { partition: [{firstOffset, lastOffset, acknowledgeTypes}] } } or null
 */
ShareConsumer.prototype._drainPendingAcks = function () {
    var self = this, keys, i, entry, acks = null;

    keys = Object.keys(self._pendingAcks);
    if (keys.length === 0) {
        return null;
    }

    acks = {};
    for (i = 0; i < keys.length; i++) {
        entry = self._pendingAcks[keys[i]];
        if (!acks[entry.topicName]) {
            acks[entry.topicName] = {};
        }
        acks[entry.topicName][entry.partition] = entry.batches;
    }

    self._pendingAcks = {};
    return acks;
};

/**
 * Leave the share group and close all connections
 *
 * @return {Promise}
 */
ShareConsumer.prototype.end = function () {
    var self = this;

    self._closed = true;
    clearTimeout(self._heartbeatTimeout);
    clearTimeout(self._fetchTimeout);

    // Send leave heartbeat (memberEpoch = -1)
    var leavePromise;
    if (self.memberId) {
        leavePromise = self.client.shareGroupHeartbeatRequest(
            self.options.groupId,
            self.memberId,
            -1,
            null,
            null
        ).catch(function (err) {
            self.client.warn('Failed to send share group leave heartbeat:', err);
        });
    } else {
        leavePromise = Promise.resolve();
    }

    return leavePromise.then(function () {
        return self.client.end();
    });
};

ShareConsumer.ACKNOWLEDGE_TYPE = ACKNOWLEDGE_TYPE;
