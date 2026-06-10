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

    // Share session epochs, per broker (KIP-932 sessions are per-broker):
    // { leaderId: epoch }. A missing entry means 0 = open a new session.
    this.shareSessionEpochs = {};

    // Pending acknowledgements: keyed by "topicName:partition"
    // Each entry: { topicName, partition, batches: [{firstOffset, lastOffset, acknowledgeTypes}] }
    this._pendingAcks = {};

    // User handler and topics
    this._handler = null;
    this._topics = [];

    this._closed = false;
    this._heartbeatTimeout = null;
    this._fetchTimeout = null;
    // the fetch loop self-perpetuates, so it must only ever be started once
    // (rejoins restart membership, not the fetch loop)
    this._fetchStarted = false;
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
            if (self._closed) {
                // end() raced this join — leave immediately so the member doesn't
                // squat in the group until the session timeout
                return self.client.shareGroupHeartbeatRequest(
                    self.options.groupId, response.memberId || self.memberId, -1, null, null
                ).catch(function () { return null; });
            }

            self.memberId = response.memberId;
            self.memberEpoch = response.memberEpoch;
            self.heartbeatIntervalMs = response.heartbeatIntervalMs;
            self.client.log('Joined share group', self.options.groupId, 'as', self.memberId, 'epoch', self.memberEpoch);

            if (response.assignment) {
                self._applyAssignment(response.assignment);
            }

            // Start the heartbeat loop; the fetch loop only on the FIRST join —
            // it keeps itself alive across rejoins
            self._heartbeatPromise = self._runHeartbeat();
            if (!self._fetchStarted) {
                self._fetchStarted = true;
                self._fetchPromise = self._fetch();
            }
            return null;
        })
        .catch(function (err) {
            var maxAttempts, warnAfter, retryDelay;
            if (self._closed) { return null; }
            maxAttempts = self.options.maxJoinAttempts || Infinity;
            warnAfter = 10;
            if (attempt >= maxAttempts) {
                self.client.error('Share group join exceeded max attempts (' + maxAttempts + '), giving up');
                throw err;
            }
            if (attempt === warnAfter) {
                self.client.warn('Share group join has failed ' + warnAfter + ' times, still retrying...');
            }
            retryDelay = Math.min(1000 * Math.pow(2, attempt), 30000);
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
    var self = this, newAssigned = {}, topicPartitions, topicNames;

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
    // Reset share sessions since assignment changed
    self._resetShareSessions();

    topicNames = Object.keys(newAssigned).map(function (id) {
        return self.client.topicNames[id] || id;
    });
    self.client.log('Share group assignment:', topicNames.length > 0
        ? topicNames.join(', ')
        : '(none)');
};

/**
 * Reopen all share sessions (epoch 0 = new session on the next fetch)
 */
ShareConsumer.prototype._resetShareSessions = function () {
    this.shareSessionEpochs = {};
};

/**
 * Run one heartbeat with a terminal-failure guard: in timer/loop context there
 * is no caller to receive a rejection, and an unhandled rejection kills the
 * process on modern Node. Terminal failures (join attempts exhausted) end the loop.
 */
ShareConsumer.prototype._runHeartbeat = function () {
    var self = this;
    return self._heartbeat().catch(function (err) {
        self.client.error('Share group heartbeat loop terminated:', err);
    });
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
            self._heartbeatPromise = self._runHeartbeat();
        }, self.heartbeatIntervalMs);
    })
    .catch(function (err) {
        if (self._closed) { return undefined; }

        // KIP-932: both errors mean the member must rejoin with epoch 0.
        // Retrying a fenced epoch unchanged can never succeed.
        if (err && (err.code === 'UnknownMemberId' || err.code === 'FencedMemberEpoch')) {
            self.client.warn('Share group heartbeat rejected (' + err.code + '), rejoining');
            if (err.code === 'UnknownMemberId') {
                self.memberId = crypto.randomUUID();
            }
            self.memberEpoch = 0;
            return self._join();
        }

        self.client.error('Share group heartbeat failed:', err);
        // Retry after interval
        self._heartbeatTimeout = setTimeout(function () {
            if (self._closed) { return; }
            self._heartbeatPromise = self._runHeartbeat();
        }, self.heartbeatIntervalMs);
        return undefined;
    });
};

/**
 * Fetch loop: sends ShareFetch to partition leaders, delivers records to handler
 */
ShareConsumer.prototype._fetch = function () {
    var self = this;

    return Promise.resolve().then(function () {
        var requests = self._buildFetchRequests();
        var leaders = Object.keys(requests);
        var sentEpochs = {};
        var epochsAtSend = self.shareSessionEpochs;
        var acks;

        if (leaders.length === 0) {
            return null;
        }

        leaders.forEach(function (l) {
            sentEpochs[l] = self.shareSessionEpochs[l] || 0;
        });

        // Collect and clear pending acks to piggyback on this fetch
        acks = self._drainPendingAcks();

        return self.client.shareFetchRequest(
            self.options.groupId,
            self.memberId,
            sentEpochs,
            requests,
            null,
            acks
        ).catch(function (err) {
            // the fetch never completed: requeue the drained acks so they are
            // not lost, and reopen the sessions we can no longer track
            if (acks) {
                self._requeueAcks(acks);
            }
            leaders.forEach(function (l) {
                delete self.shareSessionEpochs[l];
            });
            throw err;
        }).then(function (results) {
            // Advance each broker's session epoch — unless the sessions were reset
            // while this fetch was in flight (assignment change replaces the map
            // object), in which case the reset must stick
            if (self.shareSessionEpochs === epochsAtSend) {
                leaders.forEach(function (l) {
                    if ((self.shareSessionEpochs[l] || 0) === sentEpochs[l]) {
                        self.shareSessionEpochs[l] = sentEpochs[l] + 1;
                    }
                });
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
                        return undefined;
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
    var self = this, requests = {}, topicIds, i, topicId, topicName, partitions, j, leader, result, leaders;

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
    result = {}; leaders = Object.keys(requests);
    for (i = 0; i < leaders.length; i++) {
        result[leaders[i]] = Object.values(requests[leaders[i]]);
    }
    return result;
};

/**
 * Handle partition-level errors from ShareFetch
 */
ShareConsumer.prototype._partitionError = function (err, topic, partition) {
    var self = this, meta;

    if (/ShareSessionNotFound|InvalidShareSessionEpoch/.test(err.code)) {
        // the broker lost (or never had) our session — reopen it on the next fetch
        self.client.warn('Share session lost (' + err.code + ') for', topic + ':' + partition, '- reopening session');
        meta = self.client.topicMetadata[topic] && self.client.topicMetadata[topic][partition];
        if (meta) {
            delete self.shareSessionEpochs[meta.leader];
        } else {
            self._resetShareSessions();
        }
        return null;
    }

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
 * Put drained acks back into the pending queue (used when the fetch carrying
 * them failed). Drained batches go in FRONT of any acks queued meanwhile.
 */
ShareConsumer.prototype._requeueAcks = function (acks) {
    var self = this;

    Object.keys(acks).forEach(function (topicName) {
        Object.keys(acks[topicName]).forEach(function (partition) {
            var key = topicName + ':' + partition;
            if (!self._pendingAcks[key]) {
                self._pendingAcks[key] = {
                    topicName: topicName,
                    partition: Number(partition),
                    batches: []
                };
            }
            self._pendingAcks[key].batches = acks[topicName][partition].concat(self._pendingAcks[key].batches);
        });
    });
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
    var leavePromise;

    self._closed = true;
    clearTimeout(self._heartbeatTimeout);
    clearTimeout(self._fetchTimeout);
    // only send a leave heartbeat if we actually joined (broker assigned an epoch)
    if (self.memberEpoch > 0) {
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
