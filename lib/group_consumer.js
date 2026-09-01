'use strict';

var utils        = require('./utils');
var BaseConsumer = require('./base_consumer');
var Kafka        = require('./index');
var util         = require('util');
var errors       = require('./errors');
var promiseUtils = require('./promise-utils');

function GroupConsumer(options) {
    this.options = utils.defaultsDeep(options || {}, {
        groupId: 'no-kafka-group-v0.9',
        sessionTimeout: 15000, // min 6000, max 30000
        heartbeatTimeout: 1000,
        revokeTimeout: null, // bound on the onPartitionsRevoked drain, resolved just below
        retentionTime: 24 * 3600 * 1000, // offset retention time, in ms
        startingOffset: Kafka.LATEST_OFFSET
    });

    BaseConsumer.call(this, this.options);

    // revokeTimeout bounds the onPartitionsRevoked drain. It is derived from sessionTimeout so it
    // cannot be expressed in the defaults literal above; resolved once here rather than on every
    // rebalance. Resolved after BaseConsumer.call because warning needs this.client.
    if (this.options.revokeTimeout === null || this.options.revokeTimeout === undefined) {
        this.options.revokeTimeout = Math.floor(this.options.sessionTimeout / 2);
    } else if (typeof this.options.revokeTimeout !== 'number' || !isFinite(this.options.revokeTimeout) ||
               this.options.revokeTimeout < 0) {
        // Left unvalidated these reach setTimeout, which coerces them to 0 — silently turning the
        // drain off rather than bounding it. Fall back to the default instead.
        this.client.warn('Invalid revokeTimeout (' + this.options.revokeTimeout + '), expected a ' +
            'non-negative number of ms; falling back to half of sessionTimeout');
        this.options.revokeTimeout = Math.floor(this.options.sessionTimeout / 2);
    } else if (this.options.revokeTimeout === 0) {
        // Legitimate opt-out, but a silent one: the drain is the whole point of the option, and a
        // consumer that reaches this by accident gets pre-4.11 re-delivery with no other signal.
        this.client.warn('revokeTimeout is 0: onPartitionsRevoked will be notified but not awaited, ' +
            'so a rebalance can re-deliver an already emitted batch');
    } else if (this.options.revokeTimeout > this.options.sessionTimeout / 2) {
        // Heartbeats are suspended for the whole rebalance, and the drain is only one step of it —
        // JoinGroup, SyncGroup, updateMetadata and fetchOffset all share the same session budget.
        // Past half of it, a slow drain starts crowding out the steps that cannot be skipped.
        this.client.warn('revokeTimeout (' + this.options.revokeTimeout + 'ms) is more than half of ' +
            'sessionTimeout (' + this.options.sessionTimeout + 'ms); a slow onPartitionsRevoked drain ' +
            'leaves little of the session budget for the rest of the rebalance and risks eviction');
    }

    if (!this.client.validateId(this.options.groupId)) {
        throw new Error('Invalid groupId. Kafka IDs may not contain the following characters: ?:,"');
    }

    this.strategies = {}; // available assignment strategies

    this.leaderId = null;
    this.memberId = null;
    this.generationId = 0;
    this.members = null;
    this.topics = [];

    this.strategyName = null; // current strategy assigned by group coordinator

    this.ownedPartitions = []; // [{topic, partitions[]}] for cooperative rebalancing
    this._cooperative = false;
    this._needsRejoin = false;

    this._heartbeatPromise = Promise.resolve();
    this._closed = false;
}

module.exports = GroupConsumer;

util.inherits(GroupConsumer, BaseConsumer);

/**
 * Initialize GroupConsumer
 *
 * @param  {Array|Object} strategies [{name, subscriptions, metadata, strategy, handler}]
 * @return {Promise}
 */
GroupConsumer.prototype.init = function (strategies) {
    var self = this;

    return BaseConsumer.prototype.init.call(self).then(function () {
        if (!strategies || (Array.isArray(strategies) ? strategies.length === 0 : Object.keys(strategies).length === 0)) {
            throw new Error('Group consumer requires Assignment Strategies to be fully configured');
        }

        if (!Array.isArray(strategies)) {
            strategies = [strategies];
        }

        strategies.forEach(function (s) {
            if (typeof s.handler !== 'function') {
                throw new Error('Strategy ' + s.name + ' is missing data handler');
            }
            if (s.strategy === undefined) {
                s.strategy = new Kafka.DefaultAssignmentStrategy();
            }
            if (!(s.strategy instanceof Kafka.DefaultAssignmentStrategy)) {
                throw new Error('AssignmentStrategy must inherit from Kafka.DefaultAssignmentStrategy');
            }
            if (s.name === undefined) {
                s.name = s.strategy.constructor.name;
            }
            if (typeof s.metadata === 'object' && s.metadata !== null && !Array.isArray(s.metadata)) {
                s.metadata = JSON.stringify(s.metadata);
            }
            if (s.cooperative) {
                s.version = 1;
                self._cooperative = true;
            } else {
                s.version = 0;
            }
            if (typeof s.onPartitionsRevoked === 'function') {
                self._onPartitionsRevoked = s.onPartitionsRevoked;
            }
            if (typeof s.onPartitionsAssigned === 'function') {
                self._onPartitionsAssigned = s.onPartitionsAssigned;
            }
            self.strategies[s.name] = s;
            self.topics = self.topics.concat(s.subscriptions);
        });

        return self._fullRejoin();
    });
};

GroupConsumer.prototype._joinGroup = function () {
    var self = this;

    return (function _tryJoinGroup(attempt) {
        attempt = attempt || 0;
        if (attempt > 3) {
            throw new Error('Failed to join the group: GroupCoordinatorNotAvailable');
        }

        return self.client.joinConsumerGroupRequest(self.options.groupId, self.memberId, self.options.sessionTimeout, Object.values(self.strategies).map(function (s) {
            s.ownedPartitions = self.ownedPartitions;
            return s;
        }))
        .catch(function (err) {
            if (err && err.code === 'GroupCoordinatorNotAvailable') {
                return promiseUtils.delay(1000).then(function () {
                    return _tryJoinGroup(++attempt);
                });
            }
            // KIP-394: broker returns MemberIdRequired with assigned memberId on first join
            if (err && err.code === 'MemberIdRequired' && err.memberId) {
                self.memberId = err.memberId;
                return _tryJoinGroup(attempt);
            }
            throw err;
        });
    }())
    .then(function (response) {
        if (self.memberId) {
            self.client.log('Joined group', self.options.groupId, 'generationId', response.generationId, 'as', response.memberId);
            if (response.memberId === response.leaderId) {
                self.client.log('Elected as group leader');
            }
        }
        self.memberId = response.memberId;
        self.leaderId = response.leaderId;
        self.generationId = response.generationId;
        self.members = response.members;
        self.strategyName = response.groupProtocol;
    });
};

GroupConsumer.prototype._syncGroup = function () {
    var self = this;

    return Promise.resolve().then(function () {
        if (self.memberId === self.leaderId) { // leader should generate group assignments
            return self.client.updateMetadata(self.topics).then(function () {
                var r = [], topicMap = {}, topicKeys, i, j, tp;
                for (i = 0; i < self.members.length; i++) {
                    for (j = 0; j < self.members[i].subscriptions.length; j++) {
                        tp = self.members[i].subscriptions[j];
                        if (!topicMap[tp]) { topicMap[tp] = []; }
                        topicMap[tp].push(self.members[i]);
                    }
                }
                topicKeys = Object.keys(topicMap);
                for (i = 0; i < topicKeys.length; i++) {
                    if (!self.client.topicMetadata[topicKeys[i]]) {
                        self.client.error('Sync group: unknown topic:', topicKeys[i]);
                    }
                    r.push({
                        topic: topicKeys[i],
                        members: topicMap[topicKeys[i]],
                        partitions: Object.values(self.client.topicMetadata[topicKeys[i]]).map(function (p) { return p.partitionId; })
                    });
                }

                return self.strategies[self.strategyName].strategy.assignment(r);
            });
        }
        return [];
    })
    .then(function (result) {
        var ownerMap, assignments, assignedIds, memberGroups;

        // In cooperative mode, filter out partitions that are migrating between members
        if (self._cooperative && self.members && self.members.every(function (m) { return m.version >= 1; })) {
            // Build current ownership map: "topic:partition" -> memberId
            ownerMap = {};
            self.members.forEach(function (member) {
                (member.ownedPartitions || []).forEach(function (tp) {
                    tp.partitions.forEach(function (p) {
                        ownerMap[tp.topic + ':' + p] = member.id;
                    });
                });
            });
            // Keep only assignments where partition is unowned or already owned by the target member
            result = result.filter(function (a) {
                var key = a.topic + ':' + a.partition;
                return !ownerMap[key] || ownerMap[key] === a.memberId;
            });
        }

        memberGroups = utils.groupBy(result, function (a) { return a.memberId; });
        assignments = Object.keys(memberGroups).map(function (mk) {
            var mv = memberGroups[mk];
            var topicGroups = utils.groupBy(mv, function (a) { return a.topic; });
            return {
                memberId: mk,
                memberAssignment: {
                    version: 0,
                    metadata: null,
                    partitionAssignment: Object.keys(topicGroups).map(function (tk) {
                        return {
                            topic: tk,
                            partitions: topicGroups[tk].map(function (a) { return a.partition; })
                        };
                    })
                }
            };
        });

        // Ensure all members get an assignment entry (even if empty)
        if (self._cooperative && self.members) {
            assignedIds = assignments.map(function (a) { return a.memberId; });
            self.members.forEach(function (member) {
                if (assignedIds.indexOf(member.id) === -1) {
                    assignments.push({
                        memberId: member.id,
                        memberAssignment: {
                            version: 0,
                            metadata: null,
                            partitionAssignment: []
                        }
                    });
                }
            });
        }

        // console.log(require('util').inspect(assignments, true, 10, true));
        return self.client.syncConsumerGroupRequest(self.options.groupId, self.memberId, self.generationId, assignments, self.strategyName);
    })
    .then(function (response) {
        var pa = response && response.memberAssignment ? response.memberAssignment.partitionAssignment : [];
        return self._updateSubscriptions(pa || []);
    });
};

GroupConsumer.prototype._rejoin = function () {
    var self = this;

    return (function _tryRebalance(attempt) {
        attempt = attempt || 0;

        if (attempt > 3) {
            throw new Error('Failed to rejoin: RebalanceInProgress');
        }

        return self._joinGroup().then(function () {
            return self._syncGroup();
        })
        .then(function (r) {
            if (self._needsRejoin) {
                self._needsRejoin = false;
                self.client.log('Cooperative rebalance: triggering phase 2 rejoin');
                return self._rejoin();
            }
            return r;
        })
        .catch(function (err) {
            if (err && err.code === 'RebalanceInProgress') {
                return promiseUtils.delay(1000).then(function () {
                    return _tryRebalance(++attempt);
                });
            }
            throw err;
        });
    }());
};

/**
 * Await the `onPartitionsRevoked` callback for `partitions`, letting the consumer drain
 * in-flight, already-emitted-but-uncommitted work, bounded by `revokeTimeout`. Always resolves:
 * a failing or timed-out drain warns and degrades to the previous fire-and-forget behaviour
 * rather than hanging or throwing out of a rebalance path.
 *
 * @param  {Array}  partitions  flat [{topic, partition}, ...] being revoked
 * @param  {String} context     name of the rebalance path, used in the warning
 * @return {Promise} always resolves — never rejects
 */
GroupConsumer.prototype._drainRevoked = function (partitions, context) {
    var self = this, startedAt;
    if (!partitions || partitions.length === 0 || !self._onPartitionsRevoked) {
        return Promise.resolve();
    }
    if (self.options.revokeTimeout === 0) {
        // Explicit opt-out: notify fire-and-forget (the pre-4.11 behaviour) and do not wait. Racing
        // a 0ms timer instead would fail the drain on every rebalance and warn about re-delivery
        // the caller has already accepted.
        Promise.resolve().then(function () {
            return self._onPartitionsRevoked(partitions);
        })
        .catch(function (err) {
            self.client.debug('onPartitionsRevoked failed during ' + context + ' (not awaited):', err);
        });
        return Promise.resolve();
    }
    // Invoked inside the .then so a synchronous throw from the callback is caught below too.
    startedAt = Date.now();
    return Promise.resolve().then(function () {
        return promiseUtils.timeout(self._onPartitionsRevoked(partitions), self.options.revokeTimeout,
            'onPartitionsRevoked drain timed out');
    })
    .catch(function (err) {
        // The elapsed time separates a drain that exhausted revokeTimeout from one that rejected
        // immediately — different incidents with the same message otherwise.
        self.client.warn('onPartitionsRevoked drain failed during ' + context + ' after ' +
            (Date.now() - startedAt) + 'ms for ' +
            partitions.map(function (p) { return p.topic + ':' + p.partition; }).join(', ') +
            ' — these partitions may re-deliver, proceeding:', err);
    });
};

GroupConsumer.prototype._fullRejoin = function () {
    var self = this;

    var maxAttempts = self.options.maxRejoinAttempts || Infinity;
    var warnAfter = 10;

    return (function _tryFullRejoin(attempt) {
        var retryDelay, flatPartitions = [];
        attempt = attempt || 0;
        // Drain what this instance still owns BEFORE wiping ownedPartitions: the revoke diff in
        // _updateSubscriptionsCooperative is computed against ownedPartitions, so wiping first
        // would leave nothing to revoke and the drain would never run on a full rejoin.
        // memberId is cleared only after the drain for the same reason: commitOffset rejects
        // outright while memberId is null, so nulling it first makes a committing callback a no-op.
        self.ownedPartitions.forEach(function (tp) {
            tp.partitions.forEach(function (p) {
                flatPartitions.push({ topic: tp.topic, partition: p });
            });
        });
        // Re-resolve the coordinator BEFORE draining. The common trigger for a full rejoin is a
        // heartbeat failure against a coordinator that is already unhealthy, and a drain issued
        // through that connection can only fail — after burning the whole revokeTimeout to do it.
        // If this rejects we retry below with ownedPartitions still intact, so the drain is
        // re-attempted next round rather than skipped.
        return self.client.updateGroupCoordinator(self.options.groupId)
        .then(function () {
            return self._drainRevoked(flatPartitions, 'full rejoin');
        })
        .then(function () {
            self.memberId = null;
            self.ownedPartitions = [];
            return self._rejoin();
        })
        .catch(function (err) {
            // KIP-345: FencedInstanceId is fatal — do not retry
            if (err && err.code === 'FencedInstanceId') {
                self._closed = true;
                clearTimeout(self._heartbeatTimeout);
                self.client.error('Fatal: FencedInstanceId — another consumer has taken over group.instance.id');
                throw err;
            }
            if (self._closed) { return null; }
            if (attempt >= maxAttempts) {
                self.client.error('Full rejoin exceeded max attempts (' + maxAttempts + '), giving up');
                throw err;
            }
            if (attempt === warnAfter) {
                self.client.warn('Full rejoin has failed ' + warnAfter + ' times, still retrying...');
            }
            retryDelay = Math.min(1000 * Math.pow(2, attempt), 30000);
            self.client.error('Full rejoin attempt failed (attempt ' + (attempt + 1) + '):', err);
            return promiseUtils.delay(retryDelay).then(function () {
                if (self._closed) { return null; }
                return _tryFullRejoin(attempt + 1);
            });
        });
    }())
    .then(function (r) {
        self._heartbeatPromise = self._heartbeat(); // start sending heartbeats
        return r;
    });
};

GroupConsumer.prototype._heartbeat = function () {
    var self = this;

    return self.client.heartbeatRequest(self.options.groupId, self.memberId, self.generationId)
    .catch(function (err) {
        if (err && err.code === 'RebalanceInProgress') {
            // new group member has joined or existing member has left
            self.client.log('Rejoining group on RebalanceInProgress');
            return self._rejoin();
        }
        throw err;
    })
    .then(function (r) {
        if (self._closed) { return r; }
        self._heartbeatTimeout = setTimeout(function () {
            if (self._closed) { return; }
            self._heartbeatPromise = self._heartbeat();
        }, self.options.heartbeatTimeout);
        return r;
    })
    .catch(function (err) {
        if (self._closed) { return undefined; }
        // KIP-345: FencedInstanceId is fatal — another consumer has our instance ID
        if (err && err.code === 'FencedInstanceId') {
            self._closed = true;
            clearTimeout(self._heartbeatTimeout);
            self.client.error('Fatal: FencedInstanceId — another consumer has taken over group.instance.id');
            throw err;
        }
        // some severe error, such as GroupCoordinatorNotAvailable or network error
        // in this case we should start trying to rejoin from scratch
        self.client.error('Sending heartbeat failed: ', err);
        return self._fullRejoin().catch(function (_err) {
            self.client.error(_err);
            if (self._closed) { return; }
            // Safety net: schedule heartbeat retry even if _fullRejoin fails
            self._heartbeatTimeout = setTimeout(function () {
                if (self._closed) { return; }
                self._heartbeatPromise = self._heartbeat();
            }, self.options.heartbeatTimeout);
        });
    });
};

/**
 * Leave consumer group and close all connections
 *
 * @return {Promise}
 */
GroupConsumer.prototype.end = function () {
    var self = this;

    self.subscriptions = {};
    self._closed = true;
    clearTimeout(self._heartbeatTimeout);

    // KIP-345: static members skip LeaveGroup so the broker preserves their assignment
    if (self.options.groupInstanceId) {
        return BaseConsumer.prototype.end.call(self);
    }

    return self.client.leaveGroupRequest(self.options.groupId, self.memberId)
    .then(function () {
        return BaseConsumer.prototype.end.call(self);
    });
};

GroupConsumer.prototype._prepareOffsetRequest = function (type, commits) {
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
                    offset: p.offset + 1, // commit next offset instead of last consumed
                    metadata: p.metadata
                };
            })
        };
    });
};

/**
 * Commit (save) processed offsets to Kafka
 *
 * @param  {Object|Array} commits [{topic, partition, offset, metadata}]
 * @return {Promise}
 */
GroupConsumer.prototype.commitOffset = function (commits) {
    var self = this;
    if (self.memberId === null) {
        return Promise.reject(errors.byName('RebalanceInProgress'));
    }
    return self.client.offsetCommitRequestV2(self.options.groupId, self.memberId, self.generationId,
        self._prepareOffsetRequest('commit', commits));
};

/**
 * Fetch commited (saved) offsets [{topic, partition}]
 *
 * @param  {Object|Array} commits
 * @return {Promise}
 */
GroupConsumer.prototype.fetchOffset = function (commits) {
    var self = this;
    return self.client.offsetFetchRequestV1(self.options.groupId, self._prepareOffsetRequest('fetch', commits));
};

/**
 * Fetch committed offsets for `offsetRequests` and subscribe each partition from there, falling
 * back to `startingOffset` where no committed offset exists. Shared by both rebalance paths.
 * A partition that fails to subscribe is logged and skipped; it does not fail the others.
 *
 * @param  {Array}    offsetRequests [{topic, partition}, ...]
 * @param  {Function} handler        message handler to subscribe with
 * @return {Promise}
 */
GroupConsumer.prototype._fetchOffsetsAndSubscribe = function (offsetRequests, handler) {
    var self = this;

    return self.client.updateMetadata(self.topics)
    .then(function () {
        return self.fetchOffset(offsetRequests);
    })
    .then(function (results) {
        return Promise.all(results.map(function (p) {
            var options = {
                offset: p.offset
            };

            if (p.error || p.offset < 0) {
                self.client.warn('No commited offset available, time set to startingOffset for', p.topic + ':' + p.partition);
                options = {
                    time: self.options.startingOffset
                };
            }

            return self.subscribe(p.topic, p.partition, options, handler).catch(function (err) {
                self.client.error('Failed to subscribe to', p.topic + ':' + p.partition, err);
            });
        }));
    });
};

GroupConsumer.prototype._updateSubscriptions = function (partitionAssignment) {
    var self = this, offsetRequests = [],
        handler = self.strategies[self.strategyName].handler,
        previousPartitions;

    if (self._cooperative) {
        return self._updateSubscriptionsCooperative(partitionAssignment, handler);
    }

    // Capture previously held partitions before wiping
    previousPartitions = Object.keys(self.subscriptions).map(function (key) {
        var parts = key.split(':');
        return { topic: parts[0], partition: Number(parts[1]) };
    });

    // Wipe before the drain, not after: _fetch's handler-completion logic (base_consumer.js)
    // clears `paused` once an in-flight handler resolves, which would re-enable fetching for a
    // revoked partition mid-drain. _fetch only sees keys still present in self.subscriptions.
    self.subscriptions = {};

    // Await the revoke drain so the fetchOffset below reads the post-drain committed offset and
    // the rejoin cannot re-deliver an already-emitted batch. A failed drain degrades to the
    // previous behaviour (re-subscribe from the last committed offset).
    return self._drainRevoked(previousPartitions, 're-subscribe')
    .then(function () {
        // Defence in depth: until the new assignment is subscribed below this consumer owns
        // nothing, so anything present here was re-added concurrently while the drain ran (a
        // subscribe() that resolved findLeader after the wipe is one way that can happen).
        // Re-wipe rather than reason about which paths can reach it.
        self.subscriptions = {};

        if (!partitionAssignment || partitionAssignment.length === 0) {
            return self.client.warn('No partition assignment received');
        }

        partitionAssignment.forEach(function (a) {
            a.partitions.forEach(function (p) {
                offsetRequests.push({
                    topic: a.topic,
                    partition: p
                });
            });
        });

        return self._fetchOffsetsAndSubscribe(offsetRequests, handler).then(function () {
            if (self._onPartitionsAssigned) {
                self._onPartitionsAssigned(offsetRequests);
            }
        });
    });
};

GroupConsumer.prototype._updateSubscriptionsCooperative = function (partitionAssignment, handler) {
    var self = this, newSet = {}, ownedSet = {}, toRevoke = [], toAdd = [], offsetRequests = [];

    // Build set of newly assigned partitions
    partitionAssignment.forEach(function (a) {
        a.partitions.forEach(function (p) {
            newSet[a.topic + ':' + p] = true;
        });
    });

    // Find partitions to revoke (currently subscribed but not in new assignment)
    self.ownedPartitions.forEach(function (tp) {
        tp.partitions.forEach(function (p) {
            var key = tp.topic + ':' + p;
            if (!newSet[key]) {
                toRevoke.push({ topic: tp.topic, partition: p });
            }
        });
    });

    // Find partitions to add (in new assignment but not currently owned)
    self.ownedPartitions.forEach(function (tp) {
        tp.partitions.forEach(function (p) {
            ownedSet[tp.topic + ':' + p] = true;
        });
    });
    partitionAssignment.forEach(function (a) {
        a.partitions.forEach(function (p) {
            if (!ownedSet[a.topic + ':' + p]) {
                toAdd.push({ topic: a.topic, partition: p });
            }
        });
    });

    // Revoke partitions (unsubscribe removes them from the fetch loop)
    toRevoke.forEach(function (r) {
        self.unsubscribe(r.topic, r.partition);
    });

    if (toRevoke.length > 0) {
        self.client.log('Cooperative rebalance: revoked', toRevoke.length, 'partitions');
        self._needsRejoin = true;
    }

    // Update ownedPartitions to match new assignment
    self.ownedPartitions = partitionAssignment;

    // Await the revoke drain (in-flight commits for the revoked partitions) before subscribing
    // the newly-added partitions, so a partition given up here is committed past its in-flight
    // batch before it is re-fetched. A failed drain degrades to the previous behaviour.
    // A phase-2 rejoin (triggered by _needsRejoin above) runs this path again, so a rebalance
    // that revokes in both phases can spend 2 x revokeTimeout here with heartbeats suspended.
    return self._drainRevoked(toRevoke, 'cooperative rebalance').then(function () {
        // Defence in depth, mirroring the eager path: these partitions were unsubscribed above
        // and nothing here should have re-added them, but if anything did (a subscribe() that
        // resolved findLeader after the delete is one way) nothing else would remove it, and the
        // partition would keep being fetched after we gave it up while its new owner consumed it
        // too. The awaited drain widens that window to revokeTimeout, so re-drop unconditionally.
        toRevoke.forEach(function (r) {
            self.unsubscribe(r.topic, r.partition);
        });

        if (toAdd.length === 0) {
            if (toRevoke.length > 0 && self._onPartitionsAssigned) {
                self._onPartitionsAssigned([]);
            }
            return null;
        }

        // Fetch offsets and subscribe for new partitions only
        toAdd.forEach(function (a) {
            offsetRequests.push({ topic: a.topic, partition: a.partition });
        });

        return self._fetchOffsetsAndSubscribe(offsetRequests, handler).then(function () {
            self.client.log('Cooperative rebalance: added', toAdd.length, 'partitions');
            if (self._onPartitionsAssigned) {
                self._onPartitionsAssigned(toAdd);
            }
        });
    });
};
