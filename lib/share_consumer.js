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

// longest end() takes for its final acks and leave (Java's default close timeout); the acks get
// at most ACK_WAIT_MS of it, so a broker that never answers still leaves time to leave the group
var END_WAIT_MS = 30000;
var ACK_WAIT_MS = 25000;

// resolves when p settles or after ms, whichever is first
function _within(p, ms) {
    return new Promise(function (resolve) {
        var timer = setTimeout(resolve, ms);
        Promise.resolve(p).catch(function () { return null; }).then(function () {
            clearTimeout(timer);
            resolve();
        });
    });
}

// a delivery's acquired ranges as a sorted list of disjoint, non-adjacent copies (a handler
// may change the array it was given)
function _mergeRanges(ranges) {
    var merged = [];
    ranges.map(function (r) { return { firstOffset: r.firstOffset, lastOffset: r.lastOffset }; })
    .sort(function (a, b) { return a.firstOffset - b.firstOffset; })
    .forEach(function (r) {
        var prev = merged[merged.length - 1];
        if (prev && r.firstOffset <= prev.lastOffset + 1) {
            prev.lastOffset = Math.max(prev.lastOffset, r.lastOffset);
        } else {
            merged.push(r);
        }
    });
    return merged;
}

function _byFirst(a, b) {
    return a.firstOffset - b.firstOffset;
}

// an ack batch list split by delivery (_gen), oldest first
function _byGen(batches) {
    var groups = {};
    batches.forEach(function (b) { (groups[b._gen || 0] = groups[b._gen || 0] || []).push(b); });
    return Object.keys(groups).map(Number).sort(function (a, b) { return a - b; }).map(function (g) { return groups[g]; });
}

// the oldest delivery an ack batch list holds (_gen), and a filter for batches of (or not of) one
function _oldestGen(batches) {
    return batches.reduce(function (min, b) { return Math.min(min, b._gen || 0); }, Infinity); // no apply: any length
}
function _ofGen(gen, same) {
    return function (b) { return ((b._gen || 0) === gen) === same; };
}

// a partition's ack batches as sent: in offset order (as Java sends them), gap acks giving way to
// the explicit acks over the same offsets (the broker rejects overlapping batches). One sweep
function _forTheWire(batches) {
    var gaps = [], explicit = [], merged, out = [], k = 0;
    batches.forEach(function (b) { (b._gap ? gaps : explicit).push(b); });
    if (gaps.length === 0 || explicit.length === 0) {
        return batches.slice().sort(_byFirst);
    }
    merged = _mergeRanges(explicit);
    gaps.sort(_byFirst).forEach(function (g) {
        var from = g.firstOffset, m;
        while (k < merged.length && merged[k].lastOffset < from) { k++; }
        for (m = k; m < merged.length && merged[m].firstOffset <= g.lastOffset; m++) {
            if (merged[m].firstOffset > from) {
                out.push(Object.assign({}, g, { firstOffset: from, lastOffset: merged[m].firstOffset - 1 }));
            }
            from = Math.max(from, merged[m].lastOffset + 1);
        }
        if (from <= g.lastOffset) {
            out.push(Object.assign({}, g, { firstOffset: from, lastOffset: g.lastOffset }));
        }
    });
    return explicit.concat(out).sort(_byFirst);
}

// whether merged ranges (_mergeRanges) cover every offset from first to last: then one range does
function _covered(first, last, merged) {
    var lo = 0, hi = merged.length - 1, mid;
    while (lo <= hi) { // the last range starting at or before first
        mid = (lo + hi) >> 1;
        if (merged[mid].firstOffset <= first) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    return hi >= 0 && merged[hi].lastOffset >= last;
}

// the parts of an ack batch outside merged ranges (_mergeRanges), each with the batch's other fields
function _outside(batch, merged) {
    var pieces = [], from = batch.firstOffset;
    merged.forEach(function (r) {
        if (r.lastOffset < from || r.firstOffset > batch.lastOffset) {
            return;
        }
        if (r.firstOffset > from) {
            pieces.push(Object.assign({}, batch, { firstOffset: from, lastOffset: r.firstOffset - 1 }));
        }
        from = r.lastOffset + 1;
    });
    if (from <= batch.lastOffset) {
        pieces.push(Object.assign({}, batch, { firstOffset: from, lastOffset: batch.lastOffset }));
    }
    return pieces;
}

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
    // { leaderId: true }: sessions reset by an assignment change, still open on the broker
    this._reopening = {};
    this._deliveryGen = 0; // numbers deliveries, so acks for different ones aren't sent together
    // the background metadata refresh in flight (_refreshMetadata), and the topics wanted next
    this._refreshing = null;
    this._refreshWanted = Object.create(null); // keyed by topic name

    // Pending acknowledgements: keyed by "topicName:partition"
    // Each entry: { topicName, partition, batches: [{firstOffset, lastOffset, acknowledgeTypes, topicId}] }
    this._pendingAcks = {};
    // "topicName:partition" -> its delivery whose handler is running { topicId, ranges (acquiredRecords) }.
    // Only its records can be acknowledged, and only until the handler settles, as Java's acknowledge()
    // takes records only until the next poll(): an ack carries no identity, so a later one could be
    // applied to records redelivered since, or to a topic recreated under the same name. An ack is
    // tagged with the topic ID, so it is never sent for a topic recreated after it was made
    this._deliveries = {};

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
                // end() raced this join. While it is still sending its final acks, it sends this
                // member's leave after them (a leave first could release records before their acks)
                if (self._endPromise && !self._leaving) {
                    self.memberId = response.memberId || self.memberId;
                    self.memberEpoch = response.memberEpoch;
                    return null;
                }
                // otherwise leave now, so the member doesn't squat in the group until the session timeout
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
 * The broker dropped this member (UnknownMemberId): rejoin as a new one. Its share sessions, queued
 * acks and deliveries belong to the old member, and nothing the new one sends can apply or close
 * them; the broker releases the old member's records when it expires it
 */
ShareConsumer.prototype._newMember = function () {
    var self = this, dropped = Object.keys(self._pendingAcks);
    if (dropped.length > 0) {
        self.client.warn('Dropped queued acknowledgements for', dropped.join(', '), '- the member was replaced');
    }
    self.memberId = crypto.randomUUID();
    self.assignedPartitions = {}; // the old member's: fetching resumes with the new one's assignment
    self._pendingAcks = {};
    self._deliveries = {};
    self._resetShareSessions(true);
};

/**
 * Reopen all share sessions (epoch 0 = new session on the next fetch)
 */
ShareConsumer.prototype._resetShareSessions = function (lost) {
    var self = this;
    // the broker keeps a session until a fetch reopens it, so end() can still close it (and send
    // its acks) meanwhile; not one the broker reported lost
    if (lost) {
        self._reopening = {};
    } else {
        Object.keys(self.shareSessionEpochs).forEach(function (l) {
            if (self.shareSessionEpochs[l] > 0) {
                self._reopening[l] = true;
            }
        });
    }
    self.shareSessionEpochs = {};
};

// whether the broker has this member's session open (end() closes it)
ShareConsumer.prototype._sessionOpen = function (leader) {
    return this.shareSessionEpochs[leader] > 0 || this._reopening[leader] === true;
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
                self._newMember();
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
        var requests = self._ended ? {} : self._buildFetchRequests(); // end() ran: no new fetch
        var leaders = Object.keys(requests);
        var sentEpochs = {};
        var epochsAtSend = self.shareSessionEpochs;
        var acks, perLeader, member = self.memberId;
        var limit = self.options.handlerConcurrency >= 1 ? self.options.handlerConcurrency : 10, active = 0, queued = [];

        // runs fn when fewer than `limit` are running
        function _slot(fn) {
            return new Promise(function (resolve) {
                queued.push(function () {
                    active++;
                    resolve(Promise.resolve().then(fn).finally(function () {
                        active--;
                        if (queued.length > 0) { queued.shift()(); }
                    }));
                });
                if (active < limit) { queued.shift()(); }
            });
        }

        // not waited for: partitions with metadata are fetched now, the rest once it arrives
        if (!self._ended) {
            self._refreshMissingMetadata();
        }
        if (leaders.length === 0) {
            return null;
        }

        leaders.forEach(function (l) {
            sentEpochs[l] = self.shareSessionEpochs[l] || 0;
        });

        // Collect and clear the pending acks this fetch carries: those for a partition it
        // requests (acks ride on a requested partition), from a broker whose session is open
        // (one rejects acks on the request that opens it). The rest wait for a fetch that
        // carries them, or for end()
        acks = self._drainPendingAcks(true, function (entry) {
            var l = self._ackLeader(entry);
            return l !== undefined && sentEpochs[l] > 0 && !!requests[l] && requests[l].some(function (t) {
                return t.topicName === entry.topicName && t.partitions.some(function (q) { return q.partition === entry.partition; });
            });
        });

        // one request per broker (each carries only its own partitions' acks): end() waits for each
        // separately before that broker's final acks, so a silent broker can't hold up the others
        // the acks this broker's request carries
        function _acksOf(l) {
            var mine = null;
            requests[l].forEach(function (t) {
                t.partitions.forEach(function (q) {
                    var batches = acks && acks[t.topicName] && acks[t.topicName][q.partition];
                    if (batches) {
                        mine = mine || Object.create(null); // keyed by topic name
                        mine[t.topicName] = mine[t.topicName] || {};
                        mine[t.topicName][q.partition] = batches;
                    }
                });
            });
            return mine;
        }

        self._fetchRequests = {};
        perLeader = leaders.map(function (l) {
            var only = {}, mine = _acksOf(l), p;
            only[l] = requests[l];
            // this broker's session and acks are settled as soon as its own request is, before end()
            // (which waits for it) closes that session
            p = self.client.shareFetchRequest(self.options.groupId, member, sentEpochs, only, null, mine)
            .then(function (results) {
                var retry = Object.create(null); // keyed by topic name
                if (self.memberId !== member) { // the member was replaced meanwhile: none of this is its
                    return [];
                }
                // advance the session epoch, unless the sessions were reset while this was in flight
                // (an assignment change replaces the map), in which case the reset must stick; the
                // session it used or opened is then open on the broker until a fetch reopens it
                if (results.length > 0 && results.every(function (r) { return r.notFetched; })) {
                    // the broker didn't take it: the epoch stays (session errors reopen it in
                    // _partitionError). Lost in transit, it may have: reopen, and end() still closes it
                    if (results[0].error instanceof errors.NoKafkaConnectionError) {
                        delete self.shareSessionEpochs[l];
                        self._reopening[l] = true;
                    }
                } else if (self.shareSessionEpochs === epochsAtSend) {
                    if ((self.shareSessionEpochs[l] || 0) === sentEpochs[l]) {
                        self.shareSessionEpochs[l] = sentEpochs[l] + 1;
                        delete self._reopening[l]; // reopened: this session replaced the old one
                    }
                } else {
                    self._reopening[l] = true;
                }
                // a partition with no broker response (its broker rejected the whole request, or was
                // unreachable) may not have had its acks applied: send them again, once per
                // partition; the broker refuses acks for records it no longer holds for this member
                results.forEach(function (r) {
                    if (r.error && r.acknowledgeError === undefined && mine && mine[r.topic] && mine[r.topic][r.partition]) {
                        retry[r.topic] = retry[r.topic] || {};
                        retry[r.topic][r.partition] = mine[r.topic][r.partition];
                    }
                });
                self._requeueAcks(retry, l);
                return results;
            }, function (err) {
                if (self.memberId !== member) { // the member was replaced meanwhile: none of this is its
                    return [];
                }
                // never completed: requeue its acks so they are not lost, and reopen its session; the
                // other brokers' records are still delivered. A failed request (connection, decode)
                // doesn't close a session on the broker, and one sent at epoch 0 may have opened one
                // and acquired records: end() still tries to close it
                self.client.error('Share fetch from broker', l, 'failed:', err);
                if (mine) {
                    self._requeueAcks(mine, l);
                }
                self._reopening[l] = true;
                delete self.shareSessionEpochs[l];
                return [];
            });
            self._fetchRequests[l] = p.then(function () { return null; }); // holds none of its records
            return p;
        });
        function _dispatch(p) {
            var delivery;
            function _recoveryFailed(err) {
                self.client.warn('Recovery after', p.error.code || p.error, 'for', p.topic + ':' + p.partition, 'failed:', err);
            }
            // the member was replaced meanwhile (UnknownMemberId): nothing here is the new one's
            if (self.memberId !== member) {
                return null;
            }
            if (p.error) {
                // session bookkeeping happens now; a recovery (a metadata refresh) runs in the
                // background, logged if it fails: waiting for it here would hold up this round's
                // other partitions and the next fetch
                try {
                    Promise.resolve(self._partitionError(p.error, p.topic, p.partition, p.leader)).catch(_recoveryFailed);
                } catch (err) {
                    _recoveryFailed(err);
                }
                return null;
            }

            // after end() no records are handed out: their acks would not be sent, and closing
            // the session releases them
            if (self._ended || ((p.messageSet || []).length === 0 && (p.acquiredRecords || []).length === 0)) {
                return null;
            }
            delivery = self._delivered(p); // also acknowledges acquired offsets that hold no record
            if (!p.messageSet || p.messageSet.length === 0) { // only gaps: no handler
                delete self._deliveries[p.topic + ':' + p.partition];
                return null;
            }
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
            }).finally(function () {
                // the delivery is over: later acks for it are refused
                if (self._deliveries[p.topic + ':' + p.partition] === delivery) {
                    delete self._deliveries[p.topic + ':' + p.partition];
                }
            });
        }

        // each broker's records are handled as its reply arrives (not held, decompressed, until every
        // broker answers), with handlerConcurrency handlers running at most across all of them
        return Promise.all(perLeader.map(function (reply) {
            return reply.then(function (results) {
                return Promise.all(results.map(function (r) { return _slot(function () { return _dispatch(r); }); }));
            });
        }));
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
 * An assigned partition missing from cached metadata (a topic created or grown since the
 * last refresh) would never be fetched: refresh the subscribed topics, by name, first
 */
ShareConsumer.prototype._refreshMissingMetadata = function () {
    var self = this;
    var missing = Object.keys(self.assignedPartitions).some(function (topicId) {
        var name = self.client.topicNames[topicId], meta = self.client.topicMetadata[name];
        // or metadata for another topic under this name (a broker lagging behind a recreation):
        // _buildFetchRequests won't fetch it, so refresh until it matches the assignment
        return !meta || self.client.topicIds[name] !== topicId || self.assignedPartitions[topicId].some(function (p) { return !meta[p]; });
    });

    if (!missing) {
        return Promise.resolve();
    }
    return self._refreshMetadata(self._topics);
};

/**
 * Refresh metadata for topics in the background, one refresh at a time: topics asked for while one
 * runs are merged and refreshed after it, so a stalled refresh doesn't pile up a request per poll
 */
ShareConsumer.prototype._refreshMetadata = function (topics) {
    var self = this;

    topics.forEach(function (t) { self._refreshWanted[t] = true; });
    if (self._refreshing) {
        return self._refreshing;
    }
    self._refreshing = Promise.resolve().then(function _next() {
        var wanted = Object.keys(self._refreshWanted);
        self._refreshWanted = Object.create(null);
        if (wanted.length === 0 || self._ended) {
            return null;
        }
        return self.client.updateMetadata(wanted).catch(function (err) {
            self.client.warn('Metadata refresh for', wanted.join(', '), 'failed:', err);
        }).then(_next);
    }).finally(function () {
        self._refreshing = null;
    });
    return self._refreshing;
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
        // metadata (asked by name) may already be a recreated topic's: fetching its partitions under
        // this assignment's ID would mix the two; the next heartbeat brings the new assignment
        if (!topicName || !self.client.topicMetadata[topicName] || self.client.topicIds[topicName] !== topicId) { continue; }

        partitions = self.assignedPartitions[topicId];
        for (j = 0; j < partitions.length; j++) {
            leader = self.client.topicMetadata[topicName][partitions[j]];
            if (!leader) { continue; }
            leader = leader.leader;

            if (!requests[leader]) { requests[leader] = Object.create(null); } // keyed by topic name
            if (!requests[leader][topicName]) {
                requests[leader][topicName] = { topicName: topicName, topicId: topicId, partitions: [] };
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
ShareConsumer.prototype._partitionError = function (err, topic, partition, leader) {
    var self = this, meta;

    if (/ShareSessionNotFound|InvalidShareSessionEpoch/.test(err.code)) {
        // the broker lost (or never had) our session — reopen it on the next fetch
        self.client.warn('Share session lost (' + err.code + ') for', topic + ':' + partition, '- reopening session');
        // InvalidShareSessionEpoch: the session is still there, only out of step, so end() can
        // still close it (epoch -1 needs no match); ShareSessionNotFound: it is gone
        // the broker that answered: metadata refreshed since may name another leader, or none
        meta = self.client.topicMetadata[topic] && self.client.topicMetadata[topic][partition];
        leader = leader !== undefined ? leader : meta && meta.leader;
        if (leader !== undefined) {
            delete self.shareSessionEpochs[leader];
            if (err.code === 'ShareSessionNotFound') {
                delete self._reopening[leader];
            } else {
                self._reopening[leader] = true;
            }
        } else {
            self._resetShareSessions(err.code === 'ShareSessionNotFound');
        }
        return null;
    }

    // UnknownTopicId, InconsistentTopicId: the topic was recreated under a new ID
    if (/NotLeaderForPartition|LeaderNotAvailable|FencedLeaderEpoch|UnknownTopicId|InconsistentTopicId/.test(err.code)) {
        self.client.debug('Received', err.code, 'for', topic + ':' + partition);
        return self._refreshMetadata([topic]);
    }

    if (err instanceof errors.NoKafkaConnectionError) {
        self.client.debug('Received', err.toString(), 'for', topic + ':' + partition);
        return self._refreshMetadata([topic]);
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
    var self = this, i, r, key, first, last, delivery, refused = [];

    if (type === undefined) {
        type = ACKNOWLEDGE_TYPE.ACCEPT;
    }

    if (!Array.isArray(records)) {
        records = [records];
    }
    // end() has sent the final acks and closed the sessions: the broker releases the records
    if (self._ended) {
        self.client.warn('Acknowledgement after end() not sent; the records will be redelivered');
        return Promise.resolve();
    }

    for (i = 0; i < records.length; i++) {
        r = records[i];
        key = r.topic + ':' + r.partition;
        first = r.firstOffset !== undefined ? r.firstOffset : r.offset;
        last = r.lastOffset !== undefined ? r.lastOffset : r.offset;
        delivery = self._deliveries[key];
        if (!delivery || !_covered(first, last, delivery.ranges)) {
            refused.push(key + '@' + first + (last !== first ? '-' + last : ''));
            continue;
        }

        self._queueAck(r.topic, r.partition, { firstOffset: first, lastOffset: last, acknowledgeTypes: [type] }, delivery);
    }
    if (refused.length > 0) {
        self.client.warn('Not acknowledging records outside a running handler\'s delivery (they will be redelivered):',
            refused.join(', '));
    }

    return Promise.resolve();
};

/**
 * Record a partition's delivery (see _deliveries). Queued acks for offsets delivered again are for
 * an acquisition that has ended: sent with the new ones they would overlap, and the broker rejects
 * overlapping batches
 */
ShareConsumer.prototype._delivered = function (p) {
    var self = this, key = p.topic + ':' + p.partition, entry = self._pendingAcks[key], kept, before, after;
    var ranges = _mergeRanges(p.acquiredRecords || []), msgs = p.messageSet, j = 0, delivery;

    delivery = self._deliveries[key] = { topicId: p.topicId, ranges: ranges, leader: p.leader, gen: ++self._deliveryGen };
    // the partition moved to another broker: acks queued for the old one's acquisitions can't be
    // applied by the new one (it acquires afresh), so they are dropped and the records redelivered
    if (entry && entry.leader !== undefined && p.leader !== undefined && String(entry.leader) !== String(p.leader)) {
        self.client.warn('Dropped queued acknowledgements for', key, '- it moved from broker', entry.leader, 'to', p.leader);
        delete self._pendingAcks[key];
        entry = null;
    }
    if (entry) {
        kept = entry.batches.map(function (b) { return _outside(b, ranges); }).flat(); // no apply: any length
        before = entry.batches.reduce(function (n, b) { return n + b.lastOffset - b.firstOffset + 1; }, 0);
        after = kept.reduce(function (n, b) { return n + b.lastOffset - b.firstOffset + 1; }, 0);
        if (after < before) {
            self.client.warn('Dropped queued acknowledgements for', before - after, 'record(s) of', key, '- delivered again');
        }
        if (kept.length > 0) {
            entry.batches = kept;
        } else {
            delete self._pendingAcks[key];
        }
    }
    // acquired offsets that hold no record (compacted away, control records) are acknowledged as
    // gaps, as Java's ShareCompletedFetch does: no handler sees them, and unacknowledged they would
    // stay locked until they expire
    function _gap(first, last) {
        self._queueAck(p.topic, p.partition, { firstOffset: first, lastOffset: last, acknowledgeTypes: [ACKNOWLEDGE_TYPE.GAP], _gap: true }, delivery);
    }
    if (Array.isArray(msgs)) {
        ranges.forEach(function (r) {
            var next = r.firstOffset;
            while (j < msgs.length && msgs[j].offset <= r.lastOffset) {
                if (msgs[j].offset > next) {
                    _gap(next, msgs[j].offset - 1);
                }
                next = Math.max(next, msgs[j].offset + 1);
                j++;
            }
            if (next <= r.lastOffset) {
                _gap(next, r.lastOffset);
            }
        });
    }
    return delivery;
};

/**
 * Queue an ack batch for a delivery, tagged with its topic ID and broker
 */
ShareConsumer.prototype._queueAck = function (topicName, partition, batch, delivery) {
    var self = this, key = topicName + ':' + partition, entry = self._pendingAcks[key];

    if (!entry) {
        entry = self._pendingAcks[key] = { topicName: topicName, partition: partition, leader: delivery.leader, batches: [] };
    }
    batch.topicId = delivery.topicId;
    batch._gen = delivery.gen; // the delivery it acknowledges (see _drainPendingAcks)
    entry.batches.push(batch);
};

// the broker a queued entry's acks go to: the one that delivered its records (or, for one queued
// without a delivery broker, the partition's current leader)
ShareConsumer.prototype._ackLeader = function (entry) {
    var meta = this.client.topicMetadata[entry.topicName] && this.client.topicMetadata[entry.topicName][entry.partition];
    return entry.leader !== undefined ? entry.leader : meta && meta.leader;
};

/**
 * Put drained acks back into the pending queue (used when the fetch carrying
 * them failed). Drained batches go in FRONT of any acks queued meanwhile.
 */
ShareConsumer.prototype._requeueAcks = function (acks, leader) {
    var self = this;

    Object.keys(acks).forEach(function (topicName) {
        Object.keys(acks[topicName]).forEach(function (partition) {
            var key = topicName + ':' + partition;
            if (!self._pendingAcks[key]) {
                self._pendingAcks[key] = {
                    topicName: topicName,
                    partition: Number(partition),
                    leader: leader,
                    batches: []
                };
            }
            self._pendingAcks[key].batches = acks[topicName][partition].concat(self._pendingAcks[key].batches);
        });
    });
};

/**
 * Drain pending acks (those `include(entry)` accepts, if given) into the format
 * expected by shareFetchRequest
 * Returns { topicName: { partition: [{firstOffset, lastOffset, acknowledgeTypes}] } } or null
 */
ShareConsumer.prototype._drainPendingAcks = function (oldestOnly, include) {
    var self = this, keys, i, entry, acks = null, gen, take, keep;

    if (typeof oldestOnly === 'function') {
        include = oldestOnly;
        oldestOnly = false;
    }
    keys = Object.keys(self._pendingAcks);
    for (i = 0; i < keys.length; i++) {
        entry = self._pendingAcks[keys[i]];
        if (include && !include(entry)) {
            continue;
        }
        // a fetch carries one delivery's acks per partition: the broker rejects a partition's acks as a
        // whole if one is stale (its acquisition expired, say before a reassignment), which must not
        // take the fresh ones down with it. Later deliveries' acks wait for the next fetch
        take = entry.batches;
        keep = [];
        if (oldestOnly) {
            gen = _oldestGen(entry.batches);
            take = entry.batches.filter(_ofGen(gen, true));
            keep = entry.batches.filter(_ofGen(gen, false));
        }
        if (!acks) {
            acks = Object.create(null); // keyed by topic name
        }
        if (!acks[entry.topicName]) {
            acks[entry.topicName] = {};
        }
        acks[entry.topicName][entry.partition] = _forTheWire(take);
        if (keep.length > 0) {
            entry.batches = keep;
        } else {
            delete self._pendingAcks[keys[i]];
        }
    }

    return acks;
};

/**
 * Send the acks still queued with ShareAcknowledge at epoch -1, which closes each broker's
 * share session (accepted without an epoch match; the broker releases what remains
 * acquired). Only a broker with an open session can take them: the rest are dropped,
 * and their records are redelivered once their acquisition lock expires
 */
ShareConsumer.prototype._sendFinalAcks = function (leader) {
    var self = this, acks, parts = [], rounds = 1, epoch = self.shareSessionEpochs[leader] || 0, chain = Promise.resolve(), i;

    // every open session gets the epoch -1 request, acks or not: closing it releases the
    // records still acquired (a handler's still running) now, not when their locks expire
    if (!self._sessionOpen(leader)) {
        return Promise.resolve();
    }

    acks = self._drainPendingAcks(function (entry) {
        return String(self._ackLeader(entry)) === String(leader); // the broker that granted the acquisition
    });
    Object.keys(acks || {}).forEach(function (topicName) {
        Object.keys(acks[topicName]).forEach(function (partition) {
            var byGen = _byGen(acks[topicName][partition]);
            parts.push({ topicName: topicName, partition: Number(partition), byGen: byGen });
            rounds = Math.max(rounds, byGen.length);
        });
    });

    // a partition's acks from different deliveries go in separate requests (as on fetches: a stale one
    // makes the broker reject the partition's acks as a whole), its latest with the session close.
    // The earlier ones go at the session's epoch; one being reopened has none, and they are dropped
    function _request(round) {
        var topics = [], request = {};
        parts.forEach(function (part) {
            var batches = part.byGen[part.byGen.length - rounds + round], topic;
            if (!batches) {
                return;
            }
            // under the topic ID of the delivery (metadata may lag a recreation); one delivery's batches share it
            topic = topics.find(function (t) { return t.topicName === part.topicName && t.topicId === batches[0].topicId; });
            if (!topic) {
                topic = { topicName: part.topicName, topicId: batches[0].topicId, partitions: [] };
                topics.push(topic);
            }
            topic.partitions.push({ partitionIndex: part.partition, acknowledgementBatches: batches });
        });
        request[leader] = topics;
        return request;
    }
    function _report(results) {
        results.forEach(function (r) {
            if (r.error) {
                self.client.warn(r.topic === undefined ? 'Closing the share session on broker ' + leader + ' failed (its'
                    + ' records are redelivered once their locks expire):' : 'Acknowledgement on close failed for ' + r.topic
                    + ':' + r.partition + ':', r.error);
            }
        });
    }

    // the leave has started (end() past the acks' deadline): sending more would race it
    function _tooLate() {
        if (self._leaving) {
            self.client.warn('Broker', leader, 'missed the acknowledgement deadline on close: its remaining acknowledgements are not sent');
        }
        return self._leaving;
    }
    function _earlier(round) {
        return function () {
            if (_tooLate()) {
                return null;
            }
            return self.client.shareAcknowledgeRequest(self.options.groupId, self.memberId, epoch + round, _request(round)).then(_report);
        };
    }
    if (rounds > 1 && !(epoch > 0)) {
        self.client.warn('Dropped acknowledgements from earlier deliveries on broker', leader, 'on close: its session is being reopened');
    } else {
        for (i = 0; i < rounds - 1; i++) {
            chain = chain.then(_earlier(i));
        }
    }
    return chain.then(function () {
        if (_tooLate()) {
            return null;
        }
        return self.client.shareAcknowledgeRequest(self.options.groupId, self.memberId, -1, _request(rounds - 1)).then(_report);
    });
};

/**
 * Leave the share group and close all connections
 *
 * @return {Promise}
 */
ShareConsumer.prototype.end = function () {
    var self = this, started = Date.now(), inFlight = self._fetchRequests || {}, leaders = {}, acked, late = false;

    // a second caller (say a handler and a shutdown hook) shares the first one's shutdown, rather
    // than closing the client under its final acks
    if (self._endPromise) {
        return self._endPromise;
    }
    self._ended = true; // acknowledge() takes no more acks: the final ones are drained below
    self._closed = true;
    clearTimeout(self._heartbeatTimeout);
    clearTimeout(self._fetchTimeout);

    // As Java's close: send the acks made so far, then leave. Handlers still running aren't waited
    // for (one may be the caller, waiting on end()): what they ack later is not sent, and the
    // epoch -1 request releases their records for redelivery. The acks, then the leave, are bounded
    // (ACK_WAIT_MS, END_WAIT_MS): a broker that never answers, or a metadata refresh retrying until
    // the client ends (which every request waits on), must not hang end()
    Object.keys(self.shareSessionEpochs).concat(Object.keys(self._reopening), Object.keys(inFlight)).forEach(function (l) {
        leaders[l] = true;
    });
    // per broker: its request in flight carries acks (if it fails they are requeued), then its final
    // acks and session close; a silent broker holds up only itself
    acked = Promise.all(Object.keys(leaders).map(function (l) {
        return Promise.resolve(inFlight[l]).then(function () {
            // its fetch settled after the acks' deadline: the leave has begun, too late to send
            if (late) {
                self.client.warn('Broker', l, 'answered after the acknowledgement deadline on close: its final acknowledgements are not sent');
                return null;
            }
            return self._sendFinalAcks(l);
        }).catch(function (err) {
            self.client.warn('Failed to send share acknowledgements on close to broker', l + ':', err);
        });
    }));

    self._endPromise = _within(acked, ACK_WAIT_MS).then(function () {
        var dropped = Object.keys(self._pendingAcks);
        late = true;
        if (dropped.length > 0) {
            self.client.warn('No open share session to acknowledge', dropped.join(', '), 'on close');
        }
        // only send a leave heartbeat if we actually joined (broker assigned an epoch); a join that
        // lands from here on leaves by itself
        self._leaving = true;
        if (self.memberEpoch > 0) {
            return _within(self.client.shareGroupHeartbeatRequest(self.options.groupId, self.memberId, -1, null, null)
            .catch(function (err) {
                self.client.warn('Failed to send share group leave heartbeat:', err);
            }), Math.max(0, END_WAIT_MS - (Date.now() - started)));
        }
        return null;
    }).then(function () {
        return self.client.end();
    });
    return self._endPromise;
};

ShareConsumer.ACKNOWLEDGE_TYPE = ACKNOWLEDGE_TYPE;
