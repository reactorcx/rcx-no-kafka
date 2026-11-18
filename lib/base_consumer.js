'use strict';

var utils        = require('./utils');
var Client       = require('./client');
var Kafka        = require('./index');
var errors       = require('./errors');
var promiseUtils = require('./promise-utils');

function BaseConsumer(options) {
    this.options = utils.defaultsDeep(options || {}, {
        maxWaitTime: 100,
        idleTimeout: 1000, // timeout between fetch requests
        minBytes: 1,
        maxBytes: 1024 * 1024,
        recoveryOffset: Kafka.LATEST_OFFSET,
        handlerConcurrency: 10,
        isolationLevel: 0
    });

    this.idleTimeout = this.options.idleTimeout;
    this.client = new Client(this.options);

    this.subscriptions = {};
    this._closed = false;
}

module.exports = BaseConsumer;

/**
 * Initialize BaseConsumer
 *
 * @return {Prommise}
 */
BaseConsumer.prototype.init = function () {
    var self = this;
    return this.client.init().then(function () {
        // the fetch loop self-perpetuates; a second init() must not double it
        if (!self._closed && !self._fetchStarted) {
            self._fetchStarted = true;
            self._fetchPromise = self._fetch();
        }
    });
};

BaseConsumer.prototype._fetch = function () {
    var self = this;

    return Promise.resolve().then(function () {
        var subs = Object.keys(self.subscriptions), data = {}, sent = {}, i, entry, leaderSubs, topics, topicKeys, j;
        for (i = 0; i < subs.length; i++) {
            entry = self.subscriptions[subs[i]];
            if (entry.paused || self._recreated(entry)) { continue; }
            sent[subs[i]] = entry;
            if (!data[entry.leader]) { data[entry.leader] = Object.create(null); } // keyed by topic name
            if (!data[entry.leader][entry.topic]) { data[entry.leader][entry.topic] = []; } // one topic ID (see _recreated)
            data[entry.leader][entry.topic].push(entry);
        }
        // convert inner topic objects to arrays of {topicName, partitions}
        leaderSubs = Object.keys(data);
        for (i = 0; i < leaderSubs.length; i++) {
            topics = data[leaderSubs[i]];
            topicKeys = Object.keys(topics);
            data[leaderSubs[i]] = [];
            for (j = 0; j < topicKeys.length; j++) {
                // by the topic ID of its offsets: metadata refreshed before it is sent may name a recreated topic
                data[leaderSubs[i]].push({ topicName: topicKeys[j], topicId: topics[topicKeys[j]][0].topicId, partitions: topics[topicKeys[j]] });
            }
        }

        if (Object.keys(data).length === 0) {
            return null;
        }

        return self.client.fetchRequest(data).then(function (results) {
            // a subscription without a topic ID takes the one its fetch went by (see Client.fetchRequest)
            leaderSubs.forEach(function (l) {
                data[l].forEach(function (t) {
                    t.partitions.forEach(function (s) {
                        if (s.topicId === undefined) { s.topicId = t.topicId; }
                    });
                });
            });
            return promiseUtils.mapConcurrent(results, function (p) {
                var s = self.subscriptions[p.topic + ':' + p.partition], nextOffset;
                if (!s || s !== sent[p.topic + ':' + p.partition]) {
                    // unsubscribed, or re-subscribed (e.g. by a rebalance) at another offset,
                    // while we were polling: this response belongs to the old entry
                    return null;
                }
                // KIP-392: if broker suggests a preferred read replica, update the leader
                if (p.preferredReadReplica >= 0 && p.preferredReadReplica !== s.leader &&
                    self.client.brokerConnections[p.preferredReadReplica]) {
                    s.leader = p.preferredReadReplica;
                }
                if (p.error) {
                    return self._partitionError(p.error, p.topic, p.partition);
                }
                // RecordBatch fetches report nextOffset (past markers, aborted and
                // compacted records); legacy MessageSet fetches don't
                nextOffset = p.nextOffset;
                if (nextOffset === undefined) {
                    nextOffset = p.messageSet.length ? p.messageSet[p.messageSet.length - 1].offset + 1 : s.offset;
                }
                if (!p.messageSet.length && nextOffset > s.offset) {
                    s.offset = nextOffset; // nothing deliverable (e.g. only a transaction marker)
                }
                if (p.messageSet.length) {
                    s.paused = true;
                    return s.handler(p.messageSet, p.topic, p.partition, p.highwaterMarkOffset)
                    .then(function () {
                        // only advance the entry this handler ran for, not a newer one under the same key
                        if (self.subscriptions[p.topic + ':' + p.partition] === s) {
                            s.offset = nextOffset; // advance offset position
                        }
                    }, function (err) {
                        // the offset is NOT advanced on handler failure — the batch
                        // will be redelivered on the next fetch (at-least-once)
                        self.client.warn('Handler for', p.topic + ':' + p.partition, 'failed with', err, '- batch will be redelivered');
                    })
                    .then(function () {
                        if (self.subscriptions[p.topic + ':' + p.partition] === s) {
                            s.paused = false;
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
        }, self.idleTimeout);
    });
};

/**
 * The topic was deleted and recreated under its name since this subscription's offset was taken (a
 * new topic ID in metadata): the offset is a position in the deleted log, and fetched by the new ID
 * it would skip the new log's first records. It restarts at options.recoveryOffset, as an offset out
 * of range does; the fetch skips it meanwhile. A failed lookup is retried on the next fetch
 */
BaseConsumer.prototype._recreated = function (s) {
    var self = this, key = s.topic + ':' + s.partition, id = self.client.topicIds[s.topic];

    if (s.offset === undefined) { // not subscribed yet (no leader then): its own options resolve the offset, as retried
        return false;
    }
    if (s.topicId === undefined) { // subscribed before the brokers gave topic IDs: the first one is its log's
        s.topicId = id;
    }
    if (id === undefined || s.topicId === id) {
        return false;
    }
    s.paused = true;
    self.client.warn(key, 'was recreated (topic ID', s.topicId, '->', id + '): resuming at recoveryOffset');
    (function _lookup() {
        var asked = self.client.topicIds[s.topic]; // ListOffsets goes by name: the log it answers for
        return self.client.findLeader(s.topic, s.partition).then(function (leader) {
            return self.client.getPartitionOffset(leader, s.topic, s.partition, self.options.recoveryOffset).then(function (offset) {
                return { leader: leader, offset: offset };
            });
        }).then(function (r) {
            if (self.client.topicIds[s.topic] !== asked) { // metadata changed meanwhile (its leader too, maybe): again
                return _lookup();
            }
            if (self.subscriptions[key] === s) {
                s.leader = r.leader;
                s.offset = r.offset;
                s.topicId = asked;
            }
            return null;
        });
    }()).catch(function (err) {
        self.client.warn('Failed to find the recovery offset of recreated', key + ':', err);
    }).then(function () {
        if (self.subscriptions[key] === s) {
            s.paused = false;
        }
    });
    return true;
};

BaseConsumer.prototype._partitionError = function (err, topic, partition) {
    var self = this;

    var s = self.subscriptions[topic + ':' + partition];

    if (err.code === 'OffsetOutOfRange') { // update partition offset to options.recoveryOffset
        self.client.warn('Updating offset because of OffsetOutOfRange error for', topic + ':' + partition);
        return self.client.getPartitionOffset(s.leader, topic, partition, self.options.recoveryOffset).then(function (offset) {
            s.offset = offset;
        });
    } else if (err.code === 'MessageSizeTooLarge') {
        self.client.warn('Received MessageSizeTooLarge error for', topic + ':' + partition,
            'which means maxBytes option value (' + s.maxBytes + ') is too small to fit the message at offset', s.offset);
        s.offset += 1;
        return null;
    /* istanbul ignore next */
    } else if (/UnknownTopicOrPartition|NotLeaderForPartition|LeaderNotAvailable|FencedLeaderEpoch/.test(err.code)) {
        self.client.debug('Received', err.code, 'error for', topic + ':' + partition);
        return self._updateSubscription(topic, partition);
    /* istanbul ignore next */
    } else if (err instanceof errors.NoKafkaConnectionError) {
        self.client.debug('Received', err.toString(), 'for', topic + ':' + partition);
        return self._updateSubscription(topic, partition).catch(function (e) {
            if (e instanceof errors.NoKafkaConnectionError) {
                // throttle re-connection attempts (if options.idleTimeout is < 1000ms)
                self.idleTimeout = 1000;
                return;
            }
            throw e;
        });
    }
    self.client.warn('Received', err.code || err, 'error for', topic + ':' + partition);
    return self._updateSubscription(topic, partition);
};

/* istanbul ignore next */
BaseConsumer.prototype._updateSubscription = function (topic, partition) {
    var self = this;

    return self.client.updateMetadata([topic]).then(function () {
        var s = self.subscriptions[topic + ':' + partition];

        if (!s) { return null; } // already unsubscribed

        // an offset kept keeps the topic ID of its log: metadata refreshed just now may name a recreated topic
        return self.subscribe(topic, partition, s.offset !== undefined ? { offset: s.offset, _topicId: s.topicId } : s.options, s.handler)
        .catch(function (err) {
            self.client.error('Failed to re-subscribe to', topic + ':' + partition, err);
        });
    })
    .then(function (r) {
        self.idleTimeout = self.options.idleTimeout;
        return r;
    });
};

/**
 * Get offset for specified topic/partition and time
 *
 * @param  {String} topic
 * @param  {Number} partition
 * @param  {Number} time      Time in ms or two specific values: Kafka.EARLIEST_OFFSET and Kafka.LATEST_OFFSET
 * @return {Promise}
 */
BaseConsumer.prototype.offset = function (topic, partition, time) {
    var self = this;

    partition = partition || 0;

    return self.client.findLeader(topic, partition).then(function (leader) {
        return self.client.getPartitionOffset(leader, topic, partition, time);
    });
};


/**
 * Subscribe to topic/partition
 * @param  {String} topic
 * @param  {Array} [partitions]
 * @param  {Object} [options] { maxBytes, offset, time }
 * @param  {Function} handler
 * @return {Promise}
 */
BaseConsumer.prototype.subscribe = function (topic, partitions, options, handler) {
    var self = this;

    if (typeof partitions === 'function') {
        handler = partitions;
        partitions = [];
        options = {};
    } else if (typeof options === 'function') {
        handler = options;
        if (typeof partitions === 'object' && partitions !== null && !Array.isArray(partitions)) {
            options = partitions;
            partitions = [];
        } else {
            options = {};
        }
    }

    if (typeof handler !== 'function') {
        return Promise.reject(new Error('Missing data handler for subscription'));
    }

    if (!Array.isArray(partitions)) {
        partitions = [partitions];
    }

    if (partitions.length === 0) {
        partitions = self.client.getTopicPartitions(topic).then(function (parts) {
            return parts.map(function (p) { return p.partitionId; });
        });
    }

    function _subscribe(partition, leader, offset, topicId) {
        self.client.debug('Subscribed to', topic + ':' + partition, 'offset', offset, 'leader', self.client.leaderServer(leader));
        self.subscriptions[topic + ':' + partition] = {
            topic: topic,
            partition: partition,
            offset: offset,
            topicId: topicId, // the log the offset is in (see _recreated)
            leader: leader,
            maxBytes: options.maxBytes || self.options.maxBytes,
            handler: handler
        };
        return offset;
    }

    return Promise.resolve(partitions).then(function (parts) {
        // wrap handler so it always returns a Promise
        var originalHandler = handler;
        // re-subscribes (after every partition error) pass the wrapped handler back in;
        // wrapping again each time nests until the call stack overflows
        if (!originalHandler._promiseWrapped) {
            handler = function () {
                try {
                    return Promise.resolve(originalHandler.apply(this, arguments));
                } catch (e) {
                    return Promise.reject(e);
                }
            };
            handler._promiseWrapped = true;
        }

        return Promise.all(parts.map(function _lookup(partition) {
            return self.client.findLeader(topic, partition).then(function (leader) {
                var id = self.client.topicIds[topic];
                if (options.offset >= 0) {
                    return _subscribe(partition, leader, options.offset, options._topicId !== undefined ? options._topicId : id);
                }
                return self.client.getPartitionOffset(leader, topic, partition, options.time).then(function (offset) {
                    // the topic was recreated while it was looked up: which log the offset is in is unknown
                    if (self.client.topicIds[topic] !== id) {
                        return _lookup(partition);
                    }
                    return _subscribe(partition, leader, offset, id);
                });
            })
            .catch(function (err) {
                if (err && err.code === 'LeaderNotAvailable') {
                    // these subscriptions will be retried on each _fetch()
                    self.subscriptions[topic + ':' + partition] = {
                        topic: topic,
                        partition: partition,
                        options: options,
                        leader: -1,
                        handler: handler
                    };
                    return;
                }
                throw err;
            });
        }));
    });
};

/**
 * Unsubscribe from topic/partition
 *
 * @param  {String} topic
 * @param  {Array} partitions
 * @return {Promise}
 */
BaseConsumer.prototype.unsubscribe = function (topic, partitions) {
    var self = this;

    if (partitions === undefined) {
        partitions = [];
    }

    if (!Array.isArray(partitions)) {
        partitions = [partitions];
    }

    if (partitions.length === 0) {
        if (!self.client.topicMetadata[topic]) {
            return Promise.resolve(); // unknown topic — nothing subscribed
        }
        partitions = Object.keys(self.client.topicMetadata[topic]).map(function (k) { return parseInt(k); });
    }

    partitions.forEach(function (partition) {
        delete self.subscriptions[topic + ':' + partition];
    });
    return Promise.resolve();
};

BaseConsumer.prototype.end = function () {
    var self = this;

    self.subscriptions = {};
    self._closed = true;
    clearTimeout(self._fetchTimeout);

    return self.client.end();
};
