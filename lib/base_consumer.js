'use strict';

var utils        = require('./utils');
var Client       = require('./client');
var Kafka        = require('./index');
var errors       = require('./errors');
var promiseUtils = require('./promise-utils');

function BaseConsumer(options) {
    this.options = utils.defaultsDeep(options || {}, {
        maxWaitTime: 100,
        idleTimeout: 1000, // delay before the next fetch after an idle, failed or errored round
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
    this._fetchRound = 0; // which of a broker's partitions goes first in a fetch
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
    var self = this, progressed = false, errored = false;

    return Promise.resolve().then(function () {
        var subs = Object.keys(self.subscriptions), data = {}, sent = {}, i, entry, leaderSubs, topics, list, j, k;
        var refreshes = Object.create(null); // a topic's metadata refresh, shared by its partitions this round
        for (i = 0; i < subs.length; i++) {
            entry = self.subscriptions[subs[i]];
            if (entry.paused) { continue; }
            sent[subs[i]] = entry;
            (data[entry.leader] = data[entry.leader] || []).push(entry);
        }
        // each broker's partitions as [{topicName, partitions}], starting one further each round: a
        // broker fills a response in request order, so a backlogged first partition took all of it
        leaderSubs = Object.keys(data);
        for (i = 0; i < leaderSubs.length; i++) {
            list = data[leaderSubs[i]];
            k = self._fetchRound % list.length;
            topics = Object.create(null); // keyed by topic name
            data[leaderSubs[i]] = [];
            for (j = 0; j < list.length; j++) {
                entry = list[(j + k) % list.length];
                if (!topics[entry.topic]) {
                    topics[entry.topic] = { topicName: entry.topic, partitions: [] };
                    data[leaderSubs[i]].push(topics[entry.topic]);
                }
                topics[entry.topic].partitions.push(entry);
            }
        }
        self._fetchRound++;

        if (Object.keys(data).length === 0) {
            return null;
        }

        return self.client.fetchRequest(data).then(function (results) {
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
                    errored = true;
                    return self._partitionError(p.error, p.topic, p.partition, refreshes);
                }
                // RecordBatch fetches report nextOffset (past markers, aborted and
                // compacted records); legacy MessageSet fetches don't
                nextOffset = p.nextOffset;
                if (nextOffset === undefined) {
                    nextOffset = p.messageSet.length ? p.messageSet[p.messageSet.length - 1].offset + 1 : s.offset;
                }
                if (!p.messageSet.length && nextOffset > s.offset) {
                    s.offset = nextOffset; // nothing deliverable (e.g. only a transaction marker)
                    progressed = true;
                }
                if (p.messageSet.length) {
                    s.paused = true;
                    return s.handler(p.messageSet, p.topic, p.partition, p.highwaterMarkOffset)
                    .then(function () {
                        // only advance the entry this handler ran for, not a newer one under the same key
                        if (self.subscriptions[p.topic + ':' + p.partition] === s) {
                            progressed = progressed || nextOffset > s.offset;
                            s.offset = nextOffset; // advance offset position
                        }
                    }, function (err) {
                        // the offset is NOT advanced on handler failure — the batch
                        // will be redelivered on the next fetch (at-least-once), after idleTimeout
                        errored = true;
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
        errored = true;
        self.client.error(err);
    })
    .then(function () {
        if (self._closed) { return; }
        // a round that moved an offset forward fetches again at once, as Java's poll() does (the
        // broker waits maxWaitTime when there is nothing): sleeping after a full round capped a
        // consumer at a response per idleTimeout. An idle, failed or errored round waits
        self._fetchTimeout = setTimeout(function () {
            if (self._closed) { return; }
            self._fetchPromise = self._fetch();
        }, progressed && !errored ? 0 : self.idleTimeout);
    });
};

BaseConsumer.prototype._partitionError = function (err, topic, partition, refreshes) {
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
        return self._updateSubscription(topic, partition, refreshes);
    /* istanbul ignore next */
    } else if (err instanceof errors.NoKafkaConnectionError) {
        self.client.debug('Received', err.toString(), 'for', topic + ':' + partition);
        return self._updateSubscription(topic, partition, refreshes).catch(function (e) {
            if (e instanceof errors.NoKafkaConnectionError) {
                // throttle re-connection attempts (if options.idleTimeout is < 1000ms)
                self.idleTimeout = 1000;
                return;
            }
            throw e;
        });
    }
    self.client.warn('Received', err.code || err, 'error for', topic + ':' + partition);
    return self._updateSubscription(topic, partition, refreshes);
};

/* istanbul ignore next */
BaseConsumer.prototype._updateSubscription = function (topic, partition, refreshes) {
    var self = this, refresh;

    // one refresh per topic per fetch round: its partitions fail together after a leader moves
    refresh = refreshes ? (refreshes[topic] = refreshes[topic] || self.client.updateMetadata([topic])) : self.client.updateMetadata([topic]);
    return refresh.then(function () {
        var s = self.subscriptions[topic + ':' + partition];

        if (!s) { return null; } // already unsubscribed

        return self.subscribe(topic, partition, s.offset !== undefined ? { offset: s.offset } : s.options, s.handler)
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

    function _subscribe(partition, leader, offset) {
        self.client.debug('Subscribed to', topic + ':' + partition, 'offset', offset, 'leader', self.client.leaderServer(leader));
        self.subscriptions[topic + ':' + partition] = {
            topic: topic,
            partition: partition,
            offset: offset,
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

        return Promise.all(parts.map(function (partition) {
            return self.client.findLeader(topic, partition).then(function (leader) {
                if (options.offset >= 0) {
                    return _subscribe(partition, leader, options.offset);
                }
                return self.client.getPartitionOffset(leader, topic, partition, options.time).then(function (offset) {
                    return _subscribe(partition, leader, offset);
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
