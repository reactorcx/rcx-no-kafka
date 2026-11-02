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
        self._fetchPromise = self._fetch();
    });
};

BaseConsumer.prototype._fetch = function () {
    var self = this;

    return Promise.resolve().then(function () {
        var subs = Object.keys(self.subscriptions), data = {}, i, entry, leaderSubs, topics, topicKeys, j;
        for (i = 0; i < subs.length; i++) {
            entry = self.subscriptions[subs[i]];
            if (entry.paused) { continue; }
            if (!data[entry.leader]) { data[entry.leader] = {}; }
            if (!data[entry.leader][entry.topic]) { data[entry.leader][entry.topic] = []; }
            data[entry.leader][entry.topic].push(entry);
        }
        // convert inner topic objects to arrays of {topicName, partitions}
        leaderSubs = Object.keys(data);
        for (i = 0; i < leaderSubs.length; i++) {
            topics = data[leaderSubs[i]];
            topicKeys = Object.keys(topics);
            data[leaderSubs[i]] = [];
            for (j = 0; j < topicKeys.length; j++) {
                data[leaderSubs[i]].push({ topicName: topicKeys[j], partitions: topics[topicKeys[j]] });
            }
        }

        if (Object.keys(data).length === 0) {
            return null;
        }

        return self.client.fetchRequest(data).then(function (results) {
            return promiseUtils.mapConcurrent(results, function (p) {
                var s = self.subscriptions[p.topic + ':' + p.partition];
                if (!s) {
                    return null; // already unsubscribed while we were polling
                }
                // KIP-392: if broker suggests a preferred read replica, update the leader
                if (p.preferredReadReplica >= 0 && p.preferredReadReplica !== s.leader &&
                    self.client.brokerConnections[p.preferredReadReplica]) {
                    s.leader = p.preferredReadReplica;
                }
                if (p.error) {
                    return self._partitionError(p.error, p.topic, p.partition);
                }
                if (p.messageSet.length) {
                    s.paused = true;
                    return s.handler(p.messageSet, p.topic, p.partition, p.highwaterMarkOffset)
                    .catch(function (err) {
                        self.client.warn('Handler for', p.topic + ':' + p.partition, 'failed with', err);
                    })
                    .then(function () {
                        // Only advance the entry this handler was started for. If a rebalance
                        // dropped the partition and a later subscribe created a new entry under
                        // the same key while this handler ran, that entry tracks its own position
                        // and may have its own handler in flight: unpausing it or moving its
                        // offset would deliver the partition concurrently and out of order.
                        if (self.subscriptions[p.topic + ':' + p.partition] === s) {
                            s.paused = false;
                            s.offset = p.messageSet[p.messageSet.length - 1].offset + 1; // advance offset position
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

        // Guarded by `s`: if a rebalance drops this partition while the leader lookup is in
        // flight, the result is discarded rather than written back (see _subscribePartition).
        // s.handler is already wrapped by the subscribe() that created the entry.
        return self._subscribePartition(topic, partition,
            s.offset !== undefined ? { offset: s.offset, maxBytes: s.maxBytes } : s.options, s.handler, s)
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

    return Promise.resolve(partitions).then(function (parts) {
        // wrap handler so it always returns a Promise
        var originalHandler = handler;
        handler = function () {
            try {
                return Promise.resolve(originalHandler.apply(this, arguments));
            } catch (e) {
                return Promise.reject(e);
            }
        };

        return Promise.all(parts.map(function (partition) {
            return self._subscribePartition(topic, partition, options, handler);
        }));
    });
};

/**
 * Resolve the leader (and offset, when not given) for one partition and store its subscription.
 *
 * With `expected`, this is a re-subscribe of that existing entry, and the result is applied only if
 * `self.subscriptions` still holds that same entry once the lookups resolve. Both lookups are
 * asynchronous, and a rebalance can unsubscribe the partition — or hand it to another member —
 * while they are in flight. Storing unconditionally would resurrect the revoked partition at its
 * old position, and nothing would remove it again: the group's revoke diff is computed from the
 * assignment, not from `subscriptions`, so this member would keep fetching and committing a
 * partition its new owner is consuming too. Seen in production during a rolling broker restart,
 * where fetches fail with NotLeaderForPartition at the same time as the coordinator moves.
 *
 * An `expected` entry that already has an offset is refreshed in place (only its leader changes)
 * rather than replaced. The fetch loop is not guaranteed to wait for the re-subscribe: mapConcurrent
 * fails fast, so one partition's error-recovery rejecting starts the next fetch round while
 * another's is still pending. That round can advance this entry's offset and pause it for a running
 * handler, and replacing the entry would rewind it to the offset captured when the re-subscribe
 * began and clear `paused`, re-delivering the batch concurrently. Refreshing in place keeps the
 * current offset, `paused`, `maxBytes` and handler.
 *
 * @param  {String}   topic
 * @param  {Number}   partition
 * @param  {Object}   options   { maxBytes, offset, time }
 * @param  {Function} handler   already wrapped to return a Promise
 * @param  {Object}   [expected] the subscription entry being refreshed
 * @return {Promise}
 */
BaseConsumer.prototype._subscribePartition = function (topic, partition, options, handler, expected) {
    var self = this, key = topic + ':' + partition,
        // An entry that already tracks a position is refreshed in place rather than replaced.
        inPlace = expected !== undefined && expected.offset !== undefined;

    // True when this is a re-subscribe and the entry it was started for is no longer current.
    function _stale() {
        if (expected !== undefined && self.subscriptions[key] !== expected) {
            self.client.debug('Dropping re-subscribe of', key, '- it was unsubscribed or replaced meanwhile');
            return true;
        }
        return false;
    }

    function _store(entry) {
        if (_stale()) { return false; }
        self.subscriptions[key] = entry;
        return true;
    }

    function _subscribe(leader, offset) {
        if (inPlace) {
            if (_stale()) { return offset; }
            expected.leader = leader;
            self.client.debug('Refreshed', key, 'offset', expected.offset, 'leader', self.client.leaderServer(leader));
            return expected.offset;
        }
        if (_store({
            topic: topic,
            partition: partition,
            offset: offset,
            leader: leader,
            maxBytes: options.maxBytes || self.options.maxBytes,
            handler: handler
        })) {
            self.client.debug('Subscribed to', key, 'offset', offset, 'leader', self.client.leaderServer(leader));
        }
        return offset;
    }

    return self.client.findLeader(topic, partition).then(function (leader) {
        if (options.offset >= 0) {
            return _subscribe(leader, options.offset);
        }
        return self.client.getPartitionOffset(leader, topic, partition, options.time).then(function (offset) {
            return _subscribe(leader, offset);
        });
    })
    .catch(function (err) {
        if (err && err.code === 'LeaderNotAvailable') {
            // these subscriptions will be retried on each _fetch()
            if (inPlace) {
                // Keep the entry's offset, paused state and maxBytes; only the leader is unknown.
                // The next _fetch fails against leader -1 and retries through _updateSubscription.
                if (!_stale()) { expected.leader = -1; }
                return;
            }
            _store({
                topic: topic,
                partition: partition,
                options: options,
                leader: -1,
                handler: handler
            });
            return;
        }
        throw err;
    });
};

/**
 * Currently subscribed partitions, parsed from the `topic:partition` keys of `self.subscriptions`.
 * Kafka topic names cannot contain ':', so splitting the key is safe.
 *
 * @return {Array} [{topic, partition}, ...]
 */
BaseConsumer.prototype._subscribedPartitions = function () {
    return Object.keys(this.subscriptions).map(function (key) {
        var parts = key.split(':');
        return { topic: parts[0], partition: Number(parts[1]) };
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
