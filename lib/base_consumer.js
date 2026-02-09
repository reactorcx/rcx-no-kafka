'use strict';

var _            = require('lodash');
var Client       = require('./client');
var Kafka        = require('./index');
var errors       = require('./errors');
var promiseUtils = require('./promise-utils');

function BaseConsumer(options) {
    this.options = _.defaultsDeep(options || {}, {
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
    this._fetchPromise = this._fetch();
    return this.client.init();
};

BaseConsumer.prototype._fetch = function () {
    var self = this;

    return Promise.resolve().then(function () {
        var data = _(self.subscriptions).reject({ paused: true }).values().groupBy('leader').mapValues(function (v) {
            return _(v)
                .groupBy('topic')
                .map(function (p, t) {
                    return {
                        topicName: t,
                        partitions: p
                    };
                })
                .value();
        }).value();

        if (_.isEmpty(data)) {
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
                        var sub = self.subscriptions[p.topic + ':' + p.partition];
                        if (sub) {
                            sub.paused = false;
                            sub.offset = _.last(p.messageSet).offset + 1; // advance offset position
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
        if (_.isPlainObject(partitions)) {
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
        partitions = self.client.getTopicPartitions(topic).then(_.partialRight(_.map, 'partitionId'));
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
        handler = function () {
            try {
                return Promise.resolve(originalHandler.apply(this, arguments));
            } catch (e) {
                return Promise.reject(e);
            }
        };

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
        partitions = Object.keys(self.client.topicMetadata[topic]).map(_.ary(parseInt, 1));
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
