'use strict';

var _                  = require('lodash');
var Client             = require('./client');
var Kafka              = require('./index');
var errors             = require('./errors');
var promiseUtils       = require('./promise-utils');

function Producer(options) {
    this.options = _.defaultsDeep(options || {}, {
        requiredAcks: 1,
        timeout: 30000,
        partitioner: new Kafka.DefaultPartitioner(),
        retries: {
            attempts: 3,
            delay: {
                min: 1000,
                max: 3000
            }
        },
        batch: {
            size: 16384,
            maxWait: 10
        },
        codec: Kafka.COMPRESSION_NONE,
        idempotent: false
    });

    // transactional implies idempotent
    if (this.options.transactionalId) {
        this.options.idempotent = true;
    }

    // idempotent requires acks=-1
    if (this.options.idempotent) {
        this.options.requiredAcks = -1;
    }

    if (this.options.partitioner instanceof Kafka.DefaultPartitioner) {
        this.partitioner = this.options.partitioner;
    } else {
        throw new Error('Partitioner must inherit from Kafka.DefaultPartitioner');
    }

    this.client = new Client(this.options);

    this.queue = {};

    // idempotent sequence tracking: { 'topic:partition': nextSequence }
    this.sequenceNumbers = {};

    // transactional state
    this._inTransaction = false;
    this._txnPartitions = {};
}

module.exports = Producer;

/**
 * Initialize Producer
 *
 * @return {Promise}
 */
Producer.prototype.init = function () {
    var self = this;

    return self.client.init().then(function () {
        var initPromise;

        if (!self.options.idempotent) {
            return null;
        }
        if (self.options.transactionalId) {
            // find transaction coordinator first, then init producer id through it
            initPromise = self.client._findTransactionCoordinator(self.options.transactionalId)
                .then(function (connection) {
                    return self.client.initProducerIdRequest(
                        self.options.transactionalId,
                        self.options.timeout,
                        connection
                    );
                });
        } else {
            initPromise = self.client.initProducerIdRequest(null, self.options.timeout);
        }

        return initPromise.then(function (result) {
            self.client.producerId = result.producerId;
            self.client.producerEpoch = result.producerEpoch;
        });
    });
};

Producer.prototype._prepareProduceRequest = function (data) {
    var self = this;

    return promiseUtils.mapConcurrent(data, function (d) {
        delete d.error;
        return Promise.resolve().then(function () {
            if (typeof d.topic !== 'string' || d.topic === '') {
                throw new Error('Missing or wrong topic field');
            }
            if (typeof d.partition !== 'number' || d.partition < 0) {
                return self.client.getTopicPartitions(d.topic).then(function (partitions) {
                    return self.partitioner.partition(d.topic, partitions, d.message);
                })
                .then(function (partition) {
                    d.partition = partition;
                })
                .catch(function (err) {
                    if (err && err.code === 'UnknownTopicOrPartition') {
                        d.partition = -1;
                        return;
                    }
                    throw err;
                });
            }
            return null;
        })
        .then(function () {
            return self.client.findLeader(d.topic, d.partition)
            .catch(function (err) {
                d.error = err;
                return -1;
            })
            .then(function (leader) {
                d.leader = leader;
            });
        });
    }, 10).then(function () { return data; });
};

Producer.prototype._send = function (hash) {
    var self = this, task = self.queue[hash], data, result = [];

    delete self.queue[hash];

    data = Array.prototype.concat.apply([], task.data);

    function _errored(r) {
        return r.error !== undefined;
    }

    (function _try(_data, attempt) {
        attempt = attempt || 1;

        return self._prepareProduceRequest(_data).then(function (requests) {
            var toRetry = _.filter(requests, _errored);
            var validRequests = _.reject(requests, _errored);
            var addPartitionsPromise = Promise.resolve();
            var newTopics, topicsToAdd;

            // stamp sequence numbers for idempotent mode
            if (self.options.idempotent && self.client.producerId >= 0) {
                _.each(_.groupBy(validRequests, function (r) { return r.topic + ':' + r.partition; }), function (items, key) {
                    if (self.sequenceNumbers[key] === undefined) {
                        self.sequenceNumbers[key] = 0;
                    }
                    _.each(items, function (item) {
                        item._baseSequence = self.sequenceNumbers[key];
                        self.sequenceNumbers[key] = (self.sequenceNumbers[key] + 1) & 0x7FFFFFFF;
                    });
                });
            }

            // auto-register partitions for transactions
            if (self._inTransaction && self.options.transactionalId) {
                newTopics = {};
                _.each(validRequests, function (r) {
                    var key = r.topic + ':' + r.partition;
                    if (!self._txnPartitions[key]) {
                        self._txnPartitions[key] = true;
                        if (!newTopics[r.topic]) {
                            newTopics[r.topic] = [];
                        }
                        newTopics[r.topic].push(r.partition);
                    }
                });
                topicsToAdd = _.map(newTopics, function (partitions, topic) {
                    return { topic: topic, partitions: partitions };
                });
                if (topicsToAdd.length > 0) {
                    addPartitionsPromise = self.client.addPartitionsToTxnRequest(
                        self.options.transactionalId,
                        self.client.producerId,
                        self.client.producerEpoch,
                        topicsToAdd
                    );
                }
            }

            return addPartitionsPromise.then(function () {
                return self.client.produceRequest(validRequests, task.options.codec);
            }).then(function (response) {
                response.forEach(function (p) {
                    var failed;
                    if (p.error) {
                        // DuplicateSequenceNumber means broker already has this batch — treat as success
                        if (p.error.code === 'DuplicateSequenceNumber') {
                            result.push(_.omit(p, 'error'));
                        } else if (p.error.code === 'OutOfOrderSequenceNumber') {
                            // non-retryable fatal error
                            result.push(p);
                        } else if ((/UnknownTopicOrPartition|NotLeaderForPartition|LeaderNotAvailable/.test(p.error.code)
                                || p.error instanceof errors.NoKafkaConnectionError)) {
                            failed = _.filter(_data, { topic: p.topic, partition: p.partition });
                            failed = _.map(failed, function (f) {
                                f.error = p.error;
                                return f;
                            });
                            toRetry = toRetry.concat(failed);
                        } else {
                            result.push(p);
                        }
                    } else {
                        result.push(p);
                    }
                });
                return Promise.resolve()
                .then(function () {
                    var retryDelay;
                    if (toRetry.length && attempt < task.options.retries.attempts) {
                        retryDelay = _.min([attempt * task.options.retries.delay.min, task.options.retries.delay.max]);
                        return promiseUtils.delay(retryDelay).then(function () {
                            return _try(toRetry, ++attempt);
                        });
                    }
                    _.each(toRetry, function (r) {
                        result.push({ error: r.error });
                    });
                    return null;
                });
            });
        });
    }(data))
    .then(function () {
        task.resolve(result);
    })
    .catch(function (err) {
        task.reject(err);
    });
};

/**
 * Send message or messages to Kafka
 *
 * @param  {Object|Array} data [{ topic, partition, message: {key, value, attributes} }]
 * @param  {Object} options { codec, retries: { attempts, delay: { min, max } }, batch: { size } }
 * @return {Promise}
 */
Producer.prototype.send = function (data, options) {
    var self = this, hash, promise, task;

    if (!Array.isArray(data)) {
        data = [data];
    }

    options = _.merge({}, {
        codec: self.options.codec,
        retries: self.options.retries,
        batch: self.options.batch
    }, options || {});

    hash = [
        options.codec,
        options.retries.attempts,
        options.retries.delay.min,
        options.retries.delay.max,
        options.batch.size,
        options.batch.maxWait,
    ].join('.');

    if (self.queue[hash] === undefined) {
        promise = new Promise(function (resolve, reject) {
            self.queue[hash] = {
                timeout: null,
                resolve: resolve,
                reject: reject,
                options: options,
                data: [],
                dataSize: 0
            };
        });
        self.queue[hash].promise = promise;
    }

    task = self.queue[hash];
    task.data.push(data);
    task.dataSize += _.sumBy(data, _.partialRight(_.get, 'message.value.length', 0));

    if (task.dataSize >= options.batch.size || options.batch.maxWait === 0) {
        if (task.timeout !== null) {
            clearTimeout(task.timeout);
        }
        self._send(hash);
    } else if (task.timeout === null) {
        task.timeout = setTimeout(function () {
            self._send(hash);
        }, options.batch.maxWait);
    }

    return task.promise;
};

/**
 * Begin a new transaction. Requires transactionalId option.
 *
 * @return {void}
 */
Producer.prototype.beginTransaction = function () {
    if (!this.options.transactionalId) {
        throw new Error('Cannot begin transaction without transactionalId');
    }
    if (this._inTransaction) {
        throw new Error('Transaction already in progress');
    }
    this._inTransaction = true;
    this._txnPartitions = {};
};

/**
 * Commit the current transaction.
 *
 * @return {Promise}
 */
Producer.prototype.commitTransaction = function () {
    var self = this;

    if (!self.options.transactionalId) {
        return Promise.reject(new Error('Cannot commit transaction without transactionalId'));
    }
    if (!self._inTransaction) {
        return Promise.reject(new Error('No transaction in progress'));
    }

    return self.client.endTxnRequest(
        self.options.transactionalId,
        self.client.producerId,
        self.client.producerEpoch,
        true
    ).then(function () {
        self._inTransaction = false;
        self._txnPartitions = {};
    });
};

/**
 * Abort the current transaction.
 *
 * @return {Promise}
 */
Producer.prototype.abortTransaction = function () {
    var self = this;

    if (!self.options.transactionalId) {
        return Promise.reject(new Error('Cannot abort transaction without transactionalId'));
    }
    if (!self._inTransaction) {
        return Promise.reject(new Error('No transaction in progress'));
    }

    return self.client.endTxnRequest(
        self.options.transactionalId,
        self.client.producerId,
        self.client.producerEpoch,
        false
    ).then(function () {
        self._inTransaction = false;
        self._txnPartitions = {};
    });
};

/**
 * Send consumer offsets within the current transaction.
 *
 * @param  {Array} offsets [{ topic, partition, offset, metadata }]
 * @param  {String} groupId
 * @return {Promise}
 */
Producer.prototype.sendOffsets = function (offsets, groupId) {
    var self = this;

    if (!self.options.transactionalId) {
        return Promise.reject(new Error('Cannot send offsets without transactionalId'));
    }
    if (!self._inTransaction) {
        return Promise.reject(new Error('No transaction in progress'));
    }

    return self.client.addOffsetsToTxnRequest(
        self.options.transactionalId,
        self.client.producerId,
        self.client.producerEpoch,
        groupId
    ).then(function () {
        return self.client.txnOffsetCommitRequest(
            self.options.transactionalId,
            groupId,
            self.client.producerId,
            self.client.producerEpoch,
            offsets
        );
    });
};

/**
 * Close all connections
 *
 * @return {Promise}
 */
Producer.prototype.end = function () {
    var self = this;

    return self.client.end();
};
