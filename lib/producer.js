'use strict';

var utils              = require('./utils');
var Client             = require('./client');
var Kafka              = require('./index');
var errors             = require('./errors');
var promiseUtils       = require('./promise-utils');
var globals            = require('./protocol/globals');

// Transient broker errors that a produce retry can succeed against
// (NotEnoughReplicas also matches NotEnoughReplicasAfterAppend; retrying it is
// safe in idempotent mode because retries resend the original sequences)
var RETRIABLE_ERROR_CODES = new RegExp('UnknownTopicOrPartition|NotLeaderForPartition|LeaderNotAvailable'
    + '|RequestTimedOut|NetworkException|NotEnoughReplicas|KafkaStorageException|FencedLeaderEpoch');

function Producer(options) {
    this.options = utils.defaultsDeep(options || {}, {
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
        compressionLevel: -1,
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

    // idempotent sends are serialized so stamped sequences reach the broker in
    // order (overlapping in-flight batches would reorder them on the wire)
    this._sendChain = Promise.resolve();

    // raised when the sequence state is unusable (attempt failed with unknown
    // outcome, or the broker reported OutOfOrderSequenceNumber) — the next send
    // re-initializes the producer id to obtain a fresh sequence space
    this._needsReinit = false;

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
        if (!self.options.idempotent) {
            return null;
        }

        function _initWithRetry(attempts) {
            return self._initProducerId().catch(function (err) {
                if (attempts > 0 && err && (err.code === 'GroupLoadInProgress' || err.code === 'GroupCoordinatorNotAvailable')) {
                    return promiseUtils.delay(1000).then(function () {
                        return _initWithRetry(attempts - 1);
                    });
                }
                throw err;
            });
        }

        return _initWithRetry(5).then(function (result) {
            self.client.producerId = result.producerId;
            self.client.producerEpoch = result.producerEpoch;
            // Detect Transaction V2 support (KIP-890): Produce v12+ AND EndTxn v5+
            self._transactionV2 = false;
            if (self.options.transactionalId) {
                // decide from the transaction coordinator's advertised versions —
                // "any initial broker" would be wrong in a mixed-version cluster
                return self.client._findTransactionCoordinator(self.options.transactionalId)
                .then(function (conn) {
                    var produceMax, endTxnMax;
                    if (conn.apiVersions) {
                        produceMax = conn.apiVersions[globals.API_KEYS.ProduceRequest];
                        endTxnMax = conn.apiVersions[globals.API_KEYS.EndTxnRequest];
                        if (produceMax && produceMax.max >= 12 && endTxnMax && endTxnMax.max >= 5) {
                            self._transactionV2 = true;
                        }
                    }
                })
                .catch(function () {
                    return null; // detection failure just leaves V1 behavior
                });
            }
            return null;
        });
    });
};

Producer.prototype._initProducerId = function () {
    var self = this;

    if (self.options.transactionalId) {
        return self.client._findTransactionCoordinator(self.options.transactionalId)
            .then(function (connection) {
                return self.client.initProducerIdRequest(
                    self.options.transactionalId,
                    self.options.timeout,
                    connection
                );
            });
    }
    return self.client.initProducerIdRequest(null, self.options.timeout);
};

Producer.prototype._reinitProducerId = function () {
    var self = this;

    return self._initProducerId().then(function (result) {
        self.client.producerId = result.producerId;
        self.client.producerEpoch = result.producerEpoch;
        self.sequenceNumbers = {};
        self._needsReinit = false;
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
    var self = this, task = self.queue[hash], data, result = [], exec;

    delete self.queue[hash];

    data = Array.prototype.concat.apply([], task.data);

    function _errored(r) {
        return r.error !== undefined;
    }

    // stamps are per-task: once the task settles there are no more retries, so
    // the markers are removed to allow callers to safely re-send message objects
    function _clearStamps() {
        data.forEach(function (d) {
            delete d._baseSequence;
        });
    }

    function _try(_data, attempt) {
        attempt = attempt || 1;

        return self._prepareProduceRequest(_data).then(function (requests) {
            var toRetry = requests.filter(_errored);
            var validRequests = requests.filter(function (r) { return !_errored(r); });
            var addPartitionsPromise = Promise.resolve();
            var newTopics, topicsToAdd, idempotentGroups;

            // stamp sequence numbers for idempotent mode; a message is stamped
            // only once — retries must resend the original sequence so the
            // broker can deduplicate (KIP-98)
            if (self.options.idempotent && self.client.producerId >= 0) {
                idempotentGroups = utils.groupBy(validRequests, function (r) { return r.topic + ':' + r.partition; });
                Object.keys(idempotentGroups).forEach(function (key) {
                    var items = idempotentGroups[key];
                    if (self.sequenceNumbers[key] === undefined) {
                        self.sequenceNumbers[key] = 0;
                    }
                    items.forEach(function (item) {
                        if (item._baseSequence === undefined) {
                            item._baseSequence = self.sequenceNumbers[key];
                            self.sequenceNumbers[key] = (self.sequenceNumbers[key] + 1) & 0x7FFFFFFF;
                        }
                    });
                });
            }

            // auto-register partitions for transactions
            if (self._inTransaction && self.options.transactionalId) {
                newTopics = {};
                validRequests.forEach(function (r) {
                    var key = r.topic + ':' + r.partition;
                    if (!self._txnPartitions[key]) {
                        self._txnPartitions[key] = true;
                        if (!newTopics[r.topic]) {
                            newTopics[r.topic] = [];
                        }
                        newTopics[r.topic].push(r.partition);
                    }
                });
                topicsToAdd = Object.keys(newTopics).map(function (topic) {
                    var partitions = newTopics[topic];
                    return { topic: topic, partitions: partitions };
                });
                if (topicsToAdd.length > 0 && !self._transactionV2) {
                    addPartitionsPromise = self.client.addPartitionsToTxnRequest(
                        self.options.transactionalId,
                        self.client.producerId,
                        self.client.producerEpoch,
                        topicsToAdd
                    );
                }
            }

            return addPartitionsPromise.then(function () {
                return self.client.produceRequest(validRequests, task.options.codec, task.options.compressionLevel);
            }).then(function (response) {
                response.forEach(function (p) {
                    var failed, clean;
                    if (p.error) {
                        // DuplicateSequenceNumber means broker already has this batch — treat as success
                        if (p.error.code === 'DuplicateSequenceNumber') {
                            clean = Object.assign({}, p);
                            delete clean.error;
                            result.push(clean);
                        } else if (p.error.code === 'OutOfOrderSequenceNumber') {
                            // fatal for this producer epoch — recover a fresh
                            // sequence space before the next send
                            self._needsReinit = true;
                            result.push(p);
                        } else if ((RETRIABLE_ERROR_CODES.test(p.error.code)
                                || p.error instanceof errors.NoKafkaConnectionError)) {
                            failed = _data.filter(function (f) { return f.topic === p.topic && f.partition === p.partition; });
                            failed = failed.map(function (f) {
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
                        retryDelay = Math.min(attempt * task.options.retries.delay.min, task.options.retries.delay.max);
                        return promiseUtils.delay(retryDelay).then(function () {
                            return _try(toRetry, ++attempt);
                        });
                    }
                    toRetry.forEach(function (r) {
                        result.push({ topic: r.topic, partition: r.partition, error: r.error });
                    });
                    return null;
                });
            });
        });
    }

    function _run() {
        return Promise.resolve()
        .then(function () {
            // recover a poisoned sequence space before stamping new sequences;
            // mid-transaction the recovery happens via endTxn (epoch bump) instead
            if (self.options.idempotent && self._needsReinit && !self._inTransaction) {
                return self._reinitProducerId();
            }
            return null;
        })
        .then(function () {
            return _try(data).catch(function (err) {
                if (self.options.idempotent) {
                    // outcome unknown — stamped sequences may or may not have
                    // been persisted, so the sequence space cannot be reused
                    self._needsReinit = true;
                }
                throw err;
            });
        });
    }

    if (self.options.idempotent) {
        exec = self._sendChain = self._sendChain.then(_run, _run);
    } else {
        exec = _run();
    }

    exec.then(function () {
        _clearStamps();
        task.resolve(result);
    })
    .catch(function (err) {
        _clearStamps();
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

    options = utils.defaultsDeep(Object.assign({}, options || {}), {
        codec: self.options.codec,
        compressionLevel: self.options.compressionLevel,
        retries: self.options.retries,
        batch: self.options.batch
    });

    hash = [
        options.codec,
        options.compressionLevel,
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
    task.dataSize += data.reduce(function (sum, d) {
        return sum + (d.message && d.message.value && d.message.value.length ? d.message.value.length : 0);
    }, 0);

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
 * Send out all batches currently waiting on their batch.maxWait timer and
 * wait for them to settle.
 *
 * @return {Promise}
 */
Producer.prototype.flush = function () {
    var self = this, settled = [];

    Object.keys(self.queue).forEach(function (hash) {
        var task = self.queue[hash];
        if (task.timeout !== null) {
            clearTimeout(task.timeout);
            task.timeout = null;
        }
        // outcomes are delivered to the original senders; flush only waits
        settled.push(task.promise.catch(function () { return null; }));
        self._send(hash);
    });

    return Promise.all(settled).then(function () {
        // also wait for batches that were already dispatched: idempotent
        // (and therefore transactional) sends serialize through _sendChain,
        // so the chain tail settles only after all in-flight work
        return self._sendChain.then(function () { return null; }, function () { return null; });
    });
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

    // batches still waiting on their maxWait timer belong to this transaction
    // and must be produced before the transaction ends
    return self.flush().then(function () {
        return self.client.endTxnRequest(
            self.options.transactionalId,
            self.client.producerId,
            self.client.producerEpoch,
            true
        );
    }).then(function () {
        self._inTransaction = false;
        self._txnPartitions = {};
        self.sequenceNumbers = {};
        self._needsReinit = false;
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

    // settle in-flight/queued batches first so aborted records don't leak
    // into the next transaction
    return self.flush().then(function () {
        return self.client.endTxnRequest(
            self.options.transactionalId,
            self.client.producerId,
            self.client.producerEpoch,
            false
        );
    }).then(function () {
        self._inTransaction = false;
        self._txnPartitions = {};
        self.sequenceNumbers = {};
        self._needsReinit = false;
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

    if (self._transactionV2) {
        // Transaction V2: TxnOffsetCommit v5 implicitly handles AddOffsetsToTxn
        return self.client.txnOffsetCommitRequest(
            self.options.transactionalId,
            groupId,
            self.client.producerId,
            self.client.producerEpoch,
            offsets
        );
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
    var self = this, hash, task;

    for (hash in self.queue) {
        if (self.queue.hasOwnProperty(hash)) {
            task = self.queue[hash];
            if (task.timeout !== null) {
                clearTimeout(task.timeout);
            }
            task.reject(new errors.NoKafkaConnectionError(null, 'Producer closed'));
        }
    }
    self.queue = {};

    return self.client.end();
};
