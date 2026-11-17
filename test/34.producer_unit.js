'use strict';

/* global describe, it, sinon */

var Kafka  = require('../lib/index');
var errors = require('../lib/errors');
var promiseUtils = require('../lib/promise-utils');
var FAST_RETRIES = { attempts: 3, delay: { min: 1, max: 2 } };

function makeIdempotentProducer(opts) {
    var p = new Kafka.Producer(Object.assign({ idempotent: true, clientId: 'producer-unit' }, opts || {}));
    p.client.producerId = 1;
    p.client.producerEpoch = 0;
    sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
    return p;
}

describe('Producer (unit)', function () {
    describe('idempotent sequence handling', function () {
        it('should reuse the same sequence numbers when retrying a failed batch', function () {
            var p = makeIdempotentProducer();
            var calls = [];

            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                calls.push(requests.map(function (r) { return r._baseSequence; }));
                if (calls.length === 1) {
                    return Promise.resolve([{ topic: 't', partition: 0, error: errors.byName('NotLeaderForPartition') }]);
                }
                return Promise.resolve([{ topic: 't', partition: 0, offset: 0 }]);
            });

            return p.send([
                { topic: 't', partition: 0, message: { value: 'a' } },
                { topic: 't', partition: 0, message: { value: 'b' } }
            ], { retries: FAST_RETRIES })
            .then(function () {
                calls.length.should.equal(2);
                calls[1].should.eql(calls[0]);
                p.sequenceNumbers['t:0'].should.equal(2);
            });
        });

        it('should not overlap in-flight idempotent produce requests', function () {
            var p = makeIdempotentProducer();
            var inFlight = 0;
            var maxInFlight = 0;

            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                inFlight++;
                maxInFlight = Math.max(maxInFlight, inFlight);
                return promiseUtils.delay(20).then(function () {
                    inFlight--;
                    return requests.map(function (r) {
                        return { topic: r.topic, partition: r.partition, offset: 0 };
                    });
                });
            });

            // different option hashes force two separate batch tasks
            return Promise.all([
                p.send({ topic: 't', partition: 0, message: { value: 'a' } }, { retries: FAST_RETRIES }),
                p.send({ topic: 't', partition: 0, message: { value: 'b' } }, { retries: { attempts: 2, delay: { min: 1, max: 1 } } })
            ]).then(function () {
                p.client.produceRequest.callCount.should.equal(2);
                maxInFlight.should.equal(1);
            });
        });

        it('should re-initialize the producer id after a failed idempotent attempt', function () {
            var p = makeIdempotentProducer();
            var produceCalls = 0;

            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                produceCalls++;
                if (produceCalls === 1) {
                    return Promise.reject(new Error('boom'));
                }
                return Promise.resolve(requests.map(function (r) {
                    return { topic: r.topic, partition: r.partition, offset: 0 };
                }));
            });
            sinon.stub(p.client, 'initProducerIdRequest').returns(Promise.resolve({ producerId: 99, producerEpoch: 0 }));

            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }, { retries: FAST_RETRIES })
            .then(function () {
                throw new Error('expected first send to reject');
            }, function () {
                return p.send({ topic: 't', partition: 0, message: { value: 'b' } }, { retries: FAST_RETRIES });
            })
            .then(function () {
                p.client.initProducerIdRequest.callCount.should.equal(1);
                p.client.producerId.should.equal(99);
                p.sequenceNumbers['t:0'].should.equal(1); // fresh sequence space after re-init
            });
        });

        it('should re-stamp a message object that is sent again after its batch settled', function () {
            var p = makeIdempotentProducer();
            var message = { topic: 't', partition: 0, message: { value: 'a' } };
            var sequences = [];

            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                sequences.push(requests[0]._baseSequence);
                return Promise.resolve([{ topic: 't', partition: 0, offset: 0 }]);
            });

            return p.send(message, { retries: FAST_RETRIES }).then(function () {
                return p.send(message, { retries: FAST_RETRIES });
            }).then(function () {
                sequences.should.eql([0, 1]);
                message.should.not.have.property('_baseSequence');
            });
        });
    });

    describe('retriable errors', function () {
        it('should retry produce on RequestTimedOut', function () {
            var p = makeIdempotentProducer();
            var produceCalls = 0;

            sinon.stub(p.client, 'produceRequest').callsFake(function () {
                produceCalls++;
                if (produceCalls === 1) {
                    return Promise.resolve([{ topic: 't', partition: 0, error: errors.byName('RequestTimedOut') }]);
                }
                return Promise.resolve([{ topic: 't', partition: 0, offset: 5 }]);
            });

            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }, { retries: FAST_RETRIES })
            .then(function (result) {
                produceCalls.should.equal(2);
                result.should.have.length(1);
                result[0].should.have.property('offset', 5);
                result[0].should.not.have.property('error');
            });
        });

        it('should include topic and partition on retry-exhausted failures', function () {
            var p = makeIdempotentProducer();

            sinon.stub(p.client, 'produceRequest').callsFake(function () {
                return Promise.resolve([{ topic: 't', partition: 0, error: errors.byName('NotLeaderForPartition') }]);
            });

            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }, { retries: { attempts: 2, delay: { min: 1, max: 1 } } })
            .then(function (result) {
                result.should.have.length(1);
                result[0].should.have.property('topic', 't');
                result[0].should.have.property('partition', 0);
                result[0].should.have.property('error');
            });
        });
    });

    describe('transactions', function () {
        it('should flush pending batches before committing a transaction', function () {
            var p = new Kafka.Producer({
                clientId: 'producer-unit',
                transactionalId: 'tx-unit',
                batch: { size: 1024 * 1024, maxWait: 5000 }
            });
            var order = [];

            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            p._transactionV2 = true; // skip AddPartitionsToTxn
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                order.push('produce');
                return Promise.resolve(requests.map(function (r) {
                    return { topic: r.topic, partition: r.partition, offset: 0 };
                }));
            });
            sinon.stub(p.client, 'endTxnRequest').callsFake(function () {
                order.push('endTxn');
                return Promise.resolve();
            });

            p.beginTransaction();
            p.send({ topic: 't', partition: 0, message: { value: 'a' } }).catch(function () { return null; });

            return p.commitTransaction().then(function () {
                order.should.eql(['produce', 'endTxn']);
            });
        });

        it('should wait for in-flight batches (already dispatched) before committing', function () {
            var p = new Kafka.Producer({
                clientId: 'producer-unit',
                transactionalId: 'tx-unit-2',
                batch: { size: 1024 * 1024, maxWait: 0 } // dispatch immediately: in flight, not queued
            });
            var order = [];

            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            p._transactionV2 = true;
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                return promiseUtils.delay(30).then(function () {
                    order.push('produce-done');
                    return requests.map(function (r) {
                        return { topic: r.topic, partition: r.partition, offset: 0 };
                    });
                });
            });
            sinon.stub(p.client, 'endTxnRequest').callsFake(function () {
                order.push('endTxn');
                return Promise.resolve();
            });

            p.beginTransaction();
            p.send({ topic: 't', partition: 0, message: { value: 'a' } }).catch(function () { return null; });

            return p.commitTransaction().then(function () {
                order.should.eql(['produce-done', 'endTxn']);
            });
        });

        it('should refuse to commit after a rejected send or a failed sendOffsets, waiting for a pending one', function () {
            var rejected, offsets;
            function txnProducer() {
                var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-unit-4', batch: { maxWait: 0 } });
                p.client.producerId = 1;
                p.client.producerEpoch = 0;
                p._transactionV2 = true;
                sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
                sinon.stub(p.client, 'endTxnRequest').returns(Promise.resolve());
                return p;
            }
            function refused(p) {
                return p.commitTransaction().then(function () {
                    throw new Error('expected commitTransaction to reject');
                }, function (err) {
                    err.message.should.match(/abortTransaction/);
                    p.client.endTxnRequest.callCount.should.equal(0);
                });
            }
            rejected = txnProducer();
            offsets = txnProducer();
            sinon.stub(rejected.client, 'produceRequest').callsFake(function () {
                return Promise.reject(new Error('serialization failed')); // not a per-partition result.error
            });
            rejected.beginTransaction();
            sinon.stub(offsets.client, 'txnOffsetCommitRequest').callsFake(function () {
                if (offsets.client.txnOffsetCommitRequest.callCount > 1) {
                    return Promise.resolve();
                }
                return promiseUtils.delay(10).then(function () { throw errors.byName('CoordinatorNotAvailable'); });
            });
            offsets.beginTransaction();
            // the slower, earlier write fails: commit must wait for it, not only for the last one
            offsets.sendOffsets([{ topic: 'in', partition: 0, offset: 5 }], 'g').catch(function () { return null; });
            offsets.sendOffsets([{ topic: 'in', partition: 1, offset: 5 }], 'g');

            return Promise.all([
                rejected.send({ topic: 't', partition: 0, message: { value: 'a' } }).catch(function () { return null; })
                .then(function () { return refused(rejected); }),
                refused(offsets) // without waiting: commit must wait for the offset write
            ]);
        });

        [true, false].forEach(function (v2) {
            it('should finish an aborted transaction\'s offset write before the next one begins (' + (v2 ? 'V2' : 'V1') + ')', function () {
                var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-late-' + v2, batch: { maxWait: 0 } });
                var release, late, aborted, calls = [];
                p.client.producerId = 1;
                p.client.producerEpoch = 0;
                p._transactionV2 = v2;
                sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
                sinon.stub(p.client, 'addPartitionsToTxnRequest').returns(Promise.resolve([]));
                sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                    return Promise.resolve(requests.map(function (r) { return { topic: r.topic, partition: r.partition, error: null, offset: 0 }; }));
                });
                sinon.stub(p.client, 'addOffsetsToTxnRequest').callsFake(function () {
                    calls.push('addOffsets');
                    // V1: hold A's AddOffsetsToTxn itself, so its TxnOffsetCommit is still to be sent
                    return calls.length === 1 ? new Promise(function (resolve) { release = resolve; }) : Promise.resolve();
                });
                sinon.stub(p.client, 'txnOffsetCommitRequest').callsFake(function (id, groupId, pid, epoch, offsets) {
                    var failed = errors.byName('UnknownTopicOrPartition');
                    calls.push('offsets ' + offsets[0].offset);
                    if (offsets[0].offset !== 100) {
                        return Promise.resolve();
                    }
                    // A's write partly fails; under V2 it is the one held
                    return v2 ? new Promise(function (resolve, reject) { release = reject.bind(null, failed); }) : Promise.reject(failed);
                });
                sinon.stub(p.client, 'endTxnRequest').callsFake(function (id, pid, epoch, committed) {
                    calls.push('end ' + committed);
                    return Promise.resolve();
                });

                p.beginTransaction(); // A
                late = p.sendOffsets([{ topic: 'in', partition: 0, offset: 100 }], 'g').catch(function () { return null; });
                aborted = p.abortTransaction();
                return Promise.all([
                    p.sendOffsets([{ topic: 'in', partition: 0, offset: 7 }], 'g'),
                    p.send({ topic: 't', partition: 0, message: { value: 'a' } }),
                    p.commitTransaction(),
                    p.abortTransaction()
                ].map(function (call) {
                    return call.then(function () {
                        throw new Error('expected every call to reject while the abort runs');
                    }, function (err) {
                        err.message.should.match(/being committed or aborted/);
                    });
                })).then(function () {
                    return promiseUtils.delay(10);
                }).then(function () {
                    p.client.endTxnRequest.callCount.should.equal(0); // the abort waits for A's offset write
                    release();
                    return Promise.all([aborted, late]);
                }).then(function () {
                    p.beginTransaction(); // B
                    return p.send({ topic: 't', partition: 0, message: { value: 'b' } });
                }).then(function () {
                    return p.sendOffsets([{ topic: 'in', partition: 0, offset: 50 }], 'g');
                }).then(function () {
                    return p.commitTransaction(); // A's failure doesn't fail B
                }).then(function () {
                    p.beginTransaction(); // C: the ended commit no longer blocks offsets
                    return p.sendOffsets([{ topic: 'in', partition: 0, offset: 60 }], 'g');
                }).then(function () {
                    calls.should.eql(v2 ?
                        ['offsets 100', 'end false', 'offsets 50', 'end true', 'offsets 60'] :
                        ['addOffsets', 'offsets 100', 'end false', 'addOffsets', 'offsets 50', 'end true',
                            'addOffsets', 'offsets 60']);
                });
            });
        });

        it('should reject a send racing an abort, so it cannot reach the next transaction', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-late-send', batch: { maxWait: 0 } });
            var late, ends = [];
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            p._transactionV2 = true;
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                return Promise.resolve(requests.map(function (r) { return { topic: r.topic, partition: r.partition, error: null, offset: 0 }; }));
            });
            sinon.stub(p.client, 'endTxnRequest').callsFake(function (id, pid, epoch, committed) {
                ends.push(committed);
                if (!committed && !late) { // a send racing the abort, after flush() looked
                    late = p.send({ topic: 't', partition: 0, message: { value: 'late' } });
                }
                return Promise.resolve();
            });

            p.beginTransaction(); // A
            return p.abortTransaction().then(function () {
                return late.then(function () {
                    throw new Error('expected the late send to reject');
                }, function (err) { err.message.should.match(/being committed or aborted/); });
            }).then(function () {
                p.beginTransaction(); // B
                return p.send({ topic: 't', partition: 1, message: { value: 'b' } });
            }).then(function () {
                return p.commitTransaction();
            }).then(function () {
                ends.should.eql([false, true]);
                p.client.produceRequest.callCount.should.equal(1); // only B's send was produced
            });
        });

        it('should count a failed send against the transaction it was written into', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-into', batch: { maxWait: 0 } });
            var ends = [], sent;
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            p._transactionV2 = true;
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'produceRequest').callsFake(function () {
                return Promise.reject(new Error('connection lost'));
            });
            sinon.stub(p.client, 'endTxnRequest').callsFake(function (id, pid, epoch, committed) {
                ends.push(committed);
                return Promise.resolve();
            });

            sent = p.send({ topic: 't', partition: 0, message: { value: 'a' } }).catch(function () { return null; });
            p.beginTransaction(); // same tick: the batch is dispatched, but registers into this transaction
            return sent.then(function () {
                p._txnPartitions.should.eql({ 't:0': true });
                return p.commitTransaction();
            }).then(function () {
                throw new Error('expected commitTransaction to reject');
            }, function (err) {
                err.message.should.match(/abortTransaction/);
                ends.should.eql([]);
            });
        });

        it('should count a send that failed before reaching a broker against its transaction', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-unsent', batch: { maxWait: 0 } });
            var ends = [];
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            p._transactionV2 = true;
            sinon.stub(p.client, 'getTopicPartitions').callsFake(function () { // fails before any partition is registered
                return Promise.reject(new errors.NoKafkaConnectionError('b:9092', 'down'));
            });
            sinon.stub(p.client, 'endTxnRequest').callsFake(function (id, pid, epoch, committed) {
                ends.push(committed);
                return Promise.resolve();
            });

            p.beginTransaction();
            return p.send({ topic: 't', message: { value: 'a' } }).then(function () { // no partition: partitioner looks it up
                throw new Error('expected send to reject');
            }, function () {
                p._txnPartitions.should.eql({}); // nothing was written
                return p.commitTransaction(); // but its records are missing from the transaction
            }).then(function () {
                throw new Error('expected commitTransaction to reject');
            }, function (err) {
                err.message.should.match(/abortTransaction/);
                ends.should.eql([]);
            });
        });

        it('should refuse to commit a transaction in which a send failed', function () {
            var p = new Kafka.Producer({
                clientId: 'producer-unit',
                transactionalId: 'tx-unit-3',
                batch: { size: 1024 * 1024, maxWait: 5000 },
                retries: FAST_RETRIES
            });
            var ends = [];

            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            p._transactionV2 = true;
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                return Promise.resolve(requests.map(function (r) {
                    return r.topic === 'bad'
                        ? { topic: r.topic, partition: r.partition, error: new errors.NoKafkaConnectionError('b:9092', 'down') }
                        : { topic: r.topic, partition: r.partition, error: null, offset: 0 }; // as parsed off the wire
                }));
            });
            sinon.stub(p.client, 'endTxnRequest').callsFake(function (id, pid, epoch, committed) {
                ends.push(committed);
                return Promise.resolve();
            });

            p.beginTransaction();
            p.send({ topic: 'ok', partition: 0, message: { value: 'a' } }).catch(function () { return null; });
            p.send({ topic: 'bad', partition: 0, message: { value: 'b' } }).catch(function () { return null; });

            return p.commitTransaction().then(function () {
                throw new Error('expected commitTransaction to reject');
            }, function (err) {
                err.message.should.match(/abortTransaction/);
                ends.should.eql([]); // nothing committed
                return p.abortTransaction();
            }).then(function () {
                // the next transaction starts clean
                p.beginTransaction();
                p.send({ topic: 'ok', partition: 0, message: { value: 'c' } }).catch(function () { return null; });
                return p.commitTransaction();
            }).then(function () {
                ends.should.eql([false, true]);
            });
        });
    });

    describe('end()', function () {
        it('should reject pending sends with a "Producer closed" message', function () {
            var p = new Kafka.Producer({
                clientId: 'producer-unit',
                batch: { size: 1024 * 1024, maxWait: 5000 }
            });
            var pending;

            sinon.stub(p.client, 'end').returns(Promise.resolve());

            pending = p.send({ topic: 't', partition: 0, message: { value: 'a' } })
            .then(function () {
                throw new Error('expected pending send to reject');
            }, function (err) {
                err.message.should.equal('Producer closed');
            });

            return Promise.all([p.end(), pending]);
        });
    });
});
