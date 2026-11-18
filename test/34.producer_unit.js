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

        ['UnknownTopicId', 'InconsistentTopicId'].forEach(function (code) {
            it('should refresh metadata and retry on ' + code + ' (topic recreated under a new ID)', function () {
                var p = new Kafka.Producer({ clientId: 'producer-unit' });
                var calls = 0;

                sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
                sinon.stub(p.client, 'updateMetadata').returns(Promise.resolve());
                sinon.stub(p.client, 'produceRequest').callsFake(function () {
                    return Promise.resolve([++calls === 1
                        ? { topic: 't', partition: 0, error: errors.byName(code) }
                        : { topic: 't', partition: 0, offset: 7 }]);
                });

                return p.send({ topic: 't', partition: 0, message: { value: 'a' } }, { retries: FAST_RETRIES }).then(function (result) {
                    p.client.updateMetadata.args[0][0].should.eql(['t']);
                    result.should.eql([{ topic: 't', partition: 0, offset: 7 }]);
                });
            });
        });

        it('should not retry a stamped batch after UnknownTopicId even when the refresh learns no new ID', function () {
            var p = makeIdempotentProducer();
            p.client.topicIds.t = 'id-old';
            sinon.stub(p.client, 'updateMetadata').returns(Promise.resolve()); // still the old ID (or the refresh failed)
            sinon.stub(p.client, 'produceRequest').callsFake(function () {
                return Promise.resolve([{ topic: 't', partition: 0, error: errors.byName('UnknownTopicId') }]);
            });

            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }, { retries: FAST_RETRIES }).then(function (result) {
                p.client.produceRequest.callCount.should.equal(1);
                result[0].error.code.should.equal('UnknownTopicId');
                p.client.updateMetadata.args[0][0].should.eql(['t']);
            });
        });

        it('should bind a retry stamped before its topic had an ID to the first ID it was sent with', function () {
            var p = makeIdempotentProducer(), sentIds = [];
            sinon.stub(p.client, 'updateMetadata').callsFake(function () {
                p.client.topicIds.t = sentIds.length === 1 ? 'id-1' : 'id-2'; // learns an ID, then the topic is recreated
                return Promise.resolve();
            });
            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                sentIds.push(requests[0]._topicId);
                return Promise.resolve([{ topic: 't', partition: 0, error: errors.byName('RequestTimedOut') }]);
            });

            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }, { retries: FAST_RETRIES }).then(function (result) {
                sentIds.should.eql([undefined, 'id-1']); // never sent to id-2's log
                result[0].error.code.should.equal('UnknownTopicId');
            });
        });

        it('should catch a recreation after a send tied to an ID only as it went out', function () {
            var p = makeIdempotentProducer(), sent = 0;
            sinon.stub(p.client, '_findTransactionCoordinator');
            sinon.stub(p.client, 'initProducerIdRequest').returns(Promise.resolve({ producerId: 1, producerEpoch: 1 }));
            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                sent++;
                requests.forEach(function (r) { r._topicId = 'id-A'; }); // as produceRequest ties it at dispatch
                return Promise.resolve([{ topic: 't', partition: 0, offset: 0 }]);
            });

            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }).then(function () { // no ID known when stamped
                p.client.topicIds.t = 'id-B'; // recreated before the next send
                return p.send({ topic: 't', partition: 0, message: { value: 'b' } });
            }).then(function (r) {
                sent.should.equal(1); // A's next sequence never goes to B
                r[0].error.code.should.equal('UnknownTopicId');
            });
        });

        it('should keep a partition\'s sequence when its topic first gets an ID (broker upgrade)', function () {
            var p = makeIdempotentProducer(), seqs = [];
            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                seqs.push(requests[0]._baseSequence);
                return Promise.resolve([{ topic: 't', partition: 0, offset: 0 }]);
            });

            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }).then(function () { // no topic IDs yet
                p.client.topicIds.t = 'id-assigned';
                return p.send({ topic: 't', partition: 0, message: { value: 'b' } });
            }).then(function () {
                seqs.should.eql([0, 1]); // the same log: restarting at 0 would be taken as a duplicate
            });
        });

        [
            ['recreated', ['A', 'A', 'B', 'B'], [[0, 0], [1, 0], null, [0, 1]]],
            ['flips back to an earlier ID', ['A', 'B', 'A', 'A'], [[0, 0], null, [0, 1], [1, 1]]] // a new epoch: A from 0 is safe
        ].forEach(function (c) {
            it('should fail a send whose topic ID changed and start a fresh epoch (' + c[0] + ')', function () {
                var p = makeIdempotentProducer(), sent = [], results = [], epoch = 0;
                sinon.stub(p.client, '_findTransactionCoordinator');
                sinon.stub(p.client, 'initProducerIdRequest').callsFake(function () {
                    return Promise.resolve({ producerId: 1, producerEpoch: ++epoch });
                });
                sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                    sent.push([requests[0]._baseSequence, p.client.producerEpoch]);
                    return Promise.resolve([{ topic: 't', partition: 0, offset: 0 }]);
                });

                return c[1].reduce(function (prev, id, i) {
                    return prev.then(function () {
                        p.client.topicIds.t = id;
                        return p.send({ topic: 't', partition: 0, message: { value: 'v' + i } });
                    }).then(function (r) { results.push(r[0].error ? null : sent[sent.length - 1]); });
                }, Promise.resolve()).then(function () {
                    // same epoch, the new log could take a sequence it reuses as a duplicate: the send that
                    // sees the change fails (UnknownTopicId), the next one goes out under a new epoch from 0
                    results.should.eql(c[2]);
                });
            });
        });

        it('should give one result per partition when retries run out', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit' }), msgs;
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'updateMetadata').returns(Promise.resolve());
            sinon.stub(p.client, 'produceRequest').callsFake(function () {
                return Promise.resolve([{ topic: 't', partition: 0, error: errors.byName('UnknownTopicId') }]);
            });

            msgs = [{ topic: 't', partition: 0, message: { value: 'a' } }, { topic: 't', partition: 0, message: { value: 'b' } }];
            return p.send(msgs, { retries: FAST_RETRIES }).then(function (result) {
                result.should.have.length(1);
                result[0].error.code.should.equal('UnknownTopicId');
            });
        });

        it('should reject a large batch for a recreated topic in linear time', function () {
            var p = new Kafka.Producer({ idempotent: true, clientId: 'producer-unit', batch: { size: 1024 * 1024 * 1024, maxWait: 10 } });
            var msgs = [], i, t0;
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            p.client.findLeader = function () { return Promise.resolve(0); };
            p.client.topicIds.t = 'id-old';
            p.client.updateMetadata = function () { p.client.topicIds.t = 'id-new'; return Promise.resolve(); };
            p.client.produceRequest = function () { // plain fakes: sinon recording 150,000 records is slow
                return Promise.resolve([{ topic: 't', partition: 0, error: errors.byName('RequestTimedOut') }]);
            };
            for (i = 0; i < 150000; i++) { msgs.push({ topic: 't', partition: 0, message: { value: 'x' } }); }

            this.timeout(10000);
            t0 = Date.now();
            return p.send(msgs, { retries: FAST_RETRIES }).then(function (result) {
                (Date.now() - t0).should.be.below(2000); // quadratic rejection took ~6 s
                result.should.have.length(1);
                result[0].error.code.should.equal('UnknownTopicId');
            });
        });

        [
            ['UnknownTopicId', 'the broker no longer knows the topic ID'],
            ['RequestTimedOut', 'the topic was recreated while the attempt timed out']
        ].forEach(function (c) {
            it('should not retry an idempotent batch into a recreated topic (' + c[1] + ')', function () {
                var p = makeIdempotentProducer();
                p.client.topicIds.t = 'id-old';
                sinon.stub(p.client, 'updateMetadata').callsFake(function () {
                    p.client.topicIds.t = 'id-new'; // the refresh before the retry learns the new topic
                    return Promise.resolve();
                });
                sinon.stub(p.client, 'produceRequest').callsFake(function () {
                    return Promise.resolve([{ topic: 't', partition: 0, error: errors.byName(c[0]) }]);
                });

                return p.send({ topic: 't', partition: 0, message: { value: 'a' } }, { retries: FAST_RETRIES }).then(function (result) {
                    p.client.produceRequest.callCount.should.equal(1); // its sequences continue a log that is gone
                    result[0].error.code.should.equal('UnknownTopicId');
                    p._needsReinit.should.equal(true); // the next send starts a fresh sequence space
                });
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
            p._txnPartitions = { 't:9': true }; // something to abort
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
                p.client.producerEpoch = epoch + 1; // EndTxn v5 (V2) bumps the epoch
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

    describe('audit 2026-11 producer fixes', function () {
        function okResponse(requests) {
            return Promise.resolve(requests.map(function (r) { return { topic: r.topic, partition: r.partition, offset: 0 }; }));
        }

        it('should retry a partition whose leader lookup partly failed as one batch, in order', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit' });
            var calls = [], lookups = 0;
            var msgs = [0, 1, 2, 3, 4].map(function (i) { return { topic: 't', partition: 0, message: { value: 'm' + i } }; });

            sinon.stub(p.client, 'updateMetadata').returns(Promise.resolve());
            sinon.stub(p.client, 'findLeader').callsFake(function () {
                lookups++;
                return lookups === 3 ? Promise.reject(errors.byName('LeaderNotAvailable')) : Promise.resolve(0);
            });
            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                calls.push(requests.map(function (r) { return r.message.value; }));
                return okResponse(requests);
            });

            return p.send(msgs, { retries: FAST_RETRIES }).then(function (result) {
                calls.should.eql([['m0', 'm1', 'm2', 'm3', 'm4']]); // never split, never duplicated
                result.filter(function (r) { return r.error; }).should.eql([]);
            });
        });

        it('should not resend messages already queued for retry when the produce also fails', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit' });
            var calls = [], lookups = 0;
            var msgs = [0, 1, 2, 3, 4].map(function (i) { return { topic: 't', partition: 0, message: { value: 'm' + i } }; });

            sinon.stub(p.client, 'updateMetadata').returns(Promise.resolve());
            sinon.stub(p.client, 'findLeader').callsFake(function () {
                lookups++;
                return lookups === 3 ? Promise.reject(errors.byName('LeaderNotAvailable')) : Promise.resolve(0);
            });
            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                calls.push(requests.map(function (r) { return r.message.value; }));
                if (calls.length === 1) {
                    return Promise.resolve([{ topic: 't', partition: 0, error: errors.byName('NotLeaderForPartition') }]);
                }
                return okResponse(requests);
            });

            return p.send(msgs, { retries: FAST_RETRIES }).then(function () {
                calls.forEach(function (c) { c.should.eql(['m0', 'm1', 'm2', 'm3', 'm4']); }); // no duplicates, in order
            });
        });

        it('should refresh metadata before retrying', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit' });
            var refreshed = [];

            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'updateMetadata').callsFake(function (topics) {
                refreshed.push(topics);
                return Promise.resolve();
            });
            sinon.stub(p.client, 'produceRequest').returns(Promise.resolve([
                { topic: 't', partition: 0, error: errors.byName('NotLeaderForPartition') }
            ]));

            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }, { retries: FAST_RETRIES }).then(function () {
                refreshed.should.eql([['t'], ['t']]); // before each of the 2 retries
            });
        });

        it('should not reset sequences on commit when EndTxn did not bump the epoch', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-epoch' });
            var calls = 0;
            p.client.producerId = 1;
            p.client.producerEpoch = 3;
            sinon.stub(p.client, 'endTxnRequest').callsFake(function () {
                calls++;
                if (calls === 2) { p.client.producerEpoch++; } // EndTxn v5 bumps the epoch; pre-4.0 doesn't
                return Promise.resolve();
            });

            p.beginTransaction();
            p._txnPartitions = { 't:0': true };
            p.sequenceNumbers = { 't:0': 5 };
            return p.commitTransaction().then(function () {
                p.sequenceNumbers.should.eql({ 't:0': 5 });
                p.beginTransaction();
                p._txnPartitions = { 't:0': true };
                return p.abortTransaction();
            }).then(function () {
                p.sequenceNumbers.should.eql({});
            });
        });

        it('should reject a transactional send outside a transaction', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-outside' });
            sinon.stub(p.client, 'produceRequest').returns(Promise.resolve([]));
            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }).then(function () {
                throw new Error('expected rejection');
            }, function (err) {
                err.message.should.match(/beginTransaction/);
                p.client.produceRequest.callCount.should.equal(0);
            });
        });

        it('should commit an empty transaction without calling EndTxn', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-empty' });
            sinon.stub(p.client, 'endTxnRequest').callsFake(function () { return Promise.reject(errors.byName('InvalidTxnState')); });
            p.beginTransaction();
            return p.commitTransaction().then(function () {
                p._inTransaction.should.equal(false);
                p.client.endTxnRequest.callCount.should.equal(0);
            });
        });

        it('should unmark partitions when AddPartitionsToTxn fails', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-add' });
            var add = 0;
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            p._transactionV2 = false;
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'produceRequest').callsFake(okResponse);
            sinon.stub(p.client, 'addPartitionsToTxnRequest').callsFake(function () {
                add++;
                return add === 1 ? Promise.reject(errors.byName('ConcurrentTransactions')) : Promise.resolve([]);
            });
            sinon.stub(p, '_initProducerId').returns(Promise.resolve({ producerId: 1, producerEpoch: 1 }));

            p.beginTransaction();
            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }).then(function () {
                throw new Error('expected rejection');
            }, function () {
                p._txnPartitions.should.eql({});
                return p.send({ topic: 't', partition: 0, message: { value: 'b' } });
            }).then(function () {
                add.should.equal(2); // registered again on the next send
                p._txnPartitions.should.eql({ 't:0': true });
                // the failed attempt's sequence was stamped: a fresh space before registering again
                p._initProducerId.callCount.should.equal(1);
                p.client.producerEpoch.should.equal(1);
            });
        });

        it('should start a fresh sequence space when aborting after a failed AddPartitionsToTxn', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-reinit' });
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            p._transactionV2 = false;
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'addPartitionsToTxnRequest').callsFake(function () { return Promise.reject(errors.byName('ConcurrentTransactions')); });
            sinon.stub(p, '_initProducerId').returns(Promise.resolve({ producerId: 1, producerEpoch: 1 }));

            p.beginTransaction();
            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }).catch(function () {
                return p.abortTransaction();
            }).then(function () {
                p._initProducerId.callCount.should.equal(1);
                p.client.producerEpoch.should.equal(1);
                p.sequenceNumbers.should.eql({});
                p._inTransaction.should.equal(false);
            });
        });

        it('should treat aborting a transaction the broker never opened as done', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-never' });
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            sinon.stub(p.client, 'endTxnRequest').callsFake(function () { return Promise.reject(errors.byName('InvalidTxnState')); });
            sinon.stub(p, '_initProducerId').returns(Promise.resolve({ producerId: 1, producerEpoch: 1 }));

            p.beginTransaction();
            p._txnPartitions = { 't:0': true }; // marked, but every produce failed
            return p.abortTransaction().then(function () {
                p._inTransaction.should.equal(false);
                p._initProducerId.callCount.should.equal(1);
                p.beginTransaction(); // usable again
            });
        });

        it('should not report an abort as done when a failed commit may have gone through', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-lost-commit' });
            var ends = [];
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            sinon.stub(p.client, 'endTxnRequest').callsFake(function (id, pid, epoch, committed) {
                ends.push(committed);
                if (ends.length === 1) { // committed on the broker, but the response is lost
                    return Promise.reject(new errors.NoKafkaConnectionError('b:9092', 'response lost'));
                }
                // as a V1 broker after the commit: retrying it succeeds, aborting it is invalid
                return committed ? Promise.resolve() : Promise.reject(errors.byName('InvalidTxnState'));
            });
            sinon.stub(p, '_initProducerId').returns(Promise.resolve({ producerId: 1, producerEpoch: 1 }));

            p.beginTransaction();
            p._txnPartitions = { 't:0': true };
            function refused(call) {
                return call.then(function () {
                    throw new Error('expected a rejection');
                }, function (err) { return err.message; });
            }
            return p.commitTransaction().then(function () {
                throw new Error('expected commitTransaction to reject');
            }, function (err) {
                err.message.should.match(/response lost/);
                // if it did commit, these would fail and leave the transaction impossible to end
                return Promise.all([
                    refused(p.send({ topic: 't', partition: 0, message: { value: 'late' } })),
                    refused(p.sendOffsets([{ topic: 'in', partition: 0, offset: 5 }], 'g'))
                ]);
            }).then(function (messages) {
                messages.forEach(function (m) { m.should.match(/retry it first/); });
                return refused(p.abortTransaction());
            }).then(function (message) {
                message.should.match(/retry it instead of aborting/);
                p._inTransaction.should.equal(true);
                p._initProducerId.callCount.should.equal(0);
                return p.commitTransaction(); // the retry
            }).then(function () {
                ends.should.eql([true, false, true]);
                p._inTransaction.should.equal(false);
                // that's over: a later transaction the broker never opened aborts as usual
                p.beginTransaction();
                p._txnPartitions = { 't:0': true };
                return p.abortTransaction();
            }).then(function () {
                ends.should.eql([true, false, true, false]);
                p._initProducerId.callCount.should.equal(1);
            });
        });

        [[true, 'send'], [false, 'send'], [false, 'sendOffsets']].forEach(function (c) {
            var committed = c[0], first = c[1];
            it('should not fail ' + (committed ? 'a commit' : 'an abort') + ' that went through when re-initializing then fails (next: ' + first + ' first)', function () {
                var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-reinit-fail', batch: { maxWait: 0 }, logger: { logLevel: 0 } });
                var inits = 0, seqs = [];
                p.client.producerId = 1;
                p.client.producerEpoch = 0;
                p._transactionV2 = false;
                sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
                sinon.stub(p.client, 'addPartitionsToTxnRequest').returns(Promise.resolve([]));
                sinon.stub(p.client, 'endTxnRequest').returns(Promise.resolve()); // V1: the epoch stays
                sinon.stub(p, '_initProducerId').callsFake(function () {
                    return ++inits === 1 ? Promise.reject(new errors.NoKafkaConnectionError('b:9092', 'down')) : Promise.resolve({ producerId: 1, producerEpoch: 1 });
                });
                sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                    return Promise.resolve(requests.map(function (r) {
                        seqs.push([p.client.producerEpoch, r._baseSequence]);
                        return { topic: r.topic, partition: r.partition, error: null, offset: 0 };
                    }));
                });
                sinon.stub(p.client, 'addOffsetsToTxnRequest').callsFake(function (id, pid, epoch) {
                    seqs.push(['offsets', epoch]);
                    return Promise.resolve();
                });
                sinon.stub(p.client, 'txnOffsetCommitRequest').returns(Promise.resolve());

                p.beginTransaction();
                p._txnPartitions = { 't:0': true };
                p.sequenceNumbers = { 't:0': 3 };
                p._needsReinit = true; // a send in it failed
                return (committed ? p.commitTransaction() : p.abortTransaction()).then(function () {
                    var offsets;
                    p._inTransaction.should.equal(false);
                    p._needsReinit.should.equal(true); // still owed
                    p.beginTransaction();
                    if (first === 'sendOffsets') {
                        offsets = p.sendOffsets([{ topic: 'in', partition: 0, offset: 5 }], 'g');
                    }
                    return Promise.all([p.send({ topic: 't', partition: 0, message: { value: 'b' } }), offsets]);
                }).then(function () {
                    inits.should.equal(2); // before the next transaction registered anything
                    seqs.should.eql(first === 'sendOffsets' ? [['offsets', 1], [1, 0]] : [[1, 0]]);
                });
            });
        });

        it('should retry re-initializing the producer id while the coordinator completes the transaction', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-reinit-retry' });
            var calls = 0;
            this.timeout(5000); // one 1 s backoff
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            sinon.stub(p.client, '_findTransactionCoordinator').returns(Promise.resolve({}));
            sinon.stub(p.client, 'endTxnRequest').returns(Promise.resolve()); // V1: the epoch stays
            sinon.stub(p.client, 'initProducerIdRequest').callsFake(function () {
                // the coordinator is still writing the markers of the transaction that just ended
                return ++calls === 1 ? Promise.reject(errors.byName('ConcurrentTransactions')) : Promise.resolve({ producerId: 1, producerEpoch: 1 });
            });

            p.beginTransaction();
            p._txnPartitions = { 't:0': true };
            p._needsReinit = true; // a send in it ran out of retries
            return p.abortTransaction().then(function () {
                calls.should.equal(2);
                p.client.producerEpoch.should.equal(1);
                p._needsReinit.should.equal(false);
            });
        });

        it('should find the coordinator again when InitProducerId reaches one that moved', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-moved' });
            var oldConn = { close: sinon.spy(), send: sinon.stub().callsFake(function () { return Promise.reject(errors.byName('NotCoordinatorForGroup')); }) };
            var newConn = { send: sinon.stub().returns(Promise.resolve(Buffer.alloc(0))) };
            this.timeout(5000); // one 1 s backoff
            p.client.transactionCoordinators['tx-moved'] = Promise.resolve(oldConn);
            p.client.transactionCoordinators['tx-moved'].connection = oldConn; // as a resolved lookup leaves it
            p.client.initialBrokers = [{ apiVersions: { 10: { min: 0, max: 3 } } }]; // FindCoordinator v3
            sinon.stub(p.client, '_sendToAnyBroker').returns(Promise.resolve({ coordinatorHost: 'h', coordinatorPort: 2 }));
            sinon.stub(p.client, '_createConnection').returns(newConn);
            sinon.stub(p.client, 'apiVersionsRequest').returns(Promise.resolve({}));
            sinon.stub(p.client.protocol, 'read').returns({
                InitProducerIdResponse: function () { return { result: { error: null, producerId: 3, producerEpoch: 0 } }; }
            });

            return p._initProducerId().then(function (r) {
                r.producerId.should.equal(3);
                newConn.send.callCount.should.equal(1);
                oldConn.close.callCount.should.equal(1);
            });
        });

        [
            ['ran out of retries', function () { return new errors.NoKafkaConnectionError('b:9092', 'down'); }],
            ['was rejected', function () { return errors.byName('MessageSizeTooLarge'); }]
        ].forEach(function (c) {
            it('should start a fresh sequence space when aborting after a send that ' + c[0] + ' (V1)', function () {
                var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-gap', batch: { maxWait: 0 }, retries: FAST_RETRIES });
                var expected = 0, seqs = [];
                p.client.producerId = 1;
                p.client.producerEpoch = 0;
                p._transactionV2 = false;
                sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
                sinon.stub(p.client, 'addPartitionsToTxnRequest').returns(Promise.resolve([]));
                sinon.stub(p.client, 'endTxnRequest').returns(Promise.resolve()); // V1: the epoch stays
                sinon.stub(p.client, 'updateMetadata').returns(Promise.resolve());
                sinon.stub(p, '_initProducerId').callsFake(function () {
                    expected = 0; // a new epoch starts a new sequence space
                    return Promise.resolve({ producerId: 1, producerEpoch: 1 });
                });
                sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                    // a broker's sequence check; A's batch is never appended
                    return Promise.resolve(requests.map(function (r) {
                        seqs.push(r._baseSequence);
                        if (r.message.value === 'a') { return { topic: r.topic, partition: r.partition, error: c[1]() }; }
                        if (r._baseSequence !== expected) { return { topic: r.topic, partition: r.partition, error: errors.byName('OutOfOrderSequenceNumber') }; }
                        expected++;
                        return { topic: r.topic, partition: r.partition, error: null, offset: 0 };
                    }));
                });

                p.beginTransaction(); // A
                return p.send({ topic: 't', partition: 0, message: { value: 'a' } }).then(function (r) {
                    r[0].error.should.have.property('message');
                    return p.abortTransaction();
                }).then(function () {
                    p.beginTransaction(); // B
                    return p.send({ topic: 't', partition: 0, message: { value: 'b' } });
                }).then(function (r) {
                    r[0].should.have.property('error', null);
                    seqs[seqs.length - 1].should.equal(0); // B's batch starts the new sequence space
                    p._initProducerId.callCount.should.equal(1);
                });
            });
        });

        it('should reject a send made while the transaction is being committed', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-race' });
            var commit, late;
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            sinon.stub(p.client, 'endTxnRequest').returns(Promise.resolve());
            sinon.stub(p.client, 'produceRequest').returns(Promise.resolve([]));

            p.beginTransaction();
            p._txnPartitions = { 't:0': true };
            commit = p.commitTransaction();
            late = p.send({ topic: 't', partition: 0, message: { value: 'late' } }).then(function () {
                throw new Error('expected rejection');
            }, function (err) { err.message.should.match(/being committed or aborted/); });
            return Promise.all([commit, late]).then(function () {
                p.client.produceRequest.callCount.should.equal(0);
                return late;
            });
        });

        it('should refuse to commit after a send with a bad topic in the transaction', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-bad-topic', batch: { maxWait: 0 } });
            p.client.producerId = 1;
            p.client.producerEpoch = 0;
            p._transactionV2 = true;
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'produceRequest').callsFake(okResponse);
            sinon.stub(p.client, 'endTxnRequest').returns(Promise.resolve());

            p.beginTransaction();
            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }).then(function () {
                return p.send({ topic: '', partition: 0, message: { value: 'b' } }).then(function () {
                    throw new Error('expected the bad send to reject');
                }, function (err) { err.message.should.match(/topic/); });
            }).then(function () {
                return p.commitTransaction();
            }).then(function () {
                throw new Error('expected commitTransaction to reject');
            }, function (err) {
                err.message.should.match(/abortTransaction/);
                p.client.endTxnRequest.callCount.should.equal(0);
            });
        });

        it('should not fail other callers in the batch when one send has a bad topic', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', batch: { size: 1024 * 1024, maxWait: 5 } });
            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'produceRequest').callsFake(okResponse);

            return Promise.all([
                p.send({ topic: 'a', partition: 0, message: { value: 'x' } }),
                p.send({ topic: '', partition: 0, message: { value: 'y' } }).then(function () {
                    throw new Error('expected rejection');
                }, function (err) { return err.message; })
            ]).then(function (r) {
                r[0].map(function (x) { return x.topic; }).should.eql(['a']);
                r[1].should.match(/topic/);
            });
        });

        it('should reject a send with a null element instead of throwing in the linger timer', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', batch: { size: 1024 * 1024, maxWait: 5 } });
            sinon.stub(p.client, 'produceRequest').callsFake(okResponse);

            return p.send([{ topic: 'a', partition: 0, message: { value: 'x' } }, null]).then(function () {
                throw new Error('expected rejection');
            }, function (err) {
                err.message.should.match(/topic/);
                p.client.produceRequest.callCount.should.equal(0);
            });
        });

        it('should send more queued send() calls than fit in a call stack', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', batch: { size: 1024 * 1024 * 1024, maxWait: 50 } });
            var sends = [], produced = [], i;
            // plain fakes: sinon recording 150,000 calls takes seconds
            p.client.findLeader = function () { return Promise.resolve(0); };
            p.client.produceRequest = function (requests) { // one result per partition, as the broker gives
                produced.push(requests.length);
                return Promise.resolve([{ topic: 'a', partition: 0, offset: 0 }]);
            };

            for (i = 0; i < 150000; i++) {
                sends.push(p.send({ topic: 'a', partition: 0, message: { value: 'x' } }));
            }
            return Promise.all(sends).then(function (r) {
                produced.should.eql([150000]);
                r[149999].should.have.length(1);
            });
        });

        it('should give each send() call only its own results', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', batch: { size: 1024 * 1024, maxWait: 5 } });

            sinon.stub(p.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(p.client, 'produceRequest').callsFake(function (requests) {
                return Promise.resolve(requests.map(function (r) {
                    return { topic: r.topic, partition: r.partition, offset: 0, error: r.topic === 'bad' ? errors.byName('InvalidTopicException') : undefined };
                }).map(function (r) { if (!r.error) { delete r.error; } return r; }));
            });

            return Promise.all([
                p.send({ topic: 'a', partition: 0, message: { value: 'x' } }),
                p.send({ topic: 'bad', partition: 0, message: { value: 'y' } })
            ]).then(function (r) {
                r[0].map(function (x) { return x.topic; }).should.eql(['a']);
                r[1].map(function (x) { return x.topic; }).should.eql(['bad']);
            });
        });

        it('should not produce idempotently through a leader whose API versions are unknown', function () {
            var c = new Kafka.Producer({ clientId: 'producer-unit', idempotent: true }).client;
            c.producerId = 1;
            c.producerEpoch = 0;
            c.brokerConnections = { 0: { apiVersions: null, server: function () { return 'b:9092'; }, send: function () { throw new Error('must not send'); } } };
            return c.produceRequest([{ topic: 't', partition: 0, leader: 0, message: { value: 'a' }, _baseSequence: 0 }], 0)
            .then(function (r) {
                r[0].error.code.should.equal('LeaderNotAvailable');
            });
        });
    });
});
