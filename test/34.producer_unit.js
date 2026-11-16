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

        it('should commit an empty transaction without calling EndTxn', function () {
            var p = new Kafka.Producer({ clientId: 'producer-unit', transactionalId: 'tx-empty' });
            sinon.stub(p.client, 'endTxnRequest').returns(Promise.reject(errors.byName('InvalidTxnState')));
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

            p.beginTransaction();
            return p.send({ topic: 't', partition: 0, message: { value: 'a' } }).then(function () {
                throw new Error('expected rejection');
            }, function () {
                p._txnPartitions.should.eql({});
                return p.send({ topic: 't', partition: 0, message: { value: 'b' } });
            }).then(function () {
                add.should.equal(2); // registered again on the next send
                p._txnPartitions.should.eql({ 't:0': true });
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
