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
});
