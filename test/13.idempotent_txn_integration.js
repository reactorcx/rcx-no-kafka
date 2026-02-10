'use strict';

/* global describe, it, before, after, beforeEach, sinon, should */

// These tests require a Kafka v0.11+ broker at localhost:9092
// with transaction support enabled (transaction.state.log.replication.factor=1 for single-node)
// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.11 --create --topic kafka-test-topic --partitions 3 --replication-factor 1

var promiseUtils = require('../lib/promise-utils');
var Kafka   = require('../lib/index');

describe('Idempotent Producer Integration', function () {
    var producer, consumer;
    var dataHandlerSpy = sinon.spy(function () {});

    before(function () {
        // clear env vars that may be set to string "undefined" by earlier SSL tests
        if (process.env.KAFKA_CLIENT_CERT === 'undefined') { delete process.env.KAFKA_CLIENT_CERT; }
        if (process.env.KAFKA_CLIENT_CERT_KEY === 'undefined') { delete process.env.KAFKA_CLIENT_CERT_KEY; }

        producer = new Kafka.Producer({
            requiredAcks: 1,
            clientId: 'idempotent-producer',
            idempotent: true
        });
        consumer = new Kafka.SimpleConsumer({
            idleTimeout: 100,
            clientId: 'idempotent-consumer'
        });

        return Promise.all([
            producer.init(),
            consumer.init()
        ]);
    });

    after(function () {
        return Promise.all([
            producer.end(),
            consumer.end()
        ]);
    });

    it('should obtain a producer ID on init', function () {
        producer.client.producerId.should.be.at.least(0);
        producer.client.producerEpoch.should.be.at.least(0);
    });

    it('should force requiredAcks to -1', function () {
        producer.options.requiredAcks.should.equal(-1);
    });

    it('should send a single message', function () {
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: 'idempotent-test-1' }
        }).then(function (result) {
            result.should.be.an('array').and.have.length(1);
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition', 0);
            result[0].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
        });
    });

    it('should track sequence numbers per partition', function () {
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: 'idempotent-test-2' }
        }).then(function () {
            producer.sequenceNumbers.should.have.property('kafka-test-topic:0');
            producer.sequenceNumbers['kafka-test-topic:0'].should.be.at.least(1);
        });
    });

    it('should send multiple messages to different partitions', function () {
        return producer.send([
            { topic: 'kafka-test-topic', partition: 0, message: { value: 'idempotent-p0' } },
            { topic: 'kafka-test-topic', partition: 1, message: { value: 'idempotent-p1' } },
            { topic: 'kafka-test-topic', partition: 2, message: { value: 'idempotent-p2' } }
        ]).then(function (result) {
            result.should.be.an('array').and.have.length(3);
            result.forEach(function (r) {
                r.should.have.property('error', null);
            });
            // all three partitions should now have sequence numbers
            producer.sequenceNumbers.should.have.property('kafka-test-topic:0');
            producer.sequenceNumbers.should.have.property('kafka-test-topic:1');
            producer.sequenceNumbers.should.have.property('kafka-test-topic:2');
        });
    });

    it('should produce and consume messages correctly', function () {
        dataHandlerSpy.reset();

        return consumer.subscribe('kafka-test-topic', 0, dataHandlerSpy).then(function () {
            return producer.send({
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'idempotent-consume-test' }
            });
        })
        .then(promiseUtils.delayChain(500))
        .then(function () {
            var lastArgs, msg;
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            lastArgs = dataHandlerSpy.lastCall.args[0];
            lastArgs.should.be.an('array').and.have.length.gte(1);
            msg = lastArgs[lastArgs.length - 1];
            msg.message.value.toString('utf8').should.equal('idempotent-consume-test');
        });
    });

    it('should increment sequence numbers monotonically', function () {
        var seqBefore = producer.sequenceNumbers['kafka-test-topic:0'];

        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: 'seq-test' }
        }).then(function () {
            var seqAfter = producer.sequenceNumbers['kafka-test-topic:0'];
            seqAfter.should.be.above(seqBefore);
        });
    });
});

describe('Transactional Producer Integration', function () {
    var txnProducer, consumer, readCommittedConsumer;
    var dataHandlerSpy = sinon.spy(function () {});
    var txnSupported = true;

    before(function () {
        if (process.env.KAFKA_CLIENT_CERT === 'undefined') { delete process.env.KAFKA_CLIENT_CERT; }
        if (process.env.KAFKA_CLIENT_CERT_KEY === 'undefined') { delete process.env.KAFKA_CLIENT_CERT_KEY; }

        txnProducer = new Kafka.Producer({
            clientId: 'txn-producer',
            transactionalId: 'test-txn-id-' + Date.now()
        });
        consumer = new Kafka.SimpleConsumer({
            idleTimeout: 100,
            clientId: 'txn-consumer-uncommitted'
        });
        readCommittedConsumer = new Kafka.SimpleConsumer({
            idleTimeout: 100,
            clientId: 'txn-consumer-committed',
            isolationLevel: 1
        });

        return Promise.all([
            txnProducer.init().catch(function (err) {
                // broker may not have transactions configured (__transaction_state topic)
                if (/CoordinatorNotAvailable|NotCoordinator|GroupCoordinatorNotAvailable/.test(err.code || err.message || '')) {
                    txnSupported = false;
                    return null;
                }
                throw err;
            }),
            consumer.init(),
            readCommittedConsumer.init()
        ]);
    });

    beforeEach(function () {
        if (!txnSupported) {
            this.skip();
        }
    });

    after(function () {
        var promises = [consumer.end(), readCommittedConsumer.end()];
        if (txnSupported) {
            promises.push(txnProducer.end());
        }
        return Promise.all(promises).catch(function () {});
    });

    it('should obtain a producer ID for transactional producer', function () {
        txnProducer.client.producerId.should.be.at.least(0);
        txnProducer.client.producerEpoch.should.be.at.least(0);
    });

    it('should have idempotent=true forced on', function () {
        txnProducer.options.idempotent.should.equal(true);
        txnProducer.options.requiredAcks.should.equal(-1);
    });

    it('should begin a transaction', function () {
        txnProducer.beginTransaction();
        txnProducer._inTransaction.should.equal(true);
    });

    it('should produce within a transaction and commit', function () {
        var startOffset;
        dataHandlerSpy.reset();

        return consumer.offset('kafka-test-topic', 0, Kafka.LATEST_OFFSET).then(function (offset) {
            startOffset = offset;
            // already in transaction from previous test
            return txnProducer.send({
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'txn-committed-msg' }
            });
        })
        .then(function (result) {
            result.should.be.an('array').and.have.length(1);
            result[0].should.have.property('error', null);
            return txnProducer.commitTransaction();
        })
        .then(function () {
            txnProducer._inTransaction.should.equal(false);

            // verify message is visible
            return consumer.subscribe('kafka-test-topic', 0, { offset: startOffset }, dataHandlerSpy);
        })
        .then(promiseUtils.delayChain(500))
        .then(function () {
            var allMessages = [];
            var i, found;
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            for (i = 0; i < dataHandlerSpy.callCount; i++) {
                allMessages = allMessages.concat(dataHandlerSpy.getCall(i).args[0]);
            }
            found = allMessages.find(function (m) {
                return m.message.value.toString('utf8') === 'txn-committed-msg';
            });
            should.exist(found);
        });
    });

    it('should produce within a transaction and abort successfully', function () {
        txnProducer.beginTransaction();
        return txnProducer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: 'txn-aborted-msg-' + Date.now() }
        })
        .then(function (result) {
            result.should.be.an('array').and.have.length(1);
            result[0].should.have.property('error', null);
            return txnProducer.abortTransaction();
        })
        .then(function () {
            txnProducer._inTransaction.should.equal(false);
            txnProducer._txnPartitions.should.deep.equal({});
            // Note: full read_committed filtering of aborted messages requires
            // client-side processing of the abortedTransactions list from FetchResponseV4,
            // which is not yet implemented. The isolationLevel=1 option does tell the broker
            // to limit the fetch to the Last Stable Offset.
        });
    });

    it('should auto-register new partitions in a transaction', function () {
        txnProducer.beginTransaction();
        txnProducer._txnPartitions.should.deep.equal({});

        return txnProducer.send([
            { topic: 'kafka-test-topic', partition: 0, message: { value: 'auto-reg-0' } },
            { topic: 'kafka-test-topic', partition: 1, message: { value: 'auto-reg-1' } }
        ]).then(function (result) {
            result.should.be.an('array').and.have.length(2);
            result.forEach(function (r) {
                r.should.have.property('error', null);
            });
            txnProducer._txnPartitions.should.have.property('kafka-test-topic:0', true);
            txnProducer._txnPartitions.should.have.property('kafka-test-topic:1', true);
            return txnProducer.commitTransaction();
        });
    });

    it('should allow multiple transaction cycles', function () {
        // cycle 1
        txnProducer.beginTransaction();
        return txnProducer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: 'cycle-1' }
        }).then(function () {
            return txnProducer.commitTransaction();
        })
        .then(function () {
            // cycle 2
            txnProducer.beginTransaction();
            return txnProducer.send({
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'cycle-2' }
            });
        })
        .then(function () {
            return txnProducer.commitTransaction();
        })
        .then(function () {
            txnProducer._inTransaction.should.equal(false);
        });
    });
});

describe('Consumer Isolation Level Integration', function () {
    var readUncommittedConsumer, readCommittedConsumer;

    before(function () {
        if (process.env.KAFKA_CLIENT_CERT === 'undefined') { delete process.env.KAFKA_CLIENT_CERT; }
        if (process.env.KAFKA_CLIENT_CERT_KEY === 'undefined') { delete process.env.KAFKA_CLIENT_CERT_KEY; }

        readUncommittedConsumer = new Kafka.SimpleConsumer({
            idleTimeout: 100,
            clientId: 'isolation-consumer-0',
            isolationLevel: 0
        });
        readCommittedConsumer = new Kafka.SimpleConsumer({
            idleTimeout: 100,
            clientId: 'isolation-consumer-1',
            isolationLevel: 1
        });

        return Promise.all([
            readUncommittedConsumer.init(),
            readCommittedConsumer.init()
        ]);
    });

    after(function () {
        return Promise.all([
            readUncommittedConsumer.end(),
            readCommittedConsumer.end()
        ]);
    });

    it('read_uncommitted consumer should have isolationLevel=0', function () {
        readUncommittedConsumer.options.isolationLevel.should.equal(0);
    });

    it('read_committed consumer should have isolationLevel=1', function () {
        readCommittedConsumer.options.isolationLevel.should.equal(1);
    });

    it('both consumers should be able to subscribe and fetch', function () {
        var spy0 = sinon.spy(function () {});
        var spy1 = sinon.spy(function () {});

        return Promise.all([
            readUncommittedConsumer.subscribe('kafka-test-topic', 0, spy0),
            readCommittedConsumer.subscribe('kafka-test-topic', 0, spy1)
        ])
        .then(promiseUtils.delayChain(500))
        .then(function () {
            // both should subscribe without error — fetch may or may not have data
            return Promise.all([
                readUncommittedConsumer.unsubscribe('kafka-test-topic', 0),
                readCommittedConsumer.unsubscribe('kafka-test-topic', 0)
            ]);
        });
    });
});
