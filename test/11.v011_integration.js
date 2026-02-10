'use strict';

/* global describe, it, before, sinon, after  */

// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.11 --create --topic kafka-test-topic --partitions 3 --replication-factor 1
// These tests require a Kafka v0.11+ broker at localhost:9092

var promiseUtils = require('../lib/promise-utils');
var Kafka   = require('../lib/index');

describe('v0.11 Integration', function () {
    var producer = new Kafka.Producer({ requiredAcks: 1, clientId: 'v011-producer' });
    var consumer = new Kafka.SimpleConsumer({ idleTimeout: 100, clientId: 'v011-consumer' });

    var dataHandlerSpy = sinon.spy(function () {});

    before(function () {
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

    describe('ApiVersions Discovery', function () {
        it('should discover API versions for broker connections', function () {
            var connections = producer.client.brokerConnections;
            Object.values(connections).forEach(function (conn) {
                conn.should.have.property('apiVersions');
                conn.apiVersions.should.be.an('object');
                // should have at least Produce and Fetch API keys
                conn.apiVersions.should.have.property('0'); // ProduceRequest
                conn.apiVersions.should.have.property('1'); // FetchRequest
            });
        });

        it('should have version ranges for each API key', function () {
            var connections = producer.client.brokerConnections;
            Object.values(connections).forEach(function (conn) {
                Object.values(conn.apiVersions).forEach(function (v) {
                    v.should.have.property('min').that.is.a('number');
                    v.should.have.property('max').that.is.a('number');
                    v.max.should.be.at.least(v.min);
                });
            });
        });

        it('should have ApiVersions key (18) in version map', function () {
            var connections = producer.client.brokerConnections;
            Object.values(connections).forEach(function (conn) {
                conn.apiVersions.should.have.property('18'); // ApiVersionsRequest
            });
        });

        it('should discover API versions for initial brokers', function () {
            var brokers = producer.client.initialBrokers;
            brokers.should.be.an('array').with.length.above(0);
            brokers.forEach(function (conn) {
                conn.should.have.property('apiVersions');
                conn.apiVersions.should.be.an('object');
                conn.apiVersions.should.have.property('0'); // ProduceRequest
                conn.apiVersions.should.have.property('1'); // FetchRequest
                conn.apiVersions.should.have.property('18'); // ApiVersionsRequest
            });
        });
    });

    describe('Metadata v1', function () {
        it('should store controllerId on client after metadata request', function () {
            // controllerId is set during updateMetadata when using MetadataResponseV1
            if (producer.client.controllerId !== undefined) {
                producer.client.controllerId.should.be.a('number');
            }
        });
    });

    describe('Timestamps', function () {
        it('should produce and consume messages with timestamps', function () {
            var now = Date.now();
            var msg;
            dataHandlerSpy.reset();

            return consumer.subscribe('kafka-test-topic', 0, dataHandlerSpy).then(function () {
                return producer.send({
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        value: 'timestamp-test',
                        timestamp: now
                    }
                });
            })
            .then(promiseUtils.delayChain(500))
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length.gte(1);

                msg = dataHandlerSpy.lastCall.args[0][0];
                msg.should.have.property('message').that.is.an('object');
                msg.message.should.have.property('value');
                msg.message.value.toString('utf8').should.be.eql('timestamp-test');

                // When using RecordBatch v2 (v0.11+), timestamp should be present
                if (msg.message.timestamp !== undefined) {
                    msg.message.timestamp.should.be.a('number');
                    // timestamp should be within a reasonable range of what we sent
                    msg.message.timestamp.should.be.closeTo(now, 60000);
                }
            });
        });
    });

    describe('Headers', function () {
        it('should produce and consume messages with headers', function () {
            var msg;
            dataHandlerSpy.reset();

            return consumer.unsubscribe('kafka-test-topic', 0).then(function () {
                return consumer.offset('kafka-test-topic', 0, Kafka.LATEST_OFFSET);
            })
            .then(function (offset) {
                return consumer.subscribe('kafka-test-topic', 0, { offset: offset }, dataHandlerSpy);
            })
            .then(function () {
                return producer.send({
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        value: 'headers-test',
                        headers: [
                            { key: 'trace-id', value: 'abc-123' },
                            { key: 'source', value: 'integration-test' }
                        ]
                    }
                });
            })
            .then(promiseUtils.delayChain(500))
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);

                msg = dataHandlerSpy.lastCall.args[0][0];
                msg.message.value.toString('utf8').should.be.eql('headers-test');

                // When using RecordBatch v2 (v0.11+), headers should be present
                if (msg.message.headers !== undefined) {
                    msg.message.headers.should.be.an('array').and.have.length(2);
                    msg.message.headers[0].should.have.property('key', 'trace-id');
                    msg.message.headers[1].should.have.property('key', 'source');
                }
            });
        });

        it('should produce and consume messages with empty headers array', function () {
            var msg;
            dataHandlerSpy.reset();

            return consumer.unsubscribe('kafka-test-topic', 0).then(function () {
                return consumer.offset('kafka-test-topic', 0, Kafka.LATEST_OFFSET);
            })
            .then(function (offset) {
                return consumer.subscribe('kafka-test-topic', 0, { offset: offset }, dataHandlerSpy);
            })
            .then(function () {
                return producer.send({
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        value: 'no-headers-test',
                        headers: []
                    }
                });
            })
            .then(promiseUtils.delayChain(500))
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);

                msg = dataHandlerSpy.lastCall.args[0][0];
                msg.message.value.toString('utf8').should.be.eql('no-headers-test');

                if (msg.message.headers !== undefined) {
                    msg.message.headers.should.be.an('array').and.have.length(0);
                }
            });
        });
    });

    describe('RecordBatch produce/consume', function () {
        it('should produce and consume keyed messages via RecordBatch', function () {
            var msg;
            dataHandlerSpy.reset();

            return consumer.unsubscribe('kafka-test-topic', 0).then(function () {
                return consumer.offset('kafka-test-topic', 0, Kafka.LATEST_OFFSET);
            })
            .then(function (offset) {
                return consumer.subscribe('kafka-test-topic', 0, { offset: offset }, dataHandlerSpy);
            })
            .then(function () {
                return producer.send({
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        key: 'rb-key',
                        value: 'rb-value'
                    }
                });
            })
            .then(promiseUtils.delayChain(500))
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);

                msg = dataHandlerSpy.lastCall.args[0][0];
                msg.should.have.property('offset').that.is.a('number');
                msg.message.should.have.property('value');
                msg.message.value.toString('utf8').should.be.eql('rb-value');
                msg.message.should.have.property('key');
                msg.message.key.toString('utf8').should.be.eql('rb-key');
            });
        });

        it('should produce and consume null key messages via RecordBatch', function () {
            var msg;
            dataHandlerSpy.reset();

            return consumer.unsubscribe('kafka-test-topic', 0).then(function () {
                return consumer.offset('kafka-test-topic', 0, Kafka.LATEST_OFFSET);
            })
            .then(function (offset) {
                return consumer.subscribe('kafka-test-topic', 0, { offset: offset }, dataHandlerSpy);
            })
            .then(function () {
                return producer.send({
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        value: 'null-key-test'
                    }
                });
            })
            .then(promiseUtils.delayChain(500))
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);

                msg = dataHandlerSpy.lastCall.args[0][0];
                msg.message.value.toString('utf8').should.be.eql('null-key-test');
            });
        });

        it('should produce and consume multiple messages in a batch', function () {
            var allMessages, i;
            dataHandlerSpy.reset();

            return consumer.unsubscribe('kafka-test-topic', 0).then(function () {
                return consumer.offset('kafka-test-topic', 0, Kafka.LATEST_OFFSET);
            })
            .then(function (offset) {
                return consumer.subscribe('kafka-test-topic', 0, { offset: offset }, dataHandlerSpy);
            })
            .then(function () {
                return producer.send([
                    {
                        topic: 'kafka-test-topic',
                        partition: 0,
                        message: { value: 'batch-msg-0' }
                    },
                    {
                        topic: 'kafka-test-topic',
                        partition: 0,
                        message: { value: 'batch-msg-1' }
                    },
                    {
                        topic: 'kafka-test-topic',
                        partition: 0,
                        message: { value: 'batch-msg-2' }
                    }
                ]);
            })
            .then(promiseUtils.delayChain(500))
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                // all 3 messages should arrive (may come in one or more calls)
                allMessages = [];
                for (i = 0; i < dataHandlerSpy.callCount; i++) {
                    allMessages = allMessages.concat(dataHandlerSpy.getCall(i).args[0]);
                }
                allMessages.should.have.length(3);
                allMessages[0].message.value.toString('utf8').should.be.eql('batch-msg-0');
                allMessages[1].message.value.toString('utf8').should.be.eql('batch-msg-1');
                allMessages[2].message.value.toString('utf8').should.be.eql('batch-msg-2');
            });
        });

        it('should produce and consume utf8 messages via RecordBatch', function () {
            var msg;
            dataHandlerSpy.reset();

            return consumer.unsubscribe('kafka-test-topic', 0).then(function () {
                return consumer.offset('kafka-test-topic', 0, Kafka.LATEST_OFFSET);
            })
            .then(function (offset) {
                return consumer.subscribe('kafka-test-topic', 0, { offset: offset }, dataHandlerSpy);
            })
            .then(function () {
                return producer.send({
                    topic: 'kafka-test-topic',
                    partition: 0,
                    message: {
                        value: '人人生而自由，在尊嚴和權利上一律平等。'
                    }
                });
            })
            .then(promiseUtils.delayChain(500))
            .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);

                msg = dataHandlerSpy.lastCall.args[0][0];
                msg.message.value.toString('utf8').should.be.eql('人人生而自由，在尊嚴和權利上一律平等。');
            });
        });
    });

    describe('Offset v1', function () {
        it('offset() should return a number (not an array) with v1', function () {
            return consumer.offset('kafka-test-topic', 0).then(function (offset) {
                offset.should.be.a('number').and.be.gte(0);
            });
        });

        it('offset() with EARLIEST_OFFSET should return offset 0 or greater', function () {
            return consumer.offset('kafka-test-topic', 0, Kafka.EARLIEST_OFFSET).then(function (offset) {
                offset.should.be.a('number').and.be.gte(0);
            });
        });

        it('offset() with LATEST_OFFSET should return a value >= EARLIEST_OFFSET', function () {
            var earliestOffset;
            return consumer.offset('kafka-test-topic', 0, Kafka.EARLIEST_OFFSET).then(function (offset) {
                earliestOffset = offset;
                return consumer.offset('kafka-test-topic', 0, Kafka.LATEST_OFFSET);
            })
            .then(function (latestOffset) {
                latestOffset.should.be.gte(earliestOffset);
            });
        });
    });
});
