'use strict';

/* global describe, it, before, sinon, after  */

// kafka-topics.sh --zookeeper 127.0.0.1:2181/kafka0.9 --create --topic kafka-test-topic --partitions 3 --replication-factor 1

var promiseUtils = require('../lib/promise-utils');
var Kafka   = require('../lib/index');

var producer = new Kafka.Producer({ requiredAcks: 1, clientId: 'producer' });
var consumer = new Kafka.SimpleConsumer({ idleTimeout: 100, clientId: 'simple-consumer' });
var dataHandlerSpy = sinon.spy(function () {});

function isKRaftMode() {
    var conn = consumer.client.initialBrokers[0];
    return conn && conn.apiVersions && conn.apiVersions[57]; // 57 = DescribeQuorum (KRaft only)
}

describe('SimpleConsumer', function () {
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

    it('required methods', function () {
        return consumer.should
            .respondTo('init')
            .respondTo('subscribe')
            .respondTo('offset')
            .respondTo('unsubscribe')
            .respondTo('commitOffset')
            .respondTo('fetchOffset')
            .respondTo('end');
    });

    it('should receive new messages', function () {
        return consumer.subscribe('kafka-test-topic', 0, dataHandlerSpy).then(function () {
            return producer.send({
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'p00' }
            });
        })
        .then(promiseUtils.delayChain(500))
        .then(function () {
            /* jshint expr: true */
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            dataHandlerSpy.lastCall.args[1].should.be.a('string');
            dataHandlerSpy.lastCall.args[1].should.be.eql('kafka-test-topic');
            dataHandlerSpy.lastCall.args[2].should.be.a('number');
            dataHandlerSpy.lastCall.args[2].should.be.eql(0);
            dataHandlerSpy.lastCall.args[3].should.be.a('number');

            dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
            dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
            dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
        });
    });

    it('should receive new keyed messages', function () {
        dataHandlerSpy.reset();
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: {
                key: 'test-key-p00',
                value: 'p00'
            }
        })
        .then(promiseUtils.delayChain(500))
        .then(function () {
            /* jshint expr: true */
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            dataHandlerSpy.lastCall.args[1].should.be.a('string');
            dataHandlerSpy.lastCall.args[1].should.be.eql('kafka-test-topic');
            dataHandlerSpy.lastCall.args[2].should.be.a('number');
            dataHandlerSpy.lastCall.args[2].should.be.eql(0);
            dataHandlerSpy.lastCall.args[3].should.be.a('number');

            dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
            dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataHandlerSpy.lastCall.args[0][0].message.should.have.property('key');
            dataHandlerSpy.lastCall.args[0][0].message.key.toString().should.be.eql('test-key-p00');
            dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
            dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
        });
    });

    it('should correctly encode/decode utf8 string message value', function () {
        dataHandlerSpy.reset();
        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: '人人生而自由，在尊嚴和權利上一律平等。' }
        })
        .then(promiseUtils.delayChain(500))
        .then(function () {
            /* jshint expr: true */
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            dataHandlerSpy.lastCall.args[1].should.be.a('string');
            dataHandlerSpy.lastCall.args[1].should.be.eql('kafka-test-topic');
            dataHandlerSpy.lastCall.args[2].should.be.a('number');
            dataHandlerSpy.lastCall.args[2].should.be.eql(0);

            dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
            dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
            dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('人人生而自由，在尊嚴和權利上一律平等。');
        });
    });

    it('offset() should return last offset', function () {
        return consumer.offset('kafka-test-topic', 0).then(function (offset) {
            offset.should.be.a('number').and.be.gt(0);
        });
    });

    it('should reset offset to LATEST on OffsetOutOfRange error', function () {
        return consumer.offset('kafka-test-topic', 0).then(function (offset) {
            return consumer.subscribe('kafka-test-topic', 0, { offset: offset + 200 }, dataHandlerSpy)
            .then(function () {
                consumer.subscriptions['kafka-test-topic:0'].offset.should.be.eql(offset + 200);
            })
            .then(promiseUtils.delayChain(300))
            .then(function () {
                consumer.subscriptions['kafka-test-topic:0'].offset.should.be.eql(offset);
            });
        });
    });

    it('should receive messages from specified offset', function () {
        return consumer.unsubscribe('kafka-test-topic', 0).then(function () {
            dataHandlerSpy.reset();
            return producer.send([{
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'p000' }
            }, {
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'p001' }
            }]);
        })
        .then(function () {
            return consumer.offset('kafka-test-topic', 0).then(function (offset) {
                return consumer.subscribe('kafka-test-topic', 0, { offset: offset - 2 }, dataHandlerSpy)
                .then(promiseUtils.delayChain(200)) // consumer sleep timeout
                .then(function () {
                    dataHandlerSpy.should.have.been.called; // eslint-disable-line
                    dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(2);
                    dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p000');
                    dataHandlerSpy.lastCall.args[0][1].message.value.toString('utf8').should.be.eql('p001');
                });
            });
        });
    });

    it('should receive messages in maxBytes batches', function () {
        // With RecordBatch v2, messages produced together are in one batch.
        // Kafka always returns at least one complete batch, so both messages
        // arrive in a single handler call regardless of maxBytes.
        return consumer.unsubscribe('kafka-test-topic', 0).then(function () {
            dataHandlerSpy.reset();
            return consumer.offset('kafka-test-topic', 0).then(function (offset) {
                return consumer.subscribe('kafka-test-topic', 0, { offset: offset - 2 }, dataHandlerSpy)
                .then(promiseUtils.delayChain(300))
                .then(function () {
                    var allMessages = [], i; // eslint-disable-line
                    /* jshint expr: true */
                    dataHandlerSpy.should.have.been.called; // eslint-disable-line
                    for (i = 0; i < dataHandlerSpy.callCount; i++) {
                        allMessages = allMessages.concat(dataHandlerSpy.getCall(i).args[0]);
                    }
                    allMessages.should.have.length(2);
                    allMessages[0].message.value.toString('utf8').should.be.eql('p000');
                    allMessages[1].message.value.toString('utf8').should.be.eql('p001');
                    allMessages[0].should.have.property('messageSize').that.is.a('number');
                });
            });
        });
    });

    it('should skip single message larger then configured maxBytes', function () {
        // With RecordBatch v2, messages produced together are in one batch.
        // Kafka returns complete batches, so both messages arrive together.
        dataHandlerSpy.reset();
        return producer.send([{
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: 'p0000000000000001' }
        }, {
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: 'p001' }
        }])
        .then(promiseUtils.delayChain(300))
        .then(function () {
            var allMessages = [], i;
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            for (i = 0; i < dataHandlerSpy.callCount; i++) {
                allMessages = allMessages.concat(dataHandlerSpy.getCall(i).args[0]);
            }
            allMessages.should.have.length(2);
            allMessages[0].message.value.toString('utf8').should.be.eql('p0000000000000001');
            allMessages[1].message.value.toString('utf8').should.be.eql('p001');
            allMessages[0].should.have.property('messageSize').that.is.a('number');
        });
    });

    it('should be able to commit single offset', function () {
        if (isKRaftMode()) { return this.skip(); } // KRaft mode does not support v0 offset commit/fetch
        return consumer.commitOffset({
            topic: 'kafka-test-topic',
            partition: 0,
            offset: 1,
            metadata: 'm1'
        })
        .then(function (result) {
            result.should.be.an('array').that.has.length(1);
            result[0].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('error', null);
        });
    });

    it('should be able to commit offsets', function () {
        if (isKRaftMode()) { return this.skip(); } // KRaft mode does not support v0 offset commit/fetch
        return consumer.commitOffset([
            {
                topic: 'kafka-test-topic',
                partition: 0,
                offset: 1,
                metadata: 'm1'
            },
            {
                topic: 'kafka-test-topic',
                partition: 1,
                offset: 2,
                metadata: 'm2'
            },
            {
                topic: 'kafka-test-topic',
                partition: 2,
                offset: 3,
                metadata: 'm3'
            }
        ]).then(function (result) {
            result.should.be.an('array').that.has.length(3);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[2].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[1].should.have.property('topic', 'kafka-test-topic');
            result[2].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('partition').that.is.a('number');
            result[2].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('error', null);
            result[1].should.have.property('error', null);
            result[2].should.have.property('error', null);
        });
    });

    it('should be able to fetch commited offsets', function () {
        if (isKRaftMode()) { return this.skip(); } // KRaft mode does not support v0 offset commit/fetch
        return consumer.fetchOffset([
            {
                topic: 'kafka-test-topic',
                partition: 0
            },
            {
                topic: 'kafka-test-topic',
                partition: 1
            },
            {
                topic: 'kafka-test-topic',
                partition: 2
            }
        ]).then(function (result) {
            result.should.be.an('array').that.has.length(3);
            result[0].should.be.an('object');
            result[1].should.be.an('object');
            result[2].should.be.an('object');
            result[0].should.have.property('topic', 'kafka-test-topic');
            result[1].should.have.property('topic', 'kafka-test-topic');
            result[2].should.have.property('topic', 'kafka-test-topic');
            result[0].should.have.property('partition').that.is.a('number');
            result[1].should.have.property('partition').that.is.a('number');
            result[2].should.have.property('partition').that.is.a('number');
            result[0].should.have.property('offset').that.is.a('number');
            result[1].should.have.property('offset').that.is.a('number');
            result[2].should.have.property('offset').that.is.a('number');
            result[0].should.have.property('error', null);
            result[1].should.have.property('error', null);
            result[2].should.have.property('error', null);
            result.find(function (r) { return r.topic === 'kafka-test-topic' && r.partition === 0; }).offset.should.be.eql(1);
            result.find(function (r) { return r.topic === 'kafka-test-topic' && r.partition === 1; }).offset.should.be.eql(2);
            result.find(function (r) { return r.topic === 'kafka-test-topic' && r.partition === 2; }).offset.should.be.eql(3);
        });
    });

    it('should unsubscribe all partitions in a topic when partition is not specified', function () {
        return consumer.unsubscribe('kafka-test-topic').then(function () {
            consumer.subscriptions.should.not.have.property('kafka-test-topic:0');
            consumer.subscriptions.should.not.have.property('kafka-test-topic:1');
            consumer.subscriptions.should.not.have.property('kafka-test-topic:2');
        });
    });

    it('should subscribe all partitions in a topic when partition is not specified', function () {
        return consumer.unsubscribe('kafka-test-topic').then(function () {
            return consumer.subscribe('kafka-test-topic', dataHandlerSpy).then(function () {
                consumer.subscriptions.should.have.property('kafka-test-topic:0');
                consumer.subscriptions.should.have.property('kafka-test-topic:1');
                consumer.subscriptions.should.have.property('kafka-test-topic:2');
            });
        });
    });

    it('should subscribe partitions specified as array', function () {
        return consumer.unsubscribe('kafka-test-topic').then(function () {
            return consumer.subscribe('kafka-test-topic', [0, 1], dataHandlerSpy).then(function () {
                consumer.subscriptions.should.have.property('kafka-test-topic:0');
                consumer.subscriptions.should.have.property('kafka-test-topic:1');
                consumer.subscriptions.should.not.have.property('kafka-test-topic:2');
            });
        });
    });

    it('should subscribe partitions specified as array when options specified', function () {
        return consumer.unsubscribe('kafka-test-topic').then(function () {
            return consumer.subscribe('kafka-test-topic', [0, 1], {}, dataHandlerSpy)
            .then(function () {
                consumer.subscriptions.should.have.property('kafka-test-topic:0');
                consumer.subscriptions.should.have.property('kafka-test-topic:1');
                consumer.subscriptions.should.not.have.property('kafka-test-topic:2');
            });
        });
    });

    it('should subscribe all topic partitions when partition is not specified but options specified', function () {
        return consumer.unsubscribe('kafka-test-topic').then(function () {
            return consumer.subscribe('kafka-test-topic', {}, dataHandlerSpy)
            .then(function () {
                consumer.subscriptions.should.have.property('kafka-test-topic:0');
                consumer.subscriptions.should.have.property('kafka-test-topic:1');
                consumer.subscriptions.should.have.property('kafka-test-topic:2');
            });
        });
    });

    it('should throw when missing dataHandler function', function () {
        return consumer.subscribe('kafka-test-topic', [0, 1], {}).should.be.rejected;
    });

    it('should ignore sync errors in data handler', function () {
        var spy = sinon.spy(function () {
            throw new Error();
        });
        return consumer.subscribe('kafka-test-topic', 0, spy).then(function () {
            return producer.send({
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'p00' }
            });
        })
        .then(promiseUtils.delayChain(200))
        .then(function () {
            spy.should.have.been.called; // eslint-disable-line
        });
    });

    it('should ignore async errors in data handler', function () {
        var spy = sinon.spy(function () {
            return Promise.reject(new Error());
        });
        return consumer.subscribe('kafka-test-topic', 0, spy).then(function () {
            return producer.send({
                topic: 'kafka-test-topic',
                partition: 0,
                message: { value: 'p00' }
            });
        })
        .then(promiseUtils.delayChain(200))
        .then(function () {
            spy.should.have.been.called; // eslint-disable-line
        });
    });
});
