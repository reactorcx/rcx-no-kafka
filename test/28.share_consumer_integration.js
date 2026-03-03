'use strict';

/* global describe, it, before, after, afterEach, should */

// These tests require a Kafka 4.2.0+ broker at localhost:9092 with:
// - share groups enabled (KIP-932)
// - __share_group_state topic created with RF=1 (for single-broker setups)

var Kafka        = require('../lib/index');
var promiseUtils = require('../lib/promise-utils');

/**
 * Poll until conditionFn() returns true, checking every intervalMs.
 * Rejects after maxAttempts polls.
 */
function pollUntil(conditionFn, intervalMs, maxAttempts) {
    var attempt = 0;
    return new Promise(function (resolve, reject) {
        (function check() {
            if (conditionFn()) { return resolve(); }
            attempt++;
            if (attempt >= maxAttempts) {
                return reject(new Error('pollUntil timed out after ' + maxAttempts + ' attempts'));
            }
            setTimeout(check, intervalMs);
        }());
    });
}

describe('KIP-932 ShareConsumer Integration', function () {
    var TOPIC = 'test-share-integ';
    var producer;

    before(function () {
        this.timeout(30000);

        producer = new Kafka.Producer({ requiredAcks: 1, clientId: 'share-integ-producer' });

        return producer.init().then(function () {
            return producer.send([
                { topic: TOPIC, partition: 0, message: { value: 'warmup' } }
            ]).catch(function () {
                // Topic may not exist — tests will still work if topic is pre-created
            });
        });
    });

    after(function () {
        this.timeout(10000);
        return producer.end();
    });

    describe('ShareGroupHeartbeat (join/leave)', function () {
        var consumer;

        afterEach(function () {
            this.timeout(10000);
            if (consumer) {
                var c = consumer;
                consumer = null;
                return c.end();
            }
        });

        it('should join a share group and receive a memberEpoch', function () {
            this.timeout(15000);

            consumer = new Kafka.ShareConsumer({
                groupId: 'share-integ-join-' + Date.now(),
                clientId: 'share-integ-join'
            });

            return consumer.init({
                topics: [TOPIC],
                handler: function () { return Promise.resolve(); }
            }).then(function () {
                consumer.memberId.should.be.a('string').with.length.above(0);
                consumer.memberEpoch.should.be.at.least(1);
            });
        });

        it('should receive partition assignment after heartbeat cycles', function () {
            this.timeout(30000);

            consumer = new Kafka.ShareConsumer({
                groupId: 'share-integ-assign-' + Date.now(),
                clientId: 'share-integ-assign'
            });

            return consumer.init({
                topics: [TOPIC],
                handler: function () { return Promise.resolve(); }
            }).then(function () {
                return pollUntil(function () {
                    return Object.keys(consumer.assignedPartitions).length > 0;
                }, 1000, 15);
            }).then(function () {
                var topicIds = Object.keys(consumer.assignedPartitions);
                topicIds.length.should.be.at.least(1);
                topicIds.forEach(function (id) {
                    consumer.assignedPartitions[id].should.be.an('array').with.length.above(0);
                });
            });
        });

        it('should leave the share group on end()', function () {
            this.timeout(15000);

            consumer = new Kafka.ShareConsumer({
                groupId: 'share-integ-leave-' + Date.now(),
                clientId: 'share-integ-leave'
            });

            return consumer.init({
                topics: [TOPIC],
                handler: function () { return Promise.resolve(); }
            }).then(function () {
                consumer.memberId.should.be.a('string').with.length.above(0);
                return consumer.end().then(function () {
                    consumer._closed.should.equal(true);
                    consumer = null;
                });
            });
        });
    });

    describe('ShareFetch (message delivery)', function () {
        it('should fetch messages produced while consumer is active', function () {
            this.timeout(30000);

            var received = [];
            var consumer = new Kafka.ShareConsumer({
                groupId: 'share-integ-fetch-' + Date.now(),
                clientId: 'share-integ-fetch',
                idleTimeout: 500
            });

            return consumer.init({
                topics: [TOPIC],
                handler: function (messageSet, topic, partition) {
                    messageSet.forEach(function (m) {
                        received.push({
                            topic: topic,
                            partition: partition,
                            offset: m.offset,
                            value: m.message.value.toString('utf8')
                        });
                    });
                    return Promise.resolve();
                }
            }).then(function () {
                return pollUntil(function () {
                    return Object.keys(consumer.assignedPartitions).length > 0;
                }, 1000, 15);
            }).then(function () {
                return producer.send([
                    { topic: TOPIC, partition: 0, message: { value: 'share-fetch-1' } },
                    { topic: TOPIC, partition: 0, message: { value: 'share-fetch-2' } }
                ]);
            }).then(function () {
                return pollUntil(function () {
                    return received.length >= 2;
                }, 500, 20);
            }).then(function () {
                received.length.should.be.at.least(2);
                var values = received.map(function (r) { return r.value; });
                values.should.include('share-fetch-1');
                values.should.include('share-fetch-2');
                return consumer.end();
            });
        });
    });

    describe('Acknowledge (accept records)', function () {
        it('should acknowledge records via piggybacked acks on next fetch', function () {
            this.timeout(30000);

            var received = [];
            var acknowledged = false;
            var consumer = new Kafka.ShareConsumer({
                groupId: 'share-integ-ack-' + Date.now(),
                clientId: 'share-integ-ack',
                idleTimeout: 500
            });

            return consumer.init({
                topics: [TOPIC],
                handler: function (messageSet, topic, partition) {
                    messageSet.forEach(function (m) {
                        received.push(m.message.value.toString('utf8'));
                    });
                    acknowledged = true;
                    return consumer.acknowledge(
                        messageSet.map(function (m) {
                            return { topic: topic, partition: partition, offset: m.offset };
                        }),
                        Kafka.ACKNOWLEDGE_TYPE.ACCEPT
                    );
                }
            }).then(function () {
                return pollUntil(function () {
                    return Object.keys(consumer.assignedPartitions).length > 0;
                }, 1000, 15);
            }).then(function () {
                return producer.send([
                    { topic: TOPIC, partition: 0, message: { value: 'share-ack-msg' } }
                ]);
            }).then(function () {
                return pollUntil(function () {
                    return acknowledged;
                }, 500, 20);
            }).then(function () {
                received.length.should.be.at.least(1);
                received.should.include('share-ack-msg');
                // Wait for ack to be piggybacked on next fetch cycle
                return promiseUtils.delay(2000);
            }).then(function () {
                return consumer.end();
            });
        });
    });
});
