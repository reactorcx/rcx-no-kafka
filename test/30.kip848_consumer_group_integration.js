'use strict';

/* global describe, it, before, after, afterEach, should */

// These tests require a Kafka 4.0+ broker at localhost:9092 with the new
// consumer rebalance protocol enabled (KIP-848, default since 4.0).
// Topic `kafka-test-topic` (3 partitions) is created by the test fixtures.

var Kafka = require('../lib/index');

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
            return setTimeout(check, intervalMs);
        }());
    });
}

describe('KIP-848 ConsumerGroup Integration', function () {
    var TOPIC = 'kafka-test-topic';
    var producer;

    before(function () {
        this.timeout(30000);
        producer = new Kafka.Producer({ requiredAcks: 1, clientId: 'cg-integ-producer' });
        return producer.init();
    });

    after(function () {
        this.timeout(10000);
        return producer.end();
    });

    describe('ConsumerGroupHeartbeat (join/leave)', function () {
        var consumer;

        afterEach(function () {
            var c;
            this.timeout(10000);
            if (consumer) {
                c = consumer;
                consumer = null;
                return c.end();
            }
            return undefined;
        });

        it('should join a consumer group and receive a memberEpoch', function () {
            this.timeout(15000);

            consumer = new Kafka.ConsumerGroup({
                groupId: 'cg-integ-join-' + Date.now(),
                clientId: 'cg-integ-join'
            });

            return consumer.init({
                topics: [TOPIC],
                handler: function () { return Promise.resolve(); }
            }).then(function () {
                consumer.memberId.should.be.a('string').with.length.above(0);
                consumer.memberEpoch.should.be.at.least(1);
            });
        });

        it('should receive a partition assignment after heartbeat cycles', function () {
            this.timeout(30000);

            consumer = new Kafka.ConsumerGroup({
                groupId: 'cg-integ-assign-' + Date.now(),
                clientId: 'cg-integ-assign'
            });

            return consumer.init({
                topics: [TOPIC],
                handler: function () { return Promise.resolve(); }
            }).then(function () {
                return pollUntil(function () {
                    return Object.keys(consumer.owned).length > 0;
                }, 1000, 20);
            }).then(function () {
                var owned = Object.keys(consumer.owned);
                owned.length.should.be.at.least(1);
                owned.forEach(function (key) {
                    key.should.match(/^kafka-test-topic:\d+$/);
                });
            });
        });

        it('should fire onPartitionsAssigned with the assigned partitions', function () {
            var assigned = [];
            this.timeout(30000);

            consumer = new Kafka.ConsumerGroup({
                groupId: 'cg-integ-cb-' + Date.now(),
                clientId: 'cg-integ-cb'
            });

            return consumer.init({
                topics: [TOPIC],
                handler: function () { return Promise.resolve(); },
                onPartitionsAssigned: function (partitions) {
                    partitions.forEach(function (p) { assigned.push(p); });
                }
            }).then(function () {
                return pollUntil(function () {
                    return assigned.length > 0;
                }, 1000, 20);
            }).then(function () {
                assigned.length.should.be.at.least(1);
                assigned[0].should.have.property('topic', TOPIC);
                assigned[0].should.have.property('partition');
            });
        });

        it('should leave the consumer group on end()', function () {
            this.timeout(15000);

            consumer = new Kafka.ConsumerGroup({
                groupId: 'cg-integ-leave-' + Date.now(),
                clientId: 'cg-integ-leave'
            });

            return consumer.init({
                topics: [TOPIC],
                handler: function () { return Promise.resolve(); }
            }).then(function () {
                return consumer.end().then(function () {
                    consumer._closed.should.equal(true);
                    consumer = null;
                });
            });
        });
    });

    describe('Fetch + commit', function () {
        it('should consume produced messages and commit offsets', function () {
            var received = [];
            var marker = 'cg-msg-' + Date.now();
            var consumer = new Kafka.ConsumerGroup({
                groupId: 'cg-integ-fetch-' + Date.now(),
                clientId: 'cg-integ-fetch',
                idleTimeout: 500,
                startingOffset: Kafka.LATEST_OFFSET
            });
            this.timeout(40000);

            return consumer.init({
                topics: [TOPIC],
                handler: function (messageSet, topic, partition) {
                    messageSet.forEach(function (m) {
                        var value = m.message.value.toString('utf8');
                        if (value === marker) {
                            received.push({ topic: topic, partition: partition, offset: m.offset });
                        }
                    });
                    return Promise.resolve();
                }
            }).then(function () {
                // wait until at least partition 0 is owned, then produce to it
                return pollUntil(function () {
                    return consumer.owned[TOPIC + ':0'];
                }, 1000, 20);
            }).then(function () {
                return producer.send([
                    { topic: TOPIC, partition: 0, message: { value: marker } }
                ]);
            }).then(function () {
                return pollUntil(function () {
                    return received.length >= 1;
                }, 500, 30);
            }).then(function () {
                received.length.should.be.at.least(1);
                received[0].topic.should.equal(TOPIC);
                received[0].partition.should.equal(0);
                // commit the consumed offset, then read it back
                return consumer.commitOffset([{ topic: TOPIC, partition: 0, offset: received[0].offset }]);
            }).then(function () {
                return consumer.fetchOffset([{ topic: TOPIC, partition: 0 }]);
            }).then(function (committed) {
                committed.should.be.an('array').with.length.above(0);
                // committed offset = last consumed + 1
                committed[0].offset.should.equal(received[0].offset + 1);
                return consumer.end();
            });
        });
    });
});
