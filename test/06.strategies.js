'use strict';

/* global describe, it, before, sinon, after  */

var promiseUtils = require('../lib/promise-utils');
var Kafka   = require('../lib/index');

var admin = new Kafka.GroupAdmin({ clientId: 'admin' });

before(function () {
    return admin.init();
});

after(function () {
    return admin.end();
});

describe('Weighted Round Robin Assignment', function () {
    var consumers = [
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-wrr',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer1'
        }),
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-wrr',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer2'
        })
    ];

    before(function () {
        return Promise.all(consumers.map(function (consumer, ind) {
            return consumer.init({
                subscriptions: ['kafka-test-topic'],
                metadata: {
                    weight: ind + 1
                },
                strategy: new Kafka.WeightedRoundRobinAssignmentStrategy(),
                handler: function () {}
            });
        })).then(promiseUtils.delayChain(200));
    });

    after(function () {
        return Promise.all(consumers.map(function (c) {
            return c.end();
        }));
    });

    it('should split partitions according to consumer weight', function () {
        return admin.describeGroup(consumers[0].options.groupId).then(function (group) {
            var consumer1 = group.members.find(function (m) { return m.clientId === 'group-consumer1'; });
            var consumer2 = group.members.find(function (m) { return m.clientId === 'group-consumer2'; });

            consumer1.memberAssignment.partitionAssignment[0].partitions.should.be.an('array').and.have.length(1);
            consumer2.memberAssignment.partitionAssignment[0].partitions.should.be.an('array').and.have.length(2);
        });
    });
});

describe('Consistent Assignment', function () {
    var consumers = [
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-ring',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer1'
        }),
        new Kafka.GroupConsumer({
            groupId: 'no-kafka-group-v0.9-ring',
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: 'group-consumer2'
        })
    ];

    before(function () {
        return Promise.all(consumers.map(function (consumer, ind) {
            return consumer.init({
                subscriptions: ['kafka-test-topic'],
                metadata: {
                    id: 'id' + ind,
                    weight: 10
                },
                strategy: new Kafka.ConsistentAssignmentStrategy(),
                handler: function () {}
            });
        })).then(promiseUtils.delayChain(200));
    });

    after(function () {
        return Promise.all(consumers.map(function (c) {
            return c.end();
        }));
    });

    it('should split partitions according to consumer weight', function () {
        return admin.describeGroup(consumers[0].options.groupId).then(function (group) {
            var consumer1 = group.members.find(function (m) { return m.clientId === 'group-consumer1'; });
            var consumer2 = group.members.find(function (m) { return m.clientId === 'group-consumer2'; });

            consumer1.memberAssignment.partitionAssignment[0].partitions.should.have.length(1);
            consumer1.memberAssignment.partitionAssignment[0].partitions.should.include(2);

            consumer2.memberAssignment.partitionAssignment[0].partitions.should.have.length(2);
            consumer2.memberAssignment.partitionAssignment[0].partitions.should.include(0);
            consumer2.memberAssignment.partitionAssignment[0].partitions.should.include(1);
        });
    });
});
