'use strict';

/* global describe, it, before, after, sinon, should */

var Kafka = require('../lib/index');

describe('Static Group Membership (KIP-345)', function () {
    var producer, staticConsumer;

    before(function () {
        this.timeout(6000);
        producer = new Kafka.Producer({ requiredAcks: 1, clientId: 'producer' });
        return producer.init();
    });

    after(function () {
        return producer.end();
    });

    describe('static member join', function () {
        before(function () {
            this.timeout(6000);
            staticConsumer = new Kafka.GroupConsumer({
                groupId: 'test-static-membership-group',
                idleTimeout: 100,
                heartbeatTimeout: 100,
                groupInstanceId: 'test-static-instance-1',
                clientId: 'static-consumer-1'
            });

            return staticConsumer.init({
                subscriptions: ['kafka-test-topic'],
                handler: function () {}
            });
        });

        after(function () {
            return staticConsumer.end();
        });

        it('should join group successfully with groupInstanceId', function () {
            should.exist(staticConsumer.memberId);
            staticConsumer.memberId.should.be.a('string');
            staticConsumer.memberId.length.should.be.greaterThan(0);
        });
    });

    describe('end() behavior', function () {
        it('should NOT call leaveGroupRequest for static members', function () {
            var consumer = new Kafka.GroupConsumer({
                groupId: 'test-static-end-group',
                idleTimeout: 100,
                heartbeatTimeout: 100,
                groupInstanceId: 'test-static-instance-end',
                clientId: 'static-consumer-end'
            });

            this.timeout(6000);

            return consumer.init({
                subscriptions: ['kafka-test-topic'],
                handler: function () {}
            }).then(function () {
                var spy = sinon.spy(consumer.client, 'leaveGroupRequest');
                return consumer.end().then(function () {
                    spy.should.not.have.been.called; // eslint-disable-line
                    spy.restore();
                });
            });
        });

        it('should call leaveGroupRequest for dynamic members', function () {
            var consumer = new Kafka.GroupConsumer({
                groupId: 'test-dynamic-end-group',
                idleTimeout: 100,
                heartbeatTimeout: 100,
                clientId: 'dynamic-consumer-end'
            });

            this.timeout(6000);

            return consumer.init({
                subscriptions: ['kafka-test-topic'],
                handler: function () {}
            }).then(function () {
                var spy = sinon.spy(consumer.client, 'leaveGroupRequest');
                return consumer.end().then(function () {
                    spy.should.have.been.calledOnce; // eslint-disable-line
                    spy.restore();
                });
            });
        });
    });
});
