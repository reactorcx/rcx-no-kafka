'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');

describe('KIP-848 Consumer Group Protocol Tests', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    ////////////////////////////////////////////
    // ConsumerGroupHeartbeat (apiKey 68)     //
    ////////////////////////////////////////////

    describe('ConsumerGroupHeartbeat v1', function () {
        it('should encode ConsumerGroupHeartbeatRequestV1 header (apiKey=68, apiVersion=1)', function () {
            var encoded = protocol.write().ConsumerGroupHeartbeatRequestV1({
                correlationId: 1,
                clientId: 'test-client',
                groupId: 'consumer-group-1',
                memberId: 'member-uuid-123',
                memberEpoch: 0,
                instanceId: null,
                rackId: null,
                rebalanceTimeoutMs: -1,
                subscribedTopicNames: ['topic-a', 'topic-b'],
                serverAssignor: 'uniform',
                topicPartitions: []
            }).result;

            // apiKey=68 (0x0044), apiVersion=1
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(0x44);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(1);
        });

        it('should encode join heartbeat with memberEpoch=0 and server assignor', function () {
            var encoded = protocol.write().ConsumerGroupHeartbeatRequestV1({
                correlationId: 2,
                clientId: 'test-client',
                groupId: 'consumer-group-1',
                memberId: 'member-uuid-123',
                memberEpoch: 0,
                instanceId: null,
                rackId: null,
                rebalanceTimeoutMs: 30000,
                subscribedTopicNames: ['topic-a'],
                serverAssignor: 'uniform',
                topicPartitions: []
            }).result;

            encoded.length.should.be.above(10);
        });

        it('should encode heartbeat with owned topicPartitions (reconciliation ack)', function () {
            var encoded = protocol.write().ConsumerGroupHeartbeatRequestV1({
                correlationId: 3,
                clientId: 'test-client',
                groupId: 'consumer-group-1',
                memberId: 'member-uuid-123',
                memberEpoch: 5,
                instanceId: null,
                rackId: null,
                rebalanceTimeoutMs: -1,
                subscribedTopicNames: null,
                serverAssignor: null,
                topicPartitions: [{
                    topicId: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
                    partitions: [0, 1, 2]
                }]
            }).result;

            encoded.length.should.be.above(20);
        });

        it('should encode leave heartbeat with memberEpoch=-1', function () {
            var encoded = protocol.write().ConsumerGroupHeartbeatRequestV1({
                correlationId: 4,
                clientId: 'test-client',
                groupId: 'consumer-group-1',
                memberId: 'member-uuid-123',
                memberEpoch: -1,
                instanceId: null,
                rackId: null,
                rebalanceTimeoutMs: -1,
                subscribedTopicNames: null,
                serverAssignor: null,
                topicPartitions: null
            }).result;

            encoded.length.should.be.above(10);
        });

        it('should encode static member instanceId', function () {
            var encoded = protocol.write().ConsumerGroupHeartbeatRequestV1({
                correlationId: 5,
                clientId: 'test-client',
                groupId: 'consumer-group-1',
                memberId: 'member-uuid-123',
                memberEpoch: 0,
                instanceId: 'static-instance-1',
                rackId: 'rack-1',
                rebalanceTimeoutMs: 30000,
                subscribedTopicNames: ['topic-a'],
                serverAssignor: 'uniform',
                topicPartitions: []
            }).result;

            encoded.length.should.be.above(20);
        });

        it('should parse ConsumerGroupHeartbeatResponseV1 with assignment', function () {
            var writer = protocol.write();
            var result;

            writer.Int32BE(1);              // correlationId
            writer.TaggedFields();          // response header v1
            writer.Int32BE(0);              // throttleTime
            writer.Int16BE(0);              // error
            writer.compactNullableString(null);             // errorMessage
            writer.compactNullableString('member-uuid-123'); // memberId
            writer.Int32BE(3);              // memberEpoch
            writer.Int32BE(5000);           // heartbeatIntervalMs
            // assignment (present: Int8 = 1)
            writer.Int8(1);
            writer.compactArray([{
                topicId: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
                partitions: [0, 1, 2]
            }], function (item) {
                this
                    .uuid(item.topicId)
                    .compactArray(item.partitions, this.Int32BE)
                    .TaggedFields();
            });
            writer.TaggedFields();          // assignment TaggedFields
            writer.TaggedFields();          // body TaggedFields

            result = protocol.read(writer.result).ConsumerGroupHeartbeatResponseV1().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
            should.not.exist(result.error);
            result.memberId.should.equal('member-uuid-123');
            result.memberEpoch.should.equal(3);
            result.heartbeatIntervalMs.should.equal(5000);
            result.assignment.should.be.an('object');
            result.assignment.topicPartitions.should.be.an('array').with.length(1);
            result.assignment.topicPartitions[0].topicId.should.equal('a1b2c3d4-e5f6-7890-abcd-ef1234567890');
            result.assignment.topicPartitions[0].partitions.should.deep.equal([0, 1, 2]);
        });

        it('should parse ConsumerGroupHeartbeatResponseV1 with null assignment', function () {
            var writer = protocol.write();
            var result;

            writer.Int32BE(2);
            writer.TaggedFields();
            writer.Int32BE(0);
            writer.Int16BE(0);
            writer.compactNullableString(null);
            writer.compactNullableString('member-uuid-123');
            writer.Int32BE(4);
            writer.Int32BE(5000);
            writer.Int8(-1);                // assignment = null
            writer.TaggedFields();

            result = protocol.read(writer.result).ConsumerGroupHeartbeatResponseV1().result;
            result.memberEpoch.should.equal(4);
            should.not.exist(result.assignment);
        });

        it('should parse ConsumerGroupHeartbeatResponseV1 with error', function () {
            var writer = protocol.write();
            var result;

            writer.Int32BE(3);
            writer.TaggedFields();
            writer.Int32BE(0);
            writer.Int16BE(110);            // FencedMemberEpoch
            writer.compactNullableString('member epoch is fenced');
            writer.compactNullableString(null);
            writer.Int32BE(0);
            writer.Int32BE(5000);
            writer.Int8(-1);
            writer.TaggedFields();

            result = protocol.read(writer.result).ConsumerGroupHeartbeatResponseV1().result;
            result.error.should.be.an('object');
            result.error.code.should.equal('FencedMemberEpoch');
            result.errorMessage.should.equal('member epoch is fenced');
        });
    });
});
