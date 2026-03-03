'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');

describe('KIP-932 Share Group Protocol Tests', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    ////////////////////////////////////////////
    // ShareGroupHeartbeat (apiKey 76)        //
    ////////////////////////////////////////////

    describe('ShareGroupHeartbeat v1', function () {
        it('should encode ShareGroupHeartbeatRequestV1', function () {
            var encoded = protocol.write().ShareGroupHeartbeatRequestV1({
                correlationId: 1,
                clientId: 'test-client',
                groupId: 'share-group-1',
                memberId: '',
                memberEpoch: 0,
                rackId: null,
                subscribedTopicNames: ['topic-a', 'topic-b']
            }).result;

            // apiKey=76 (0x004C), apiVersion=1
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(0x4C);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(1);
        });

        it('should encode join heartbeat with memberEpoch=0', function () {
            var encoded = protocol.write().ShareGroupHeartbeatRequestV1({
                correlationId: 2,
                clientId: 'test-client',
                groupId: 'share-group-1',
                memberId: 'member-uuid-123',
                memberEpoch: 0,
                rackId: 'rack-1',
                subscribedTopicNames: ['topic-a']
            }).result;

            encoded.length.should.be.above(10);
        });

        it('should encode leave heartbeat with memberEpoch=-1', function () {
            var encoded = protocol.write().ShareGroupHeartbeatRequestV1({
                correlationId: 3,
                clientId: 'test-client',
                groupId: 'share-group-1',
                memberId: 'member-uuid-123',
                memberEpoch: -1,
                rackId: null,
                subscribedTopicNames: null
            }).result;

            encoded.length.should.be.above(10);
        });

        it('should parse ShareGroupHeartbeatResponseV1 with assignment', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // error
            writer.Int16BE(0);
            // errorMessage (null)
            writer.compactNullableString(null);
            // memberId
            writer.compactNullableString('member-uuid-123');
            // memberEpoch
            writer.Int32BE(1);
            // heartbeatIntervalMs
            writer.Int32BE(5000);
            // assignment (non-null struct: Int8 = 1, then fields)
            writer.Int8(1);
            // topicPartitions array (1 item)
            writer.compactArray([{
                topicId: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
                partitions: [0, 1, 2]
            }], function (item) {
                this
                    .uuid(item.topicId)
                    .compactArray(item.partitions, this.Int32BE)
                    .TaggedFields();
            });
            // assignment TaggedFields
            writer.TaggedFields();
            // body TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).ShareGroupHeartbeatResponseV1().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
            should.not.exist(result.error);
            result.memberId.should.equal('member-uuid-123');
            result.memberEpoch.should.equal(1);
            result.heartbeatIntervalMs.should.equal(5000);
            result.assignment.should.be.an('object');
            result.assignment.topicPartitions.should.be.an('array').with.length(1);
            result.assignment.topicPartitions[0].topicId.should.equal('a1b2c3d4-e5f6-7890-abcd-ef1234567890');
            result.assignment.topicPartitions[0].partitions.should.deep.equal([0, 1, 2]);
        });

        it('should parse ShareGroupHeartbeatResponseV1 with null assignment', function () {
            var writer = protocol.write();
            var result;

            writer.Int32BE(2);
            writer.TaggedFields();
            writer.Int32BE(0);
            writer.Int16BE(0);
            writer.compactNullableString(null);
            writer.compactNullableString('member-uuid-123');
            writer.Int32BE(2);
            writer.Int32BE(5000);
            // assignment = null (Int8 = -1)
            writer.Int8(-1);
            writer.TaggedFields();

            result = protocol.read(writer.result).ShareGroupHeartbeatResponseV1().result;
            result.memberEpoch.should.equal(2);
            should.not.exist(result.assignment);
        });

        it('should parse ShareGroupHeartbeatResponseV1 with error', function () {
            var writer = protocol.write();
            var result;

            writer.Int32BE(3);
            writer.TaggedFields();
            writer.Int32BE(0);
            // error code 15 = GroupCoordinatorNotAvailable
            writer.Int16BE(15);
            writer.compactNullableString('coordinator not available');
            writer.compactNullableString(null);
            writer.Int32BE(0);
            writer.Int32BE(5000);
            writer.Int8(-1);
            writer.TaggedFields();

            result = protocol.read(writer.result).ShareGroupHeartbeatResponseV1().result;
            result.error.should.be.an('object');
            result.error.code.should.equal('GroupCoordinatorNotAvailable');
            result.errorMessage.should.equal('coordinator not available');
        });
    });

    ////////////////////////////////////////////
    // ShareFetch (apiKey 78)                 //
    ////////////////////////////////////////////

    describe('ShareFetch v1', function () {
        it('should encode ShareFetchRequestV1', function () {
            var encoded = protocol.write().ShareFetchRequestV1({
                correlationId: 10,
                clientId: 'test-client',
                groupId: 'share-group-1',
                memberId: 'member-uuid-123',
                shareSessionEpoch: 0,
                maxWaitMs: 100,
                minBytes: 1,
                maxBytes: 1048576,
                topics: [{
                    topicId: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
                    partitions: [{
                        partitionIndex: 0,
                        acknowledgementBatches: []
                    }]
                }],
                forgottenTopicsData: []
            }).result;

            // apiKey=78 (0x004E), apiVersion=1
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(0x4E);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(1);
        });

        it('should encode ShareFetchRequestV1 with piggyback acknowledgements', function () {
            var encoded = protocol.write().ShareFetchRequestV1({
                correlationId: 11,
                clientId: 'test-client',
                groupId: 'share-group-1',
                memberId: 'member-uuid-123',
                shareSessionEpoch: 1,
                maxWaitMs: 100,
                minBytes: 1,
                maxBytes: 1048576,
                topics: [{
                    topicId: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
                    partitions: [{
                        partitionIndex: 0,
                        acknowledgementBatches: [{
                            firstOffset: 0,
                            lastOffset: 4,
                            acknowledgeTypes: [1] // ACCEPT
                        }]
                    }]
                }],
                forgottenTopicsData: []
            }).result;

            encoded.length.should.be.above(30);
        });

        it('should parse ShareFetchResponseV1 with records and acquired metadata', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(10);
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // error
            writer.Int16BE(0);
            // errorMessage
            writer.compactNullableString(null);
            // acquisitionLockTimeoutMs
            writer.Int32BE(30000);
            // responses array (1 topic)
            writer.UVarint(2); // compact array: N+1
            // topic item
            writer.uuid('a1b2c3d4-e5f6-7890-abcd-ef1234567890');
            // partitions array (1 partition)
            writer.UVarint(2);
            // partition item
            writer.Int32BE(0); // partitionIndex
            writer.Int16BE(0); // error
            writer.compactNullableString(null); // errorMessage
            writer.Int16BE(0); // acknowledgeError
            writer.compactNullableString(null); // acknowledgeErrorMessage
            // currentLeader struct
            writer.Int32BE(1); // leaderId
            writer.Int32BE(5); // leaderEpoch
            writer.TaggedFields(); // leader TaggedFields
            // records = null (UVarint 0)
            writer.UVarint(0);
            // acquiredRecords array (1 item)
            writer.UVarint(2);
            writer.Int64BE(0); // firstOffset
            writer.Int64BE(4); // lastOffset
            writer.Int16BE(1); // deliveryCount
            writer.TaggedFields(); // acquiredRecord TaggedFields
            writer.TaggedFields(); // partition TaggedFields
            writer.TaggedFields(); // topic TaggedFields
            // nodeEndpoints: empty
            writer.UVarint(1);
            // body TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).ShareFetchResponseV1().result;
            result.correlationId.should.equal(10);
            result.throttleTime.should.equal(0);
            should.not.exist(result.error);
            result.acquisitionLockTimeoutMs.should.equal(30000);
            result.responses.should.be.an('array').with.length(1);
            result.responses[0].topicId.should.equal('a1b2c3d4-e5f6-7890-abcd-ef1234567890');
            result.responses[0].partitions.should.be.an('array').with.length(1);

            var p = result.responses[0].partitions[0];
            p.partitionIndex.should.equal(0);
            should.not.exist(p.error);
            p.currentLeader.leaderId.should.equal(1);
            p.currentLeader.leaderEpoch.should.equal(5);
            p.acquiredRecords.should.be.an('array').with.length(1);
            p.acquiredRecords[0].firstOffset.should.equal(0);
            p.acquiredRecords[0].lastOffset.should.equal(4);
            p.acquiredRecords[0].deliveryCount.should.equal(1);
        });

        it('should parse ShareFetchResponseV1 with partition error', function () {
            var writer = protocol.write();
            var result;

            writer.Int32BE(12);
            writer.TaggedFields();
            writer.Int32BE(0);
            writer.Int16BE(0);
            writer.compactNullableString(null);
            writer.Int32BE(30000);
            // responses: 1 topic
            writer.UVarint(2);
            writer.uuid('a1b2c3d4-e5f6-7890-abcd-ef1234567890');
            // 1 partition
            writer.UVarint(2);
            writer.Int32BE(0); // partitionIndex
            writer.Int16BE(6); // NotLeaderForPartition
            writer.compactNullableString('not the leader');
            writer.Int16BE(0); // acknowledgeError = none
            writer.compactNullableString(null);
            // currentLeader
            writer.Int32BE(2);
            writer.Int32BE(10);
            writer.TaggedFields();
            // records = null
            writer.UVarint(0);
            // acquiredRecords: empty
            writer.UVarint(1);
            writer.TaggedFields(); // partition
            writer.TaggedFields(); // topic
            // nodeEndpoints: empty
            writer.UVarint(1);
            writer.TaggedFields();

            result = protocol.read(writer.result).ShareFetchResponseV1().result;
            var p = result.responses[0].partitions[0];
            p.error.should.be.an('object');
            p.error.code.should.equal('NotLeaderForPartition');
            p.errorMessage.should.equal('not the leader');
            p.currentLeader.leaderId.should.equal(2);
        });
    });

    ////////////////////////////////////////////
    // ShareAcknowledge (apiKey 79)           //
    ////////////////////////////////////////////

    describe('ShareAcknowledge v1', function () {
        it('should encode ShareAcknowledgeRequestV1', function () {
            var encoded = protocol.write().ShareAcknowledgeRequestV1({
                correlationId: 20,
                clientId: 'test-client',
                groupId: 'share-group-1',
                memberId: 'member-uuid-123',
                shareSessionEpoch: 1,
                topics: [{
                    topicId: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
                    partitions: [{
                        partitionIndex: 0,
                        acknowledgementBatches: [{
                            firstOffset: 0,
                            lastOffset: 9,
                            acknowledgeTypes: [1] // ACCEPT
                        }]
                    }]
                }]
            }).result;

            // apiKey=79 (0x004F), apiVersion=1
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(0x4F);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(1);
        });

        it('should encode ShareAcknowledgeRequestV1 with multiple ack types', function () {
            var encoded = protocol.write().ShareAcknowledgeRequestV1({
                correlationId: 21,
                clientId: 'test-client',
                groupId: 'share-group-1',
                memberId: 'member-uuid-123',
                shareSessionEpoch: 2,
                topics: [{
                    topicId: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
                    partitions: [{
                        partitionIndex: 0,
                        acknowledgementBatches: [
                            { firstOffset: 0, lastOffset: 2, acknowledgeTypes: [1] }, // ACCEPT
                            { firstOffset: 3, lastOffset: 3, acknowledgeTypes: [3] }, // REJECT
                            { firstOffset: 4, lastOffset: 9, acknowledgeTypes: [2] }  // RELEASE
                        ]
                    }]
                }]
            }).result;

            encoded.length.should.be.above(30);
        });

        it('should parse ShareAcknowledgeResponseV1 success', function () {
            var writer = protocol.write();
            var result;

            writer.Int32BE(20);
            writer.TaggedFields();
            writer.Int32BE(0); // throttleTime
            writer.Int16BE(0); // error
            writer.compactNullableString(null); // errorMessage
            // responses: 1 topic
            writer.UVarint(2);
            writer.uuid('a1b2c3d4-e5f6-7890-abcd-ef1234567890');
            // 1 partition
            writer.UVarint(2);
            writer.Int32BE(0); // partitionIndex
            writer.Int16BE(0); // error
            writer.compactNullableString(null); // errorMessage
            // currentLeader
            writer.Int32BE(1); // leaderId
            writer.Int32BE(5); // leaderEpoch
            writer.TaggedFields();
            writer.TaggedFields(); // partition TaggedFields
            writer.TaggedFields(); // topic TaggedFields
            // nodeEndpoints: empty
            writer.UVarint(1);
            writer.TaggedFields();

            result = protocol.read(writer.result).ShareAcknowledgeResponseV1().result;
            result.correlationId.should.equal(20);
            should.not.exist(result.error);
            result.responses.should.be.an('array').with.length(1);
            result.responses[0].topicId.should.equal('a1b2c3d4-e5f6-7890-abcd-ef1234567890');
            var p = result.responses[0].partitions[0];
            p.partitionIndex.should.equal(0);
            should.not.exist(p.error);
            p.currentLeader.leaderId.should.equal(1);
            p.currentLeader.leaderEpoch.should.equal(5);
        });

        it('should parse ShareAcknowledgeResponseV1 with partition error', function () {
            var writer = protocol.write();
            var result;

            writer.Int32BE(21);
            writer.TaggedFields();
            writer.Int32BE(0);
            writer.Int16BE(0);
            writer.compactNullableString(null);
            // responses: 1 topic
            writer.UVarint(2);
            writer.uuid('a1b2c3d4-e5f6-7890-abcd-ef1234567890');
            // 1 partition with error
            writer.UVarint(2);
            writer.Int32BE(0);
            writer.Int16BE(25); // UnknownMemberId
            writer.compactNullableString('unknown member');
            writer.Int32BE(-1); // leaderId
            writer.Int32BE(-1); // leaderEpoch
            writer.TaggedFields(); // leader
            writer.TaggedFields(); // partition
            writer.TaggedFields(); // topic
            writer.UVarint(1); // empty nodeEndpoints
            writer.TaggedFields();

            result = protocol.read(writer.result).ShareAcknowledgeResponseV1().result;
            var p = result.responses[0].partitions[0];
            p.error.should.be.an('object');
            p.error.code.should.equal('UnknownMemberId');
            p.errorMessage.should.equal('unknown member');
        });
    });
});
