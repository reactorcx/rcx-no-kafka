'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');
var errors   = require('../lib/errors');

describe('v2.1 Protocol Upgrade', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    //////////////////
    // Error Codes  //
    //////////////////

    describe('Error Codes', function () {
        it('should resolve error code 52 (UnsupportedCompressionType)', function () {
            var err = errors.byCode(52);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('UnsupportedCompressionType');
        });

        it('should resolve error code 57 (LogDirNotFound)', function () {
            var err = errors.byCode(57);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('LogDirNotFound');
        });

        it('should resolve error code 58 (SaslAuthenticationFailed)', function () {
            var err = errors.byCode(58);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('SaslAuthenticationFailed');
        });

        it('should resolve error code 59 (UnknownProducerId)', function () {
            var err = errors.byCode(59);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('UnknownProducerId');
        });

        it('should resolve error code 66 (DelegationTokenExpired)', function () {
            var err = errors.byCode(66);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('DelegationTokenExpired');
        });
    });

    //////////////////////////////
    // Simple V2 Version Bumps  //
    //////////////////////////////

    describe('HeartbeatRequest v2', function () {
        it('should serialize with apiVersion=2', function () {
            var buf = protocol.write().HeartbeatRequestV2({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(2); // apiVersion
        });
    });

    describe('LeaveGroupRequest v2', function () {
        it('should serialize with apiVersion=2', function () {
            var buf = protocol.write().LeaveGroupRequestV2({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                memberId: 'member-1'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(2); // apiVersion
        });
    });

    describe('SyncConsumerGroupRequest v2', function () {
        it('should serialize with apiVersion=2', function () {
            var buf = protocol.write().SyncConsumerGroupRequestV2({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1',
                groupAssignment: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(2); // apiVersion
        });
    });

    describe('DescribeGroupRequest v2', function () {
        it('should serialize with apiVersion=2', function () {
            var buf = protocol.write().DescribeGroupRequestV2({
                correlationId: 1,
                clientId: 'test',
                groups: ['test-group']
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(2); // apiVersion
        });
    });

    describe('ListGroupsRequest v2', function () {
        it('should serialize with apiVersion=2', function () {
            var buf = protocol.write().ListGroupsRequestV2({
                correlationId: 1,
                clientId: 'test'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(2); // apiVersion
        });
    });

    describe('ApiVersionsRequest v2', function () {
        it('should serialize with apiVersion=2 and apiKey=18', function () {
            var buf = protocol.write().ApiVersionsRequestV2({
                correlationId: 1,
                clientId: 'test'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(0).should.equal(18); // apiKey
            buf.readInt16BE(2).should.equal(2); // apiVersion
        });
    });

    describe('FindCoordinatorRequest v2', function () {
        it('should serialize with apiVersion=2', function () {
            var buf = protocol.write().FindCoordinatorRequestV2({
                correlationId: 1,
                clientId: 'test',
                key: 'test-group',
                coordinatorType: 0
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(2); // apiVersion
        });
    });

    //////////////////////////
    // Produce v7           //
    //////////////////////////

    describe('ProduceRequest v7', function () {
        it('should serialize with apiVersion=7', function () {
            var buf = protocol.write().ProduceRequestV7({
                correlationId: 1,
                clientId: 'test',
                transactionalId: null,
                requiredAcks: 1,
                timeout: 30000,
                topics: [{
                    topicName: 'test-topic',
                    partitions: [{
                        partition: 0,
                        batch: {
                            baseOffset: 0,
                            records: [{ key: null, value: Buffer.from('hello') }],
                            codec: 0
                        }
                    }]
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(7); // apiVersion
        });
    });

    describe('ProduceResponse v7', function () {
        it('should deserialize response with recordErrors and errorMessage', function () {
            // Build mock ProduceResponseV7:
            // correlationId(4) + topics_array(4) + topicName + partitions_array(4) +
            // partition(4) + errorCode(2) + offset(8) + timestamp(8) + logStartOffset(8) +
            // recordErrors_array(4) [1 item: batchIndex(4) + batchIndexErrorMessage(string)] +
            // errorMessage(string) + throttleTime(4)
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var batchErrMsgBuf = Buffer.from('batch error', 'utf8');
            var errMsgBuf = Buffer.from('top error', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 2 + topicBuf.length + 4 + 4 + 2 + 8 + 8 + 8 +
                4 + 4 + 2 + batchErrMsgBuf.length + 2 + errMsgBuf.length + 4);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(1, offset); offset += 4; // topics array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4; // partitions array length
            buf.writeInt32BE(0, offset); offset += 4; // partition
            buf.writeInt16BE(0, offset); offset += 2; // error code (no error)
            // offset (Int64BE = 42)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(42, offset); offset += 4;
            // timestamp (Int64BE)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(1500000000, offset); offset += 4;
            // logStartOffset (Int64BE = 10)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(10, offset); offset += 4;
            // recordErrors array length = 1
            buf.writeInt32BE(1, offset); offset += 4;
            // recordError item: batchIndex(4) + batchIndexErrorMessage(string)
            buf.writeInt32BE(3, offset); offset += 4; // batchIndex = 3
            buf.writeInt16BE(batchErrMsgBuf.length, offset); offset += 2;
            batchErrMsgBuf.copy(buf, offset); offset += batchErrMsgBuf.length;
            // errorMessage (string)
            buf.writeInt16BE(errMsgBuf.length, offset); offset += 2;
            errMsgBuf.copy(buf, offset); offset += errMsgBuf.length;
            // throttleTime
            buf.writeInt32BE(50, offset); offset += 4;

            result = protocol.read(buf.slice(0, offset)).ProduceResponseV7().result;
            result.correlationId.should.equal(1);
            result.topics.should.be.an('array').with.length(1);
            result.topics[0].topicName.should.equal('test-topic');
            result.topics[0].partitions[0].partition.should.equal(0);
            should.equal(result.topics[0].partitions[0].error, null);
            result.topics[0].partitions[0].offset.should.equal(42);
            result.topics[0].partitions[0].logStartOffset.should.equal(10);
            result.topics[0].partitions[0].recordErrors.should.be.an('array').with.length(1);
            result.topics[0].partitions[0].recordErrors[0].batchIndex.should.equal(3);
            result.topics[0].partitions[0].recordErrors[0].batchIndexErrorMessage.should.equal('batch error');
            result.topics[0].partitions[0].errorMessage.should.equal('top error');
            result.throttleTime.should.equal(50);
        });
    });

    //////////////////////////
    // JoinGroup v3         //
    //////////////////////////

    describe('JoinConsumerGroupRequest v3', function () {
        it('should serialize with apiVersion=3', function () {
            var buf = protocol.write().JoinConsumerGroupRequestV3({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                sessionTimeout: 10000,
                rebalanceTimeout: 60000,
                memberId: '',
                groupProtocols: [{
                    name: 'range',
                    version: 0,
                    subscriptions: ['test-topic'],
                    metadata: null
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(3); // apiVersion
        });
    });

    describe('JoinConsumerGroupResponse v3', function () {
        it('should deserialize response with throttleTime before error', function () {
            // Build mock JoinConsumerGroupResponseV3:
            // correlationId(4) + throttleTime(4) + errorCode(2) + generationId(4) +
            // groupProtocol(string) + leaderId(string) + memberId(string) + members_array(4) [empty]
            var gpBuf = Buffer.from('range', 'utf8');
            var leaderBuf = Buffer.from('leader-1', 'utf8');
            var memberBuf = Buffer.from('member-1', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 2 + 4 + 2 + gpBuf.length + 2 + leaderBuf.length + 2 + memberBuf.length + 4);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(100, offset); offset += 4; // throttleTime
            buf.writeInt16BE(0, offset); offset += 2; // error code (no error)
            buf.writeInt32BE(5, offset); offset += 4; // generationId
            buf.writeInt16BE(gpBuf.length, offset); offset += 2;
            gpBuf.copy(buf, offset); offset += gpBuf.length;
            buf.writeInt16BE(leaderBuf.length, offset); offset += 2;
            leaderBuf.copy(buf, offset); offset += leaderBuf.length;
            buf.writeInt16BE(memberBuf.length, offset); offset += 2;
            memberBuf.copy(buf, offset); offset += memberBuf.length;
            buf.writeInt32BE(0, offset); offset += 4; // members array length = 0

            result = protocol.read(buf.slice(0, offset)).JoinConsumerGroupResponseV3().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(100);
            should.equal(result.error, null);
            result.generationId.should.equal(5);
            result.groupProtocol.should.equal('range');
            result.leaderId.should.equal('leader-1');
            result.memberId.should.equal('member-1');
            result.members.should.be.an('array').with.length(0);
        });
    });

    //////////////////////////
    // OffsetCommit v6      //
    //////////////////////////

    describe('OffsetCommitRequest v6', function () {
        it('should serialize with apiVersion=6', function () {
            var buf = protocol.write().OffsetCommitRequestV6({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1',
                topics: [{
                    topicName: 'test-topic',
                    partitions: [{
                        partition: 0,
                        offset: 100,
                        metadata: null
                    }]
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(6); // apiVersion
        });

        it('should have different apiVersion than v3', function () {
            var v6Buf = Buffer.from(protocol.write().OffsetCommitRequestV6({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1',
                topics: [{
                    topicName: 'test-topic',
                    partitions: [{
                        partition: 0,
                        offset: 100,
                        metadata: null
                    }]
                }]
            }).result);

            var v3Buf = Buffer.from(protocol.write().OffsetCommitRequestV3({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1',
                retentionTime: -1,
                topics: [{
                    topicName: 'test-topic',
                    partitions: [{
                        partition: 0,
                        offset: 100,
                        metadata: null
                    }]
                }]
            }).result);

            v6Buf.readInt16BE(2).should.equal(6);
            v3Buf.readInt16BE(2).should.equal(3);
        });
    });

    describe('OffsetCommitResponse v6', function () {
        it('should deserialize response with throttleTime', function () {
            // Build mock OffsetCommitResponseV6:
            // correlationId(4) + throttleTime(4) + array_length(4) + topicName + array_length(4) + partition(4) + errorCode(2)
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 4 + 2 + topicBuf.length + 4 + 4 + 2);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(200, offset); offset += 4; // throttleTime
            buf.writeInt32BE(1, offset); offset += 4; // topics array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4; // partitions array length
            buf.writeInt32BE(0, offset); offset += 4; // partition
            buf.writeInt16BE(0, offset); offset += 2; // error code

            result = protocol.read(buf).OffsetCommitResponseV6().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(200);
            result.topics.should.be.an('array').with.length(1);
            result.topics[0].topicName.should.equal('test-topic');
            result.topics[0].partitions[0].partition.should.equal(0);
            should.equal(result.topics[0].partitions[0].error, null);
        });
    });

    //////////////////////////
    // OffsetFetch v5       //
    //////////////////////////

    describe('OffsetFetchRequest v5', function () {
        it('should serialize with apiVersion=5', function () {
            var buf = protocol.write().OffsetFetchRequestV5({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                topics: [{
                    topicName: 'test-topic',
                    partitions: [0, 1]
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(5); // apiVersion
        });
    });

    describe('OffsetFetchResponse v5', function () {
        it('should deserialize response with committedLeaderEpoch per partition', function () {
            // Build mock OffsetFetchResponseV5:
            // correlationId(4) + throttleTime(4) + topics_array(4) + topicName +
            // partitions_array(4) + partition(4) + offset(8) + committedLeaderEpoch(4) +
            // metadata(string) + errorCode(2) + top-level errorCode(2)
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var metaBuf = Buffer.from('', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 4 + 2 + topicBuf.length + 4 + 4 + 8 + 4 + 2 + metaBuf.length + 2 + 2);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(150, offset); offset += 4; // throttleTime
            buf.writeInt32BE(1, offset); offset += 4; // topics array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4; // partitions array length
            buf.writeInt32BE(0, offset); offset += 4; // partition
            // offset (Int64BE = 75)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(75, offset); offset += 4;
            // committedLeaderEpoch
            buf.writeInt32BE(12, offset); offset += 4;
            // metadata
            buf.writeInt16BE(metaBuf.length, offset); offset += 2;
            metaBuf.copy(buf, offset); offset += metaBuf.length;
            buf.writeInt16BE(0, offset); offset += 2; // partition error code
            buf.writeInt16BE(0, offset); offset += 2; // top-level error code

            result = protocol.read(buf.slice(0, offset)).OffsetFetchResponseV5().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(150);
            should.equal(result.error, null);
            result.topics.should.be.an('array').with.length(1);
            result.topics[0].topicName.should.equal('test-topic');
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].offset.should.equal(75);
            result.topics[0].partitions[0].committedLeaderEpoch.should.equal(12);
        });
    });

    //////////////////////////
    // Metadata v7          //
    //////////////////////////

    describe('MetadataRequest v7', function () {
        it('should serialize with apiVersion=7', function () {
            var buf = protocol.write().MetadataRequestV7({
                correlationId: 1,
                clientId: 'test',
                topicNames: ['topic1']
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(7); // apiVersion
        });
    });

    describe('MetadataResponse v7', function () {
        it('should deserialize response with leaderEpoch per partition', function () {
            // Build mock MetadataResponseV7:
            // correlationId(4) + throttleTime(4) + brokers_array(4) + clusterId + controllerId(4) +
            // topicMetadata_array(4) + errorCode(2) + topicName + isInternal(1) +
            // partitionMetadata_array(4) + errorCode(2) + partitionId(4) + leader(4) + leaderEpoch(4) +
            // replicas_array(4+4) + isr_array(4+4) + offlineReplicas_array(4+4)
            var clusterIdBuf = Buffer.from('test-cluster-id', 'utf8');
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var buf = Buffer.alloc(200);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(50, offset); offset += 4; // throttleTime
            buf.writeInt32BE(0, offset); offset += 4; // brokers array length = 0
            // clusterId
            buf.writeInt16BE(clusterIdBuf.length, offset); offset += 2;
            clusterIdBuf.copy(buf, offset); offset += clusterIdBuf.length;
            buf.writeInt32BE(1, offset); offset += 4; // controllerId
            // topicMetadata array
            buf.writeInt32BE(1, offset); offset += 4; // 1 topic
            buf.writeInt16BE(0, offset); offset += 2; // error code = 0
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt8(0, offset); offset += 1; // isInternal
            // partitionMetadata array
            buf.writeInt32BE(1, offset); offset += 4; // 1 partition
            buf.writeInt16BE(0, offset); offset += 2; // error code = 0
            buf.writeInt32BE(0, offset); offset += 4; // partitionId
            buf.writeInt32BE(1, offset); offset += 4; // leader
            buf.writeInt32BE(7, offset); offset += 4; // leaderEpoch
            // replicas
            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(1, offset); offset += 4;
            // isr
            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(1, offset); offset += 4;
            // offlineReplicas
            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(2, offset); offset += 4;

            result = protocol.read(buf.slice(0, offset)).MetadataResponseV7().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(50);
            result.clusterId.should.equal('test-cluster-id');
            result.controllerId.should.equal(1);
            result.topicMetadata.should.be.an('array').with.length(1);
            result.topicMetadata[0].topicName.should.equal('test-topic');
            result.topicMetadata[0].partitionMetadata[0].partitionId.should.equal(0);
            result.topicMetadata[0].partitionMetadata[0].leader.should.equal(1);
            result.topicMetadata[0].partitionMetadata[0].leaderEpoch.should.equal(7);
            result.topicMetadata[0].partitionMetadata[0].offlineReplicas.should.deep.equal([2]);
        });
    });

    //////////////////////////
    // ListOffsets v4       //
    //////////////////////////

    describe('OffsetRequest v4', function () {
        it('should serialize with apiVersion=4', function () {
            var buf = protocol.write().OffsetRequestV4({
                correlationId: 1,
                clientId: 'test',
                isolationLevel: 0,
                topics: [{
                    topicName: 'test-topic',
                    partitions: [{
                        partition: 0,
                        time: -1,
                        currentLeaderEpoch: -1
                    }]
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(4); // apiVersion
        });
    });

    describe('OffsetResponse v4', function () {
        it('should deserialize response with leaderEpoch per partition', function () {
            // Build mock OffsetResponseV4:
            // correlationId(4) + throttleTime(4) + topics_array(4) + topicName +
            // partitions_array(4) + partition(4) + errorCode(2) + timestamp(8) + offset(8) + leaderEpoch(4)
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 4 + 2 + topicBuf.length + 4 + 4 + 2 + 8 + 8 + 4);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(80, offset); offset += 4; // throttleTime
            buf.writeInt32BE(1, offset); offset += 4; // topics array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4; // partitions array length
            buf.writeInt32BE(0, offset); offset += 4; // partition
            buf.writeInt16BE(0, offset); offset += 2; // error code (no error)
            // timestamp (Int64BE = 1600000000)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(1600000000, offset); offset += 4;
            // offset (Int64BE = 250)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(250, offset); offset += 4;
            // leaderEpoch
            buf.writeInt32BE(9, offset); offset += 4;

            result = protocol.read(buf.slice(0, offset)).OffsetResponseV4().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(80);
            result.topics.should.be.an('array').with.length(1);
            result.topics[0].topicName.should.equal('test-topic');
            result.topics[0].partitions[0].partition.should.equal(0);
            should.equal(result.topics[0].partitions[0].error, null);
            result.topics[0].partitions[0].offset.should.equal(250);
            result.topics[0].partitions[0].leaderEpoch.should.equal(9);
        });
    });

    //////////////////////////
    // Fetch v7             //
    //////////////////////////

    describe('FetchRequest v7', function () {
        it('should serialize with apiVersion=7 and sessionId/sessionEpoch fields', function () {
            var buf = protocol.write().FetchRequestV7({
                correlationId: 1,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 1048576,
                isolationLevel: 0,
                sessionId: 42,
                sessionEpoch: 3,
                topics: [],
                forgottenTopicsData: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(7); // apiVersion
        });
    });

    describe('FetchRequest v10', function () {
        it('should serialize with apiVersion=10', function () {
            var buf = protocol.write().FetchRequestV10({
                correlationId: 1,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 1048576,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [],
                forgottenTopicsData: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(10); // apiVersion
        });
    });

    describe('FetchResponse v7', function () {
        it('should deserialize response with errorCode and sessionId', function () {
            // Build mock FetchResponseV7:
            // correlationId(4) + throttleTime(4) + errorCode(2) + sessionId(4) + topics_array(4) [empty]
            var buf = Buffer.alloc(4 + 4 + 2 + 4 + 4);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(60, offset); offset += 4; // throttleTime
            buf.writeInt16BE(0, offset); offset += 2; // errorCode (no error)
            buf.writeInt32BE(99, offset); offset += 4; // sessionId
            buf.writeInt32BE(0, offset); offset += 4; // topics array length = 0

            result = protocol.read(buf).FetchResponseV7().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(60);
            should.equal(result.error, null);
            result.sessionId.should.equal(99);
            result.topics.should.be.an('array').with.length(0);
        });
    });
});
