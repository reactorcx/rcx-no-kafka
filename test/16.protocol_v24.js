'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');
var errors   = require('../lib/errors');

describe('v2.4 Protocol Upgrade', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    //////////////////
    // Error Codes  //
    //////////////////

    describe('Error Codes (68-89)', function () {
        it('should resolve error code 68 (NonEmptyGroup)', function () {
            var err = errors.byCode(68);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('NonEmptyGroup');
        });

        it('should resolve error code 69 (GroupIdNotFound)', function () {
            var err = errors.byCode(69);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('GroupIdNotFound');
        });

        it('should resolve error code 74 (FencedLeaderEpoch)', function () {
            var err = errors.byCode(74);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('FencedLeaderEpoch');
        });

        it('should resolve error code 79 (MemberIdRequired)', function () {
            var err = errors.byCode(79);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('MemberIdRequired');
        });

        it('should resolve error code 82 (FencedInstanceId)', function () {
            var err = errors.byCode(82);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('FencedInstanceId');
        });

        it('should resolve error code 89 (ThrottlingQuotaExceeded)', function () {
            var err = errors.byCode(89);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('ThrottlingQuotaExceeded');
        });
    });

    //////////////////////////////
    // Simple Version Bumps     //
    //////////////////////////////

    describe('ProduceRequest v8', function () {
        it('should serialize with apiVersion=8', function () {
            var buf = protocol.write().ProduceRequestV8({
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
            buf.readInt16BE(2).should.equal(8); // apiVersion
        });
    });

    describe('OffsetRequest v5', function () {
        it('should serialize with apiVersion=5', function () {
            var buf = protocol.write().OffsetRequestV5({
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
            buf.readInt16BE(2).should.equal(5); // apiVersion
        });
    });

    describe('InitProducerIdRequest v1', function () {
        it('should serialize with apiVersion=1', function () {
            var buf = protocol.write().InitProducerIdRequestV1({
                correlationId: 1,
                clientId: 'test',
                transactionalId: null,
                transactionTimeoutMs: 60000
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(1); // apiVersion
        });
    });

    //////////////////////////////////////
    // Static Membership Protocols      //
    //////////////////////////////////////

    describe('JoinConsumerGroupRequest v5', function () {
        it('should serialize with apiVersion=5 and groupInstanceId', function () {
            var buf = protocol.write().JoinConsumerGroupRequestV5({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                sessionTimeout: 10000,
                rebalanceTimeout: 60000,
                memberId: '',
                groupInstanceId: 'instance-1',
                groupProtocols: [{
                    name: 'range',
                    version: 0,
                    subscriptions: ['test-topic'],
                    metadata: null
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(5); // apiVersion
        });

        it('should serialize with null groupInstanceId', function () {
            var buf = protocol.write().JoinConsumerGroupRequestV5({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                sessionTimeout: 10000,
                rebalanceTimeout: 60000,
                memberId: '',
                groupInstanceId: null,
                groupProtocols: [{
                    name: 'range',
                    version: 0,
                    subscriptions: ['test-topic'],
                    metadata: null
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(5); // apiVersion
        });
    });

    describe('JoinConsumerGroupResponse v5', function () {
        it('should deserialize response with groupInstanceId per member', function () {
            var gpBuf = Buffer.from('range', 'utf8');
            var leaderBuf = Buffer.from('leader-1', 'utf8');
            var memberBuf = Buffer.from('member-1', 'utf8');
            var memberIdBuf = Buffer.from('m-1', 'utf8');
            var instanceIdBuf = Buffer.from('instance-1', 'utf8');
            // member metadata: bytes_length(4) + version(2) + subscriptions_array(4) + topic_string + metadata_bytes(4)
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var metadataSize = 4 + 2 + 4 + 2 + topicBuf.length + 4;
            var buf = Buffer.alloc(4 + 4 + 2 + 4 + 2 + gpBuf.length + 2 + leaderBuf.length + 2 + memberBuf.length +
                4 + 2 + memberIdBuf.length + 2 + instanceIdBuf.length + metadataSize);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(100, offset); offset += 4; // throttleTime
            buf.writeInt16BE(0, offset); offset += 2; // error code
            buf.writeInt32BE(5, offset); offset += 4; // generationId
            buf.writeInt16BE(gpBuf.length, offset); offset += 2;
            gpBuf.copy(buf, offset); offset += gpBuf.length;
            buf.writeInt16BE(leaderBuf.length, offset); offset += 2;
            leaderBuf.copy(buf, offset); offset += leaderBuf.length;
            buf.writeInt16BE(memberBuf.length, offset); offset += 2;
            memberBuf.copy(buf, offset); offset += memberBuf.length;
            // members array
            buf.writeInt32BE(1, offset); offset += 4; // 1 member
            buf.writeInt16BE(memberIdBuf.length, offset); offset += 2;
            memberIdBuf.copy(buf, offset); offset += memberIdBuf.length;
            buf.writeInt16BE(instanceIdBuf.length, offset); offset += 2;
            instanceIdBuf.copy(buf, offset); offset += instanceIdBuf.length;
            // member metadata bytes
            buf.writeInt32BE(metadataSize - 4, offset); offset += 4; // bytes length
            buf.writeInt16BE(0, offset); offset += 2; // version
            buf.writeInt32BE(1, offset); offset += 4; // subscriptions array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(-1, offset); offset += 4; // metadata bytes = null

            result = protocol.read(buf.slice(0, offset)).JoinConsumerGroupResponseV5().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(100);
            should.equal(result.error, null);
            result.generationId.should.equal(5);
            result.memberId.should.equal('member-1');
            result.members.should.be.an('array').with.length(1);
            result.members[0].id.should.equal('m-1');
            result.members[0].groupInstanceId.should.equal('instance-1');
        });
    });

    describe('HeartbeatRequest v3', function () {
        it('should serialize with apiVersion=3 and groupInstanceId', function () {
            var buf = protocol.write().HeartbeatRequestV3({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1',
                groupInstanceId: 'instance-1'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(3); // apiVersion
        });
    });

    describe('SyncConsumerGroupRequest v3', function () {
        it('should serialize with apiVersion=3 and groupInstanceId', function () {
            var buf = protocol.write().SyncConsumerGroupRequestV3({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1',
                groupInstanceId: 'instance-1',
                groupAssignment: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(3); // apiVersion
        });
    });

    describe('LeaveGroupRequest v3', function () {
        it('should serialize with apiVersion=3 and members array', function () {
            var buf = protocol.write().LeaveGroupRequestV3({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                members: [
                    { memberId: 'member-1', groupInstanceId: 'instance-1' },
                    { memberId: 'member-2', groupInstanceId: null }
                ]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(3); // apiVersion
        });
    });

    describe('LeaveGroupResponse v3', function () {
        it('should deserialize response with per-member errors', function () {
            var memberIdBuf = Buffer.from('member-1', 'utf8');
            var instanceIdBuf = Buffer.from('instance-1', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 2 + 4 + 2 + memberIdBuf.length + 2 + instanceIdBuf.length + 2);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(50, offset); offset += 4; // throttleTime
            buf.writeInt16BE(0, offset); offset += 2; // top-level error code
            buf.writeInt32BE(1, offset); offset += 4; // members array length
            buf.writeInt16BE(memberIdBuf.length, offset); offset += 2;
            memberIdBuf.copy(buf, offset); offset += memberIdBuf.length;
            buf.writeInt16BE(instanceIdBuf.length, offset); offset += 2;
            instanceIdBuf.copy(buf, offset); offset += instanceIdBuf.length;
            buf.writeInt16BE(0, offset); offset += 2; // member error code

            result = protocol.read(buf.slice(0, offset)).LeaveGroupResponseV3().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(50);
            should.equal(result.error, null);
            result.members.should.be.an('array').with.length(1);
            result.members[0].memberId.should.equal('member-1');
            result.members[0].groupInstanceId.should.equal('instance-1');
            should.equal(result.members[0].error, null);
        });
    });

    describe('OffsetCommitRequest v7', function () {
        it('should serialize with apiVersion=7 and groupInstanceId', function () {
            var buf = protocol.write().OffsetCommitRequestV7({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1',
                groupInstanceId: 'instance-1',
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
            buf.readInt16BE(2).should.equal(7); // apiVersion
        });
    });

    describe('DescribeGroupRequest v3', function () {
        it('should serialize with apiVersion=3 and includeAuthorizedOperations', function () {
            var buf = protocol.write().DescribeGroupRequestV3({
                correlationId: 1,
                clientId: 'test',
                groups: ['test-group'],
                includeAuthorizedOperations: 1
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(3); // apiVersion
        });
    });

    describe('DescribeGroupRequest v4', function () {
        it('should serialize with apiVersion=4', function () {
            var buf = protocol.write().DescribeGroupRequestV4({
                correlationId: 1,
                clientId: 'test',
                groups: ['test-group'],
                includeAuthorizedOperations: 0
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(4); // apiVersion
        });
    });

    describe('DescribeGroupResponse v3', function () {
        it('should deserialize response with authorizedOperations', function () {
            var groupIdBuf = Buffer.from('test-group', 'utf8');
            var stateBuf = Buffer.from('Stable', 'utf8');
            var protoTypeBuf = Buffer.from('consumer', 'utf8');
            var protocolBuf = Buffer.from('range', 'utf8');
            var buf = Buffer.alloc(200);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(100, offset); offset += 4; // throttleTime
            buf.writeInt32BE(1, offset); offset += 4; // groups array length
            buf.writeInt16BE(0, offset); offset += 2; // error code
            buf.writeInt16BE(groupIdBuf.length, offset); offset += 2;
            groupIdBuf.copy(buf, offset); offset += groupIdBuf.length;
            buf.writeInt16BE(stateBuf.length, offset); offset += 2;
            stateBuf.copy(buf, offset); offset += stateBuf.length;
            buf.writeInt16BE(protoTypeBuf.length, offset); offset += 2;
            protoTypeBuf.copy(buf, offset); offset += protoTypeBuf.length;
            buf.writeInt16BE(protocolBuf.length, offset); offset += 2;
            protocolBuf.copy(buf, offset); offset += protocolBuf.length;
            buf.writeInt32BE(0, offset); offset += 4; // members array length = 0
            buf.writeInt32BE(2047, offset); offset += 4; // authorizedOperations

            result = protocol.read(buf.slice(0, offset)).DescribeGroupResponseV3().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(100);
            result.groups.should.be.an('array').with.length(1);
            result.groups[0].groupId.should.equal('test-group');
            result.groups[0].state.should.equal('Stable');
            result.groups[0].authorizedOperations.should.equal(2047);
        });
    });

    //////////////////////////////
    // Metadata v8              //
    //////////////////////////////

    describe('MetadataRequest v8', function () {
        it('should serialize with apiVersion=8 and authorized operations flags', function () {
            var buf = protocol.write().MetadataRequestV8({
                correlationId: 1,
                clientId: 'test',
                topicNames: ['topic1'],
                includeClusterAuthorizedOperations: 1,
                includeTopicAuthorizedOperations: 1
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(8); // apiVersion
        });
    });

    describe('MetadataResponse v8', function () {
        it('should deserialize response with authorizedOperations', function () {
            var clusterIdBuf = Buffer.from('cluster-1', 'utf8');
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var buf = Buffer.alloc(200);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(50, offset); offset += 4; // throttleTime
            buf.writeInt32BE(0, offset); offset += 4; // brokers array length = 0
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
            buf.writeInt32BE(0, offset); offset += 4;
            // topicAuthorizedOperations
            buf.writeInt32BE(1023, offset); offset += 4;
            // clusterAuthorizedOperations
            buf.writeInt32BE(511, offset); offset += 4;

            result = protocol.read(buf.slice(0, offset)).MetadataResponseV8().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(50);
            result.clusterId.should.equal('cluster-1');
            result.controllerId.should.equal(1);
            result.topicMetadata.should.be.an('array').with.length(1);
            result.topicMetadata[0].topicName.should.equal('test-topic');
            result.topicMetadata[0].topicAuthorizedOperations.should.equal(1023);
            result.clusterAuthorizedOperations.should.equal(511);
        });
    });

    //////////////////////////////
    // Fetch v11                //
    //////////////////////////////

    describe('FetchRequest v11', function () {
        it('should serialize with apiVersion=11 and rackId', function () {
            var buf = protocol.write().FetchRequestV11({
                correlationId: 1,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 1048576,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [],
                forgottenTopicsData: [],
                rackId: 'rack-1'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(11); // apiVersion
        });
    });

    describe('FetchResponse v11', function () {
        it('should deserialize response with preferredReadReplica per partition', function () {
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var buf = Buffer.alloc(200);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(60, offset); offset += 4; // throttleTime
            buf.writeInt16BE(0, offset); offset += 2; // errorCode
            buf.writeInt32BE(99, offset); offset += 4; // sessionId
            buf.writeInt32BE(1, offset); offset += 4; // topics array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4; // partitions array length
            buf.writeInt32BE(0, offset); offset += 4; // partition
            buf.writeInt16BE(0, offset); offset += 2; // error code
            // highwaterMarkOffset (Int64BE = 100)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(100, offset); offset += 4;
            // lastStableOffset (Int64BE = 100)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(100, offset); offset += 4;
            // logStartOffset (Int64BE = 0)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(0, offset); offset += 4;
            // abortedTransactions array length = 0
            buf.writeInt32BE(0, offset); offset += 4;
            // preferredReadReplica
            buf.writeInt32BE(2, offset); offset += 4;
            // messageSetSize = 0
            buf.writeInt32BE(0, offset); offset += 4;

            result = protocol.read(buf.slice(0, offset)).FetchResponseV11().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(60);
            should.equal(result.error, null);
            result.sessionId.should.equal(99);
            result.topics.should.be.an('array').with.length(1);
            result.topics[0].topicName.should.equal('test-topic');
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].preferredReadReplica.should.equal(2);
            result.topics[0].partitions[0].highwaterMarkOffset.should.equal(100);
            result.topics[0].partitions[0].messageSet.should.be.an('array').with.length(0);
        });
    });
});
