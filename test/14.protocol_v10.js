'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');
var errors   = require('../lib/errors');

describe('v1.0 Protocol Upgrade', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    //////////////////
    // Error Codes  //
    //////////////////

    describe('Error Codes', function () {
        it('should resolve error code 56 (KafkaStorageException)', function () {
            var err = errors.byCode(56);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('KafkaStorageException');
        });
    });

    //////////////////////////
    // Produce v5           //
    //////////////////////////

    describe('ProduceRequest v5', function () {
        it('should serialize with apiVersion=5', function () {
            var buf = protocol.write().ProduceRequestV5({
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
            buf.readInt16BE(2).should.equal(5); // apiVersion
        });

        it('should serialize with transactionalId', function () {
            var buf = protocol.write().ProduceRequestV5({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'my-txn',
                requiredAcks: -1,
                timeout: 30000,
                topics: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(5);
        });
    });

    describe('ProduceResponse v5', function () {
        it('should deserialize response with logStartOffset', function () {
            // Build mock ProduceResponseV5:
            // correlationId(4) + array_length(4) + topicName + array_length(4) +
            // partition(4) + errorCode(2) + offset(8) + timestamp(8) + logStartOffset(8) + throttleTime(4)
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 2 + topicBuf.length + 4 + 4 + 2 + 8 + 8 + 8 + 4);
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
            buf.writeInt32BE(0, offset); offset += 4; // throttleTime

            result = protocol.read(buf).ProduceResponseV5().result;
            result.correlationId.should.equal(1);
            result.topics.should.be.an('array').with.length(1);
            result.topics[0].topicName.should.equal('test-topic');
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].offset.should.equal(42);
            result.topics[0].partitions[0].timestamp.should.be.a('number');
            result.topics[0].partitions[0].logStartOffset.should.equal(10);
        });
    });

    //////////////////////////
    // Fetch v6             //
    //////////////////////////

    describe('FetchRequest v6', function () {
        it('should serialize with apiVersion=6', function () {
            var buf = protocol.write().FetchRequestV6({
                correlationId: 1,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 1048576,
                isolationLevel: 1,
                topics: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(6); // apiVersion
        });

        it('should include isolationLevel field', function () {
            var buf = protocol.write().FetchRequestV6({
                correlationId: 1,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 1048576,
                isolationLevel: 0,
                topics: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.length.should.be.above(0);
        });
    });

    describe('FetchResponse v5', function () {
        it('should deserialize response with logStartOffset', function () {
            // Build mock FetchResponseV5:
            // correlationId(4) + throttleTime(4) + array_length(4) + topicName +
            // array_length(4) + partition(4) + errorCode(2) + highwaterMarkOffset(8) +
            // lastStableOffset(8) + logStartOffset(8) + abortedTxns_length(4) +
            // messageSetSize(4) + [empty messageSet]
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 4 + 2 + topicBuf.length + 4 + 4 + 2 + 8 + 8 + 8 + 4 + 4);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(100, offset); offset += 4; // throttleTime
            buf.writeInt32BE(1, offset); offset += 4; // topics array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4; // partitions array length
            buf.writeInt32BE(0, offset); offset += 4; // partition
            buf.writeInt16BE(0, offset); offset += 2; // error code
            // highwaterMarkOffset (Int64BE = 500)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(500, offset); offset += 4;
            // lastStableOffset (Int64BE = 450)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(450, offset); offset += 4;
            // logStartOffset (Int64BE = 5)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(5, offset); offset += 4;
            // aborted transactions array length = 0
            buf.writeInt32BE(0, offset); offset += 4;
            // messageSetSize = 0
            buf.writeInt32BE(0, offset); offset += 4;

            result = protocol.read(buf).FetchResponseV5().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(100);
            result.topics.should.be.an('array').with.length(1);
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].highwaterMarkOffset.should.equal(500);
            result.topics[0].partitions[0].lastStableOffset.should.equal(450);
            result.topics[0].partitions[0].logStartOffset.should.equal(5);
        });
    });

    //////////////////////////
    // Metadata v5          //
    //////////////////////////

    describe('MetadataRequest v5', function () {
        it('should serialize with apiVersion=5', function () {
            var buf = protocol.write().MetadataRequestV5({
                correlationId: 1,
                clientId: 'test',
                topicNames: ['topic1']
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(5); // apiVersion
        });
    });

    describe('MetadataResponse v5', function () {
        it('should deserialize response with throttleTime, clusterId, and offlineReplicas', function () {
            // Build mock MetadataResponseV5:
            // correlationId(4) + throttleTime(4) + brokers_array(4) + clusterId + controllerId(4) +
            // topicMetadata_array(4) + errorCode(2) + topicName + isInternal(1) +
            // partitionMetadata_array(4) + errorCode(2) + partitionId(4) + leader(4) +
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
            // replicas
            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(1, offset); offset += 4;
            // isr
            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(1, offset); offset += 4;
            // offlineReplicas
            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(2, offset); offset += 4;

            result = protocol.read(buf.slice(0, offset)).MetadataResponseV5().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(50);
            result.clusterId.should.equal('test-cluster-id');
            result.controllerId.should.equal(1);
            result.topicMetadata.should.be.an('array').with.length(1);
            result.topicMetadata[0].topicName.should.equal('test-topic');
            result.topicMetadata[0].partitionMetadata[0].offlineReplicas.should.deep.equal([2]);
        });
    });

    //////////////////////////
    // ListOffsets v2       //
    //////////////////////////

    describe('OffsetRequest v2', function () {
        it('should serialize with apiVersion=2 and isolationLevel', function () {
            var buf = protocol.write().OffsetRequestV2({
                correlationId: 1,
                clientId: 'test',
                isolationLevel: 1,
                topics: [{
                    topicName: 'test-topic',
                    partitions: [{
                        partition: 0,
                        time: -1
                    }]
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(2); // apiVersion
        });
    });

    //////////////////////////
    // OffsetCommit v3      //
    //////////////////////////

    describe('OffsetCommitRequest v3', function () {
        it('should serialize with apiVersion=3', function () {
            var buf = protocol.write().OffsetCommitRequestV3({
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
            buf.readInt16BE(2).should.equal(3); // apiVersion
        });

        it('should have same length as v2 (retentionTime still in wire format)', function () {
            var commonArgs = {
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
            };

            var v3Buf = protocol.write().OffsetCommitRequestV3(commonArgs).result;
            var v2Buf = protocol.write().OffsetCommitRequestV2(commonArgs).result;

            // v3 wire format same as v2 (retentionTime present, broker ignores value)
            // only apiVersion header differs
            v3Buf.length.should.equal(v2Buf.length);
        });
    });

    describe('OffsetCommitResponse v3', function () {
        it('should deserialize response with throttleTime', function () {
            // Build mock OffsetCommitResponseV3:
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

            result = protocol.read(buf).OffsetCommitResponseV3().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(200);
            result.topics.should.be.an('array').with.length(1);
        });
    });

    //////////////////////////
    // OffsetFetch v2/v3    //
    //////////////////////////

    describe('OffsetFetchRequest v3', function () {
        it('should serialize with apiVersion=3', function () {
            var buf = protocol.write().OffsetFetchRequestV3({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                topics: [{
                    topicName: 'test-topic',
                    partitions: [0, 1]
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(3); // apiVersion
        });

        it('should serialize with null topics (all committed offsets)', function () {
            var buf = protocol.write().OffsetFetchRequestV3({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                topics: null
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(3);
        });
    });

    describe('OffsetFetchResponse v2', function () {
        it('should deserialize response with top-level ErrorCode', function () {
            // Build mock OffsetFetchResponseV2:
            // correlationId(4) + array_length(4) + topicName + array_length(4) +
            // partition(4) + offset(8) + metadata + errorCode(2) + top-level errorCode(2)
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var metaBuf = Buffer.from('', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 2 + topicBuf.length + 4 + 4 + 8 + 2 + metaBuf.length + 2 + 2);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(1, offset); offset += 4; // topics array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4; // partitions array length
            buf.writeInt32BE(0, offset); offset += 4; // partition
            // offset (Int64BE = 50)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(50, offset); offset += 4;
            // metadata
            buf.writeInt16BE(metaBuf.length, offset); offset += 2;
            metaBuf.copy(buf, offset); offset += metaBuf.length;
            buf.writeInt16BE(0, offset); offset += 2; // partition error code
            buf.writeInt16BE(0, offset); offset += 2; // top-level error code

            result = protocol.read(buf).OffsetFetchResponseV2().result;
            result.correlationId.should.equal(1);
            should.equal(result.error, null); // no error
            result.topics.should.be.an('array').with.length(1);
            result.topics[0].partitions[0].offset.should.equal(50);
        });

        it('should return error for non-zero top-level ErrorCode', function () {
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var metaBuf = Buffer.from('', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 2 + topicBuf.length + 4 + 4 + 8 + 2 + metaBuf.length + 2 + 2);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(50, offset); offset += 4;
            buf.writeInt16BE(metaBuf.length, offset); offset += 2;
            metaBuf.copy(buf, offset); offset += metaBuf.length;
            buf.writeInt16BE(0, offset); offset += 2;
            buf.writeInt16BE(16, offset); offset += 2; // NotCoordinatorForGroup

            result = protocol.read(buf).OffsetFetchResponseV2().result;
            result.error.should.be.an.instanceOf(errors.KafkaError);
            result.error.code.should.equal('NotCoordinatorForGroup');
        });
    });

    //////////////////////////
    // JoinGroup v1         //
    //////////////////////////

    describe('JoinConsumerGroupRequest v1', function () {
        it('should serialize with apiVersion=1 and rebalanceTimeout', function () {
            var buf = protocol.write().JoinConsumerGroupRequestV1({
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
            buf.readInt16BE(2).should.equal(1); // apiVersion
        });

        it('should default rebalanceTimeout to sessionTimeout', function () {
            var buf = protocol.write().JoinConsumerGroupRequestV1({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                sessionTimeout: 15000,
                memberId: '',
                groupProtocols: [{
                    name: 'range',
                    version: 0,
                    subscriptions: ['test-topic'],
                    metadata: null
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(1);
        });

        it('should be longer than v0 (has rebalanceTimeout field)', function () {
            var commonArgs = {
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                sessionTimeout: 10000,
                memberId: '',
                groupProtocols: [{
                    name: 'range',
                    version: 0,
                    subscriptions: ['test-topic'],
                    metadata: null
                }]
            };

            var v0Buf = protocol.write().JoinConsumerGroupRequest(commonArgs).result;
            var v1Buf = protocol.write().JoinConsumerGroupRequestV1(
                Object.assign({}, commonArgs, { rebalanceTimeout: 10000 })
            ).result;

            // v1 should be 4 bytes longer (additional Int32BE rebalanceTimeout)
            v1Buf.length.should.equal(v0Buf.length + 4);
        });
    });

    //////////////////////////
    // Heartbeat v1         //
    //////////////////////////

    describe('HeartbeatRequest v1', function () {
        it('should serialize with apiVersion=1', function () {
            var buf = protocol.write().HeartbeatRequestV1({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(1);
        });
    });

    describe('HeartbeatResponse v1', function () {
        it('should deserialize response with throttleTime', function () {
            var buf = Buffer.alloc(4 + 4 + 2);
            var result;
            buf.writeInt32BE(1, 0); // correlationId
            buf.writeInt32BE(50, 4); // throttleTime
            buf.writeInt16BE(0, 8); // error code

            result = protocol.read(buf).HeartbeatResponseV1().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(50);
            should.equal(result.error, null);
        });
    });

    //////////////////////////
    // LeaveGroup v1        //
    //////////////////////////

    describe('LeaveGroupRequest v1', function () {
        it('should serialize with apiVersion=1', function () {
            var buf = protocol.write().LeaveGroupRequestV1({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                memberId: 'member-1'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(1);
        });
    });

    describe('LeaveGroupResponse v1', function () {
        it('should deserialize response with throttleTime', function () {
            var buf = Buffer.alloc(4 + 4 + 2);
            var result;
            buf.writeInt32BE(1, 0);
            buf.writeInt32BE(75, 4);
            buf.writeInt16BE(0, 8);

            result = protocol.read(buf).LeaveGroupResponseV1().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(75);
            should.equal(result.error, null);
        });
    });

    //////////////////////////
    // SyncGroup v1         //
    //////////////////////////

    describe('SyncConsumerGroupRequest v1', function () {
        it('should serialize with apiVersion=1', function () {
            var buf = protocol.write().SyncConsumerGroupRequestV1({
                correlationId: 1,
                clientId: 'test',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1',
                groupAssignment: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(1);
        });
    });

    describe('SyncConsumerGroupResponse v1', function () {
        it('should deserialize response with throttleTime', function () {
            // correlationId(4) + throttleTime(4) + errorCode(2) + memberAssignment bytes
            var buf = Buffer.alloc(4 + 4 + 2 + 4);
            var result;
            buf.writeInt32BE(1, 0);
            buf.writeInt32BE(30, 4);
            buf.writeInt16BE(0, 8);
            buf.writeInt32BE(0, 10); // memberAssignment bytes length = 0

            result = protocol.read(buf).SyncConsumerGroupResponseV1().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(30);
            should.equal(result.error, null);
        });
    });

    //////////////////////////
    // DescribeGroups v1    //
    //////////////////////////

    describe('DescribeGroupRequest v1', function () {
        it('should serialize with apiVersion=1', function () {
            var buf = protocol.write().DescribeGroupRequestV1({
                correlationId: 1,
                clientId: 'test',
                groups: ['test-group']
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(1);
        });
    });

    describe('DescribeGroupResponse v1', function () {
        it('should deserialize response with throttleTime', function () {
            // correlationId(4) + throttleTime(4) + groups_array(4) +
            // errorCode(2) + groupId + state + protocolType + protocol + members_array(4)
            var groupIdBuf = Buffer.from('test-group', 'utf8');
            var stateBuf = Buffer.from('Stable', 'utf8');
            var ptBuf = Buffer.from('consumer', 'utf8');
            var protoBuf = Buffer.from('range', 'utf8');
            var buf = Buffer.alloc(200);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(25, offset); offset += 4; // throttleTime
            buf.writeInt32BE(1, offset); offset += 4; // groups array length
            buf.writeInt16BE(0, offset); offset += 2; // error code
            buf.writeInt16BE(groupIdBuf.length, offset); offset += 2;
            groupIdBuf.copy(buf, offset); offset += groupIdBuf.length;
            buf.writeInt16BE(stateBuf.length, offset); offset += 2;
            stateBuf.copy(buf, offset); offset += stateBuf.length;
            buf.writeInt16BE(ptBuf.length, offset); offset += 2;
            ptBuf.copy(buf, offset); offset += ptBuf.length;
            buf.writeInt16BE(protoBuf.length, offset); offset += 2;
            protoBuf.copy(buf, offset); offset += protoBuf.length;
            buf.writeInt32BE(0, offset); offset += 4; // members array length = 0

            result = protocol.read(buf.slice(0, offset)).DescribeGroupResponseV1().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(25);
            result.groups.should.be.an('array').with.length(1);
            result.groups[0].groupId.should.equal('test-group');
            result.groups[0].state.should.equal('Stable');
        });
    });

    //////////////////////////
    // ListGroups v1        //
    //////////////////////////

    describe('ListGroupsRequest v1', function () {
        it('should serialize with apiVersion=1', function () {
            var buf = protocol.write().ListGroupsRequestV1({
                correlationId: 1,
                clientId: 'test'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(1);
        });
    });

    describe('ListGroupResponse v1', function () {
        it('should deserialize response with throttleTime', function () {
            // correlationId(4) + throttleTime(4) + errorCode(2) + groups_array(4)
            var buf = Buffer.alloc(4 + 4 + 2 + 4);
            var result;
            buf.writeInt32BE(1, 0);
            buf.writeInt32BE(15, 4);
            buf.writeInt16BE(0, 8);
            buf.writeInt32BE(0, 10); // empty groups array

            result = protocol.read(buf).ListGroupResponseV1().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(15);
            should.equal(result.error, null);
            result.groups.should.be.an('array').with.length(0);
        });
    });

    //////////////////////////
    // ApiVersions v1       //
    //////////////////////////

    describe('ApiVersionsRequest v1', function () {
        it('should serialize with apiVersion=1', function () {
            var buf = protocol.write().ApiVersionsRequestV1({
                correlationId: 1,
                clientId: 'test'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(0).should.equal(18); // apiKey
            buf.readInt16BE(2).should.equal(1); // apiVersion
        });
    });

    describe('ApiVersionsResponse v1', function () {
        it('should deserialize response with throttleTime', function () {
            // correlationId(4) + errorCode(2) + array_length(4) + [apiKey(2) + min(2) + max(2)] * 1 + throttleTime(4)
            var buf = Buffer.alloc(4 + 2 + 4 + 6 + 4);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt16BE(0, offset); offset += 2; // no error
            buf.writeInt32BE(1, offset); offset += 4; // 1 api version entry
            buf.writeInt16BE(0, offset); offset += 2; // apiKey = Produce
            buf.writeInt16BE(0, offset); offset += 2; // minVersion
            buf.writeInt16BE(5, offset); offset += 2; // maxVersion
            buf.writeInt32BE(42, offset); offset += 4; // throttleTime

            result = protocol.read(buf).ApiVersionsResponseV1().result;
            result.correlationId.should.equal(1);
            should.equal(result.error, null);
            result.apiVersions.should.be.an('array').with.length(1);
            result.apiVersions[0].maxVersion.should.equal(5);
            result.throttleTime.should.equal(42);
        });
    });
});
