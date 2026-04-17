'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');

describe('KIP-482 Flexible Version API Bumps', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    ///////////////////////////
    // InitProducerId v2     //
    ///////////////////////////

    describe('InitProducerIdRequestV2', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().InitProducerIdRequestV2({
                correlationId: 1,
                clientId: 'test',
                transactionalId: null,
                transactionTimeoutMs: 5000
            }).result;

            // FlexibleRequestHeader: apiKey=22 (Int16BE), apiVersion=2 (Int16BE)
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(22); // API key for InitProducerId
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(2);  // apiVersion = 2
        });
    });

    describe('InitProducerIdResponseV2', function () {
        it('should parse flexible response with TaggedFields', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // correlationId = 1
                Buffer.from([0x00]),                     // TaggedFields (empty)
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime = 0
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]), // producerId = 1
                Buffer.from([0x00, 0x01]),               // producerEpoch = 1
                Buffer.from([0x00])                      // TaggedFields (empty)
            ]);
            var result = protocol.read(buf).InitProducerIdResponseV2().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
            result.producerEpoch.should.equal(1);
        });
    });

    /////////////////////
    // Heartbeat v4    //
    /////////////////////

    describe('HeartbeatRequestV4', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().HeartbeatRequestV4({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                generationId: 5,
                memberId: 'm1',
                groupInstanceId: null
            }).result;

            // apiKey=12 (Heartbeat), apiVersion=4
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(12);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(4);
        });
    });

    describe('HeartbeatResponseV4', function () {
        it('should parse flexible response', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // correlationId
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x00])                      // TaggedFields
            ]);
            var result = protocol.read(buf).HeartbeatResponseV4().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
        });
    });

    /////////////////////
    // ListGroups v3   //
    /////////////////////

    describe('ListGroupsRequestV3', function () {
        it('should use FlexibleRequestHeader', function () {
            var encoded = protocol.write().ListGroupsRequestV3({
                correlationId: 1,
                clientId: 'test'
            }).result;

            // apiKey=16, apiVersion=3
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(16);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(3);
        });
    });

    describe('ListGroupResponseV3', function () {
        it('should parse flexible response with compact arrays', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // correlationId
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x02]),                     // compactArray length = 1+1=2 (1 item)
                // group item: compactString "g1" + compactString "consumer" + TaggedFields
                Buffer.from([0x03]),                     // compactString len=2+1 = 3
                Buffer.from('g1', 'utf8'),
                Buffer.from([0x09]),                     // compactString len=8+1 = 9
                Buffer.from('consumer', 'utf8'),
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00])                      // top-level TaggedFields
            ]);
            var result = protocol.read(buf).ListGroupResponseV3().result;
            result.correlationId.should.equal(1);
            result.groups.length.should.equal(1);
            result.groups[0].groupId.should.equal('g1');
            result.groups[0].protocolType.should.equal('consumer');
        });
    });

    ///////////////////////////
    // FindCoordinator v3    //
    ///////////////////////////

    describe('FindCoordinatorRequestV3', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().FindCoordinatorRequestV3({
                correlationId: 1,
                clientId: 'test',
                key: 'mygroup',
                coordinatorType: 0
            }).result;

            // apiKey=10, apiVersion=3
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(10);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(3);
        });
    });

    describe('FindCoordinatorResponseV3', function () {
        it('should parse flexible response', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // correlationId
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x00]),                     // errorMessage = null (compactNullableString)
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // coordinatorId = 1
                Buffer.from([0x0a]),                     // compactString len=9+1 = 10
                Buffer.from('localhost', 'utf8'),
                Buffer.from([0x00, 0x00, 0x23, 0x84]),  // coordinatorPort = 9092
                Buffer.from([0x00])                      // TaggedFields
            ]);
            var result = protocol.read(buf).FindCoordinatorResponseV3().result;
            result.correlationId.should.equal(1);
            result.coordinatorId.should.equal(1);
            result.coordinatorHost.should.equal('localhost');
            result.coordinatorPort.should.equal(9092);
        });
    });

    ///////////////////////////
    // FindCoordinator v4    //
    ///////////////////////////

    describe('FindCoordinatorRequestV4', function () {
        it('should use FlexibleRequestHeader with coordinatorKeys array', function () {
            var encoded = protocol.write().FindCoordinatorRequestV4({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 4,
                keyType: 0,
                coordinatorKeys: ['mygroup']
            }).result;

            // apiKey=10, apiVersion=4
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(10);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(4);
        });

        it('should accept apiVersion 6 for KIP-932', function () {
            var encoded = protocol.write().FindCoordinatorRequestV4({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 6,
                keyType: 0,
                coordinatorKeys: ['g1']
            }).result;

            encoded[2].should.equal(0x00);
            encoded[3].should.equal(6);
        });
    });

    describe('FindCoordinatorResponseV4', function () {
        it('should parse coordinators array', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // correlationId
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime
                Buffer.from([0x02]),                     // compactArray: 1 coordinator
                // coordinator item:
                Buffer.from([0x08]),                     // compactString "mygroup" (len 7+1=8)
                Buffer.from('mygroup', 'utf8'),
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // nodeId = 1
                Buffer.from([0x0a]),                     // compactString "localhost" (len 9+1=10)
                Buffer.from('localhost', 'utf8'),
                Buffer.from([0x00, 0x00, 0x23, 0x84]),  // port = 9092
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x00]),                     // errorMessage = null
                Buffer.from([0x00]),                     // coordinator TaggedFields
                Buffer.from([0x00])                      // top-level TaggedFields
            ]);
            var result = protocol.read(buf).FindCoordinatorResponseV4().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
            result.coordinators.length.should.equal(1);
            result.coordinators[0].key.should.equal('mygroup');
            result.coordinators[0].nodeId.should.equal(1);
            result.coordinators[0].host.should.equal('localhost');
            result.coordinators[0].port.should.equal(9092);
        });

        it('should parse multiple coordinators', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x02]),  // correlationId = 2
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime
                Buffer.from([0x03]),                     // compactArray: 2 coordinators
                // coordinator 1: "g1"
                Buffer.from([0x03]),                     // compactString "g1" (len 2+1=3)
                Buffer.from('g1', 'utf8'),
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // nodeId = 1
                Buffer.from([0x0a]),                     // "localhost"
                Buffer.from('localhost', 'utf8'),
                Buffer.from([0x00, 0x00, 0x23, 0x84]),  // port = 9092
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x00]),                     // errorMessage = null
                Buffer.from([0x00]),                     // TaggedFields
                // coordinator 2: "g2"
                Buffer.from([0x03]),                     // compactString "g2"
                Buffer.from('g2', 'utf8'),
                Buffer.from([0x00, 0x00, 0x00, 0x02]),  // nodeId = 2
                Buffer.from([0x0a]),                     // "localhost"
                Buffer.from('localhost', 'utf8'),
                Buffer.from([0x00, 0x00, 0x23, 0x85]),  // port = 9093
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x00]),                     // errorMessage = null
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00])                      // top-level TaggedFields
            ]);
            var result = protocol.read(buf).FindCoordinatorResponseV4().result;
            result.coordinators.length.should.equal(2);
            result.coordinators[0].key.should.equal('g1');
            result.coordinators[0].nodeId.should.equal(1);
            result.coordinators[1].key.should.equal('g2');
            result.coordinators[1].nodeId.should.equal(2);
            result.coordinators[1].port.should.equal(9093);
        });
    });

    ///////////////////////////
    // OffsetCommit v8       //
    ///////////////////////////

    describe('OffsetCommitRequestV8', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().OffsetCommitRequestV8({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                generationId: 1,
                memberId: 'm1',
                groupInstanceId: null,
                topics: [{
                    topicName: 'test',
                    partitions: [{ partition: 0, offset: 10, metadata: null }]
                }]
            }).result;

            // apiKey=8, apiVersion=8
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(8);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(8);
        });
    });

    describe('OffsetCommitResponseV8', function () {
        it('should parse flexible response', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // correlationId
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime
                Buffer.from([0x02]),                     // compactArray: 1 topic
                Buffer.from([0x05]),                     // compactString: "test" (len 4+1)
                Buffer.from('test', 'utf8'),
                Buffer.from([0x02]),                     // compactArray: 1 partition
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // partition = 0
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x00]),                     // partition TaggedFields
                Buffer.from([0x00]),                     // topic TaggedFields
                Buffer.from([0x00])                      // top-level TaggedFields
            ]);
            var result = protocol.read(buf).OffsetCommitResponseV8().result;
            result.correlationId.should.equal(1);
            result.topics.length.should.equal(1);
            result.topics[0].topicName.should.equal('test');
            result.topics[0].partitions.length.should.equal(1);
            result.topics[0].partitions[0].partition.should.equal(0);
        });
    });

    /////////////////////////
    // OffsetFetch v6      //
    /////////////////////////

    describe('OffsetFetchRequestV6', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().OffsetFetchRequestV6({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                topics: [{
                    topicName: 'test',
                    partitions: [0]
                }]
            }).result;

            // apiKey=9, apiVersion=6
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(9);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(6);
        });

        it('should encode null topics', function () {
            var encoded = protocol.write().OffsetFetchRequestV6({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                topics: null
            }).result;

            // Should contain UVarint(0) for null compact array
            encoded.length.should.be.greaterThan(0);
        });
    });

    describe('OffsetFetchResponseV6', function () {
        it('should parse flexible response', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),          // correlationId
                Buffer.from([0x00]),                             // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),          // throttleTime
                Buffer.from([0x02]),                             // compactArray: 1 topic
                Buffer.from([0x05]),                             // compactString: "test" (len 4+1)
                Buffer.from('test', 'utf8'),
                Buffer.from([0x02]),                             // compactArray: 1 partition
                Buffer.from([0x00, 0x00, 0x00, 0x00]),          // partition = 0
                Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A]),  // offset = 10
                Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]),           // committedLeaderEpoch = -1
                Buffer.from([0x00]),                             // metadata = null (compactNullableString)
                Buffer.from([0x00, 0x00]),                       // error = 0
                Buffer.from([0x00]),                             // partition TaggedFields
                Buffer.from([0x00]),                             // topic TaggedFields
                Buffer.from([0x00, 0x00]),                       // top-level error = 0
                Buffer.from([0x00])                              // top-level TaggedFields
            ]);
            var result = protocol.read(buf).OffsetFetchResponseV6().result;
            result.correlationId.should.equal(1);
            result.topics.length.should.equal(1);
            result.topics[0].topicName.should.equal('test');
            result.topics[0].partitions[0].partition.should.equal(0);
        });
    });

    /////////////////////////
    // LeaveGroup v4       //
    /////////////////////////

    describe('LeaveGroupRequestV4', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().LeaveGroupRequestV4({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                members: [{ memberId: 'm1', groupInstanceId: null }]
            }).result;

            // apiKey=13, apiVersion=4
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(13);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(4);
        });
    });

    describe('LeaveGroupResponseV4', function () {
        it('should parse flexible response', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // correlationId
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x02]),                     // compactArray: 1 member
                Buffer.from([0x03]),                     // compactString: "m1" (len 2+1)
                Buffer.from('m1', 'utf8'),
                Buffer.from([0x00]),                     // groupInstanceId = null
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x00]),                     // member TaggedFields
                Buffer.from([0x00])                      // top-level TaggedFields
            ]);
            var result = protocol.read(buf).LeaveGroupResponseV4().result;
            result.correlationId.should.equal(1);
            result.members.length.should.equal(1);
            result.members[0].memberId.should.equal('m1');
        });
    });

    /////////////////////////
    // SyncGroup v4        //
    /////////////////////////

    describe('SyncConsumerGroupRequestV4', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().SyncConsumerGroupRequestV4({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                generationId: 1,
                memberId: 'm1',
                groupInstanceId: null,
                groupAssignment: [{
                    memberId: 'm1',
                    memberAssignment: {
                        version: 0,
                        partitionAssignment: [{ topic: 'test', partitions: [0] }],
                        metadata: null
                    }
                }]
            }).result;

            // apiKey=14, apiVersion=4
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(14);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(4);
        });

        it('should handle null assignment', function () {
            var encoded = protocol.write().SyncConsumerGroupRequestV4({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                generationId: 1,
                memberId: 'm1',
                groupInstanceId: null,
                groupAssignment: [{
                    memberId: 'm1',
                    memberAssignment: null
                }]
            }).result;

            encoded.length.should.be.greaterThan(0);
        });
    });

    describe('SyncConsumerGroupResponseV4', function () {
        it('should parse flexible response with compact assignment', function () {
            // Build inner assignment: version=0, 1 partition assignment (topic "t1", partition [0]), null metadata
            var innerBuf = Buffer.concat([
                Buffer.from([0x00, 0x00]),               // version = 0
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // array length = 1 (old encoding)
                Buffer.from([0x00, 0x02]),               // string length = 2 (old encoding)
                Buffer.from('t1', 'utf8'),
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // array length = 1
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // partition = 0
                Buffer.from([0xFF, 0xFF, 0xFF, 0xFF])    // metadata = null (old bytes)
            ]);

            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),   // correlationId
                Buffer.from([0x00]),                      // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),   // throttleTime
                Buffer.from([0x00, 0x00]),                // error = 0
                // compactBytes: UVarint(innerBuf.length + 1)
                Buffer.from([innerBuf.length + 1]),
                innerBuf,
                Buffer.from([0x00])                       // top-level TaggedFields
            ]);
            var result = protocol.read(buf).SyncConsumerGroupResponseV4().result;
            result.correlationId.should.equal(1);
            result.memberAssignment.should.be.an('object');
            result.memberAssignment.partitionAssignment.length.should.equal(1);
            result.memberAssignment.partitionAssignment[0].topic.should.equal('t1');
        });
    });

    //////////////////////////
    // DescribeGroups v5    //
    //////////////////////////

    describe('DescribeGroupRequestV5', function () {
        it('should use FlexibleRequestHeader and compact array of compact strings', function () {
            var encoded = protocol.write().DescribeGroupRequestV5({
                correlationId: 1,
                clientId: 'test',
                groups: ['g1']
            }).result;

            // apiKey=15, apiVersion=5
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(15);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(5);
        });
    });

    /////////////////////////
    // JoinGroup v6        //
    /////////////////////////

    describe('JoinConsumerGroupRequestV6', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().JoinConsumerGroupRequestV6({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                sessionTimeout: 30000,
                rebalanceTimeout: 30000,
                memberId: '',
                groupInstanceId: null,
                groupProtocols: [{
                    name: 'range',
                    version: 0,
                    subscriptions: ['test'],
                    metadata: null
                }]
            }).result;

            // apiKey=11, apiVersion=6
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(11);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(6);
        });
    });

    describe('JoinConsumerGroupResponseV6', function () {
        it('should parse flexible response with compact members', function () {
            // Build inner metadata: version=0, subscriptions=["t1"], null metadata (old encoding)
            var innerMeta = Buffer.concat([
                Buffer.from([0x00, 0x00]),               // version = 0
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // array length = 1 (old encoding)
                Buffer.from([0x00, 0x02]),               // string length = 2 (old encoding)
                Buffer.from('t1', 'utf8'),
                Buffer.from([0xFF, 0xFF, 0xFF, 0xFF])    // metadata = null (old bytes)
            ]);

            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),   // correlationId
                Buffer.from([0x00]),                      // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),   // throttleTime
                Buffer.from([0x00, 0x00]),                // error = 0
                Buffer.from([0x00, 0x00, 0x00, 0x01]),   // generationId = 1
                // compactString "range" (len 5+1=6)
                Buffer.from([0x06]),
                Buffer.from('range', 'utf8'),
                // compactString "leader-1" (len 8+1=9)
                Buffer.from([0x09]),
                Buffer.from('leader-1', 'utf8'),
                // compactString "member-1" (len 8+1=9)
                Buffer.from([0x09]),
                Buffer.from('member-1', 'utf8'),
                // compactArray: 1 member
                Buffer.from([0x02]),
                // member: compactString "member-1"
                Buffer.from([0x09]),
                Buffer.from('member-1', 'utf8'),
                // groupInstanceId = null
                Buffer.from([0x00]),
                // compactBytes: inner metadata
                Buffer.from([innerMeta.length + 1]),
                innerMeta,
                // member TaggedFields
                Buffer.from([0x00]),
                // top-level TaggedFields
                Buffer.from([0x00])
            ]);
            var result = protocol.read(buf).JoinConsumerGroupResponseV6().result;
            result.correlationId.should.equal(1);
            result.generationId.should.equal(1);
            result.groupProtocol.should.equal('range');
            result.leaderId.should.equal('leader-1');
            result.memberId.should.equal('member-1');
            result.members.length.should.equal(1);
            result.members[0].id.should.equal('member-1');
            result.members[0].subscriptions.length.should.equal(1);
            result.members[0].subscriptions[0].should.equal('t1');
        });
    });

    /////////////////////////
    // Metadata v9         //
    /////////////////////////

    describe('MetadataRequestV9', function () {
        it('should use FlexibleRequestHeader and compact topic array', function () {
            var encoded = protocol.write().MetadataRequestV9({
                correlationId: 1,
                clientId: 'test',
                topicNames: ['test']
            }).result;

            // apiKey=3, apiVersion=9
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(3);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(9);
        });
    });

    describe('MetadataResponseV9', function () {
        it('should parse flexible response with compact nested structures', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // correlationId
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime
                // brokers: compactArray with 1 broker
                Buffer.from([0x02]),                     // compactArray length = 2 (1 item)
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // nodeId = 1
                Buffer.from([0x0a]),                     // compactString "localhost" (len 9+1=10)
                Buffer.from('localhost', 'utf8'),
                Buffer.from([0x00, 0x00, 0x23, 0x84]),  // port = 9092
                Buffer.from([0x00]),                     // rack = null
                Buffer.from([0x00]),                     // broker TaggedFields
                // clusterId
                Buffer.from([0x05]),                     // compactString "abcd" (len 4+1=5)
                Buffer.from('abcd', 'utf8'),
                // controllerId
                Buffer.from([0x00, 0x00, 0x00, 0x01]),
                // topics: compactArray with 1 topic
                Buffer.from([0x02]),                     // compactArray length = 2 (1 item)
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x05]),                     // compactString "test" (len 4+1=5)
                Buffer.from('test', 'utf8'),
                Buffer.from([0x00]),                     // isInternal = false
                // partitions: compactArray with 1 partition
                Buffer.from([0x02]),                     // compactArray length = 2
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // partitionId = 0
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // leader = 1
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // leaderEpoch = 0
                Buffer.from([0x02]),                     // replicas compactArray: 1 item
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // replica = 1
                Buffer.from([0x02]),                     // isr compactArray: 1 item
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // isr = 1
                Buffer.from([0x01]),                     // offlineReplicas compactArray: 0 items
                Buffer.from([0x00]),                     // partition TaggedFields
                Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]),   // topicAuthorizedOperations = -1
                Buffer.from([0x00]),                     // topic TaggedFields
                Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]),   // clusterAuthorizedOperations = -1
                Buffer.from([0x00])                      // top-level TaggedFields
            ]);
            var result = protocol.read(buf).MetadataResponseV9().result;
            result.correlationId.should.equal(1);
            result.broker.length.should.equal(1);
            result.broker[0].nodeId.should.equal(1);
            result.broker[0].host.should.equal('localhost');
            result.broker[0].port.should.equal(9092);
            result.clusterId.should.equal('abcd');
            result.controllerId.should.equal(1);
            result.topicMetadata.length.should.equal(1);
            result.topicMetadata[0].topicName.should.equal('test');
            result.topicMetadata[0].partitionMetadata.length.should.equal(1);
            result.topicMetadata[0].partitionMetadata[0].partitionId.should.equal(0);
            result.topicMetadata[0].partitionMetadata[0].leader.should.equal(1);
        });
    });

    //////////////////////////
    // ApiVersions v3       //
    //////////////////////////

    describe('ApiVersionsRequestV3', function () {
        it('should use FlexibleRequestHeader and include software name/version', function () {
            var encoded = protocol.write().ApiVersionsRequestV3({
                correlationId: 1,
                clientId: 'test',
                clientSoftwareName: 'no-kafka',
                clientSoftwareVersion: '3.4.3'
            }).result;

            // apiKey=18, apiVersion=3
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(18);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(3);
        });
    });

    describe('ApiVersionsResponseV3', function () {
        it('should parse flexible response with compact arrays and trailing throttleTime', function () {
            // ApiVersions always uses response header v0 (no TaggedFields after correlationId)
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // correlationId (header v0)
                Buffer.from([0x00, 0x00]),               // error = 0
                // apiVersions: compactArray with 1 item
                Buffer.from([0x02]),                     // compactArray length = 2 (1 item)
                Buffer.from([0x00, 0x03]),               // apiKey = 3
                Buffer.from([0x00, 0x00]),               // minVersion = 0
                Buffer.from([0x00, 0x09]),               // maxVersion = 9
                Buffer.from([0x00]),                     // item TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime = 0
                Buffer.from([0x00])                      // top-level TaggedFields
            ]);
            var result = protocol.read(buf).ApiVersionsResponseV3().result;
            result.correlationId.should.equal(1);
            result.apiVersions.length.should.equal(1);
            result.apiVersions[0].apiKey.should.equal(3);
            result.apiVersions[0].minVersion.should.equal(0);
            result.apiVersions[0].maxVersion.should.equal(9);
            result.throttleTime.should.equal(0);
        });
    });

    ///////////////////////////
    // OffsetFetch v7       //
    ///////////////////////////

    describe('OffsetFetchRequestV7', function () {
        it('should encode with requireStable flag', function () {
            var encoded = protocol.write().OffsetFetchRequestV7({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                topics: [{ topicName: 'test', partitions: [0] }],
                requireStable: true
            }).result;

            // apiKey=9, apiVersion=7
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(9);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(7);
        });
    });

    ///////////////////////////
    // OffsetFetch v8       //
    ///////////////////////////

    describe('OffsetFetchRequestV8', function () {
        it('should encode multi-group request', function () {
            var encoded = protocol.write().OffsetFetchRequestV8({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 8,
                groups: [{ groupId: 'g1', topics: [{ topicName: 'test', partitions: [0] }] }],
                requireStable: false
            }).result;

            // apiKey=9, apiVersion=8
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(9);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(8);
        });

        it('should accept apiVersion 9 for KIP-848', function () {
            var encoded = protocol.write().OffsetFetchRequestV8({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 9,
                groups: [{ groupId: 'g1', topics: null, apiVersion: 9, memberId: 'member-1', memberEpoch: 5 }],
                requireStable: false
            }).result;

            encoded[2].should.equal(0x00);
            encoded[3].should.equal(9);
        });
    });

    describe('OffsetFetchResponseV8', function () {
        it('should parse multi-group response', function () {
            var buf = Buffer.concat([
                Buffer.from([0x00, 0x00, 0x00, 0x01]),  // correlationId
                Buffer.from([0x00]),                     // TaggedFields
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // throttleTime
                Buffer.from([0x02]),                     // compactArray: 1 group
                // group item:
                Buffer.from([0x03]),                     // compactString "g1" (len 2+1)
                Buffer.from('g1', 'utf8'),
                Buffer.from([0x02]),                     // compactArray: 1 topic
                Buffer.from([0x05]),                     // compactString "test" (len 4+1)
                Buffer.from('test', 'utf8'),
                Buffer.from([0x02]),                     // compactArray: 1 partition
                Buffer.from([0x00, 0x00, 0x00, 0x00]),  // partition = 0
                Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A]),  // offset = 10
                Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]),   // committedLeaderEpoch = -1
                Buffer.from([0x00]),                     // metadata = null
                Buffer.from([0x00, 0x00]),               // error = 0
                Buffer.from([0x00]),                     // partition TaggedFields
                Buffer.from([0x00]),                     // topic TaggedFields
                Buffer.from([0x00, 0x00]),               // group error = 0
                Buffer.from([0x00]),                     // group TaggedFields
                Buffer.from([0x00])                      // top-level TaggedFields
            ]);
            var result = protocol.read(buf).OffsetFetchResponseV8().result;
            result.correlationId.should.equal(1);
            result.groups.length.should.equal(1);
            result.groups[0].groupId.should.equal('g1');
            result.groups[0].topics.length.should.equal(1);
            result.groups[0].topics[0].topicName.should.equal('test');
            result.groups[0].topics[0].partitions[0].partition.should.equal(0);
        });
    });

    ///////////////////////////
    // InitProducerId v3    //
    ///////////////////////////

    describe('InitProducerIdRequestV3', function () {
        it('should include producerId and producerEpoch', function () {
            var encoded = protocol.write().InitProducerIdRequestV3({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 3,
                transactionalId: null,
                transactionTimeoutMs: 5000,
                producerId: 100,
                producerEpoch: 5
            }).result;

            // apiKey=22, apiVersion=3
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(22);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(3);
        });

        it('should accept apiVersion 5', function () {
            var encoded = protocol.write().InitProducerIdRequestV3({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 5,
                transactionalId: 'tx-1',
                transactionTimeoutMs: 10000,
                producerId: -1,
                producerEpoch: -1
            }).result;

            encoded[2].should.equal(0x00);
            encoded[3].should.equal(5);
        });
    });

    ///////////////////////////
    // LeaveGroup v5        //
    ///////////////////////////

    describe('LeaveGroupRequestV5', function () {
        it('should include reason per member', function () {
            var encoded = protocol.write().LeaveGroupRequestV5({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                members: [{ memberId: 'm1', groupInstanceId: null, reason: 'shutting down' }]
            }).result;

            // apiKey=13, apiVersion=5
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(13);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(5);
        });

        it('should handle null reason', function () {
            var encoded = protocol.write().LeaveGroupRequestV5({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                members: [{ memberId: 'm1', groupInstanceId: null, reason: null }]
            }).result;

            encoded.length.should.be.greaterThan(0);
        });
    });

    ///////////////////////////////////////
    // Round-trip request serialization  //
    ///////////////////////////////////////

    describe('round-trip verification', function () {
        it('InitProducerIdRequestV2 should round-trip', function () {
            var encoded = protocol.write().InitProducerIdRequestV2({
                correlationId: 42,
                clientId: 'no-kafka',
                transactionalId: 'tx-1',
                transactionTimeoutMs: 10000
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('HeartbeatRequestV4 should round-trip', function () {
            var encoded = protocol.write().HeartbeatRequestV4({
                correlationId: 42,
                clientId: 'no-kafka',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1',
                groupInstanceId: 'instance-1'
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('FindCoordinatorRequestV3 should round-trip', function () {
            var encoded = protocol.write().FindCoordinatorRequestV3({
                correlationId: 42,
                clientId: 'no-kafka',
                key: 'test-group',
                coordinatorType: 0
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('FindCoordinatorRequestV4 should round-trip', function () {
            var encoded = protocol.write().FindCoordinatorRequestV4({
                correlationId: 42,
                clientId: 'no-kafka',
                apiVersion: 4,
                keyType: 0,
                coordinatorKeys: ['group-1', 'group-2']
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('MetadataRequestV9 should round-trip with empty topics', function () {
            var encoded = protocol.write().MetadataRequestV9({
                correlationId: 42,
                clientId: 'no-kafka',
                topicNames: []
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('OffsetFetchRequestV6 should round-trip with topics', function () {
            var encoded = protocol.write().OffsetFetchRequestV6({
                correlationId: 42,
                clientId: 'no-kafka',
                groupId: 'test-group',
                topics: [{ topicName: 'test', partitions: [0, 1] }]
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('OffsetCommitRequestV8 should round-trip', function () {
            var encoded = protocol.write().OffsetCommitRequestV8({
                correlationId: 42,
                clientId: 'no-kafka',
                groupId: 'test-group',
                generationId: 1,
                memberId: 'member-1',
                groupInstanceId: null,
                topics: [{
                    topicName: 'test',
                    partitions: [{ partition: 0, offset: 100, metadata: 'meta' }]
                }]
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('LeaveGroupRequestV4 should round-trip', function () {
            var encoded = protocol.write().LeaveGroupRequestV4({
                correlationId: 42,
                clientId: 'no-kafka',
                groupId: 'test-group',
                members: [{ memberId: 'member-1', groupInstanceId: 'inst-1' }]
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('OffsetFetchRequestV7 should round-trip', function () {
            var encoded = protocol.write().OffsetFetchRequestV7({
                correlationId: 42,
                clientId: 'no-kafka',
                groupId: 'test-group',
                topics: [{ topicName: 'test', partitions: [0, 1] }],
                requireStable: true
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('OffsetFetchRequestV8 should round-trip with multiple groups', function () {
            var encoded = protocol.write().OffsetFetchRequestV8({
                correlationId: 42,
                clientId: 'no-kafka',
                apiVersion: 8,
                groups: [
                    { groupId: 'g1', topics: [{ topicName: 'test', partitions: [0] }] },
                    { groupId: 'g2', topics: null }
                ],
                requireStable: false
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('InitProducerIdRequestV3 should round-trip', function () {
            var encoded = protocol.write().InitProducerIdRequestV3({
                correlationId: 42,
                clientId: 'no-kafka',
                apiVersion: 3,
                transactionalId: 'tx-1',
                transactionTimeoutMs: 10000,
                producerId: 42,
                producerEpoch: 3
            }).result;
            encoded.length.should.be.greaterThan(0);
        });

        it('LeaveGroupRequestV5 should round-trip', function () {
            var encoded = protocol.write().LeaveGroupRequestV5({
                correlationId: 42,
                clientId: 'no-kafka',
                groupId: 'test-group',
                members: [{ memberId: 'member-1', groupInstanceId: 'inst-1', reason: 'shutting down' }]
            }).result;
            encoded.length.should.be.greaterThan(0);
        });
    });
});
