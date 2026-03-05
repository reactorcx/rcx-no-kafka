'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');

describe('Kafka 4.1.1 Protocol Version Tests', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    ////////////////////////////////////////
    // Phase 1: Group Membership APIs     //
    ////////////////////////////////////////

    describe('JoinGroup v7', function () {
        it('should encode JoinConsumerGroupRequestV7 with apiVersion=7', function () {
            var encoded = protocol.write().JoinConsumerGroupRequestV7({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                sessionTimeout: 10000,
                rebalanceTimeout: 30000,
                memberId: '',
                groupProtocols: [{
                    name: 'roundrobin',
                    version: 0,
                    subscriptions: ['t1'],
                    metadata: null
                }]
            }).result;

            // apiKey=11, apiVersion=7
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(11);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(7);
        });

        it('should parse JoinConsumerGroupResponseV7 with protocolType', function () {
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
            // generationId
            writer.Int32BE(5);
            // protocolType (NEW in v7 — compactNullableString between generationId and groupProtocol)
            writer.compactNullableString('consumer');
            // groupProtocol
            writer.compactString('roundrobin');
            // leaderId
            writer.compactString('leader-1');
            // memberId
            writer.compactString('member-1');
            // members: empty
            writer.compactArray([], function () {});
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).JoinConsumerGroupResponseV7().result;
            result.correlationId.should.equal(1);
            result.generationId.should.equal(5);
            result.protocolType.should.equal('consumer');
            result.groupProtocol.should.equal('roundrobin');
            result.leaderId.should.equal('leader-1');
            result.memberId.should.equal('member-1');
        });
    });

    describe('JoinGroup v8', function () {
        it('should encode JoinConsumerGroupRequestV8 with reason field', function () {
            var v7Encoded, v8Encoded;

            v7Encoded = protocol.write().JoinConsumerGroupRequestV7({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                sessionTimeout: 10000,
                rebalanceTimeout: 30000,
                memberId: '',
                groupProtocols: [{
                    name: 'roundrobin',
                    version: 0,
                    subscriptions: ['t1'],
                    metadata: null
                }]
            }).result;

            v8Encoded = protocol.write().JoinConsumerGroupRequestV8({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                sessionTimeout: 10000,
                rebalanceTimeout: 30000,
                memberId: '',
                reason: 'initial rebalance',
                groupProtocols: [{
                    name: 'roundrobin',
                    version: 0,
                    subscriptions: ['t1'],
                    metadata: null
                }]
            }).result;

            // apiKey=11, apiVersion=8
            v8Encoded[0].should.equal(0x00);
            v8Encoded[1].should.equal(11);
            v8Encoded[2].should.equal(0x00);
            v8Encoded[3].should.equal(8);

            // v8 buffer should be longer than v7 due to reason field
            v8Encoded.length.should.be.above(v7Encoded.length);
        });
    });

    describe('JoinGroup v9', function () {
        it('should encode JoinConsumerGroupRequestV9 with apiVersion=9', function () {
            var encoded = protocol.write().JoinConsumerGroupRequestV9({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                sessionTimeout: 10000,
                rebalanceTimeout: 30000,
                memberId: '',
                reason: null,
                groupProtocols: [{
                    name: 'roundrobin',
                    version: 0,
                    subscriptions: ['t1'],
                    metadata: null
                }]
            }).result;

            // apiKey=11, apiVersion=9
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(11);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(9);
        });

        it('should parse JoinConsumerGroupResponseV9 with skipAssignment', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(2);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // error
            writer.Int16BE(0);
            // generationId
            writer.Int32BE(10);
            // protocolType
            writer.compactNullableString('consumer');
            // groupProtocol
            writer.compactString('roundrobin');
            // leaderId
            writer.compactString('leader-1');
            // skipAssignment (NEW in v9 — Int8 between leaderId and memberId)
            writer.Int8(1);
            // memberId
            writer.compactString('member-1');
            // members: empty
            writer.compactArray([], function () {});
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).JoinConsumerGroupResponseV9().result;
            result.correlationId.should.equal(2);
            result.generationId.should.equal(10);
            result.protocolType.should.equal('consumer');
            result.skipAssignment.should.equal(1);
            result.memberId.should.equal('member-1');
        });
    });

    describe('SyncGroup v5', function () {
        it('should encode SyncConsumerGroupRequestV5 with protocolType and protocolName', function () {
            var v4Encoded, v5Encoded;

            v4Encoded = protocol.write().SyncConsumerGroupRequestV4({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                generationId: 1,
                memberId: 'member-1',
                groupAssignment: []
            }).result;

            v5Encoded = protocol.write().SyncConsumerGroupRequestV5({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                generationId: 1,
                memberId: 'member-1',
                protocolType: 'consumer',
                protocolName: 'roundrobin',
                groupAssignment: []
            }).result;

            // apiKey=14, apiVersion=5
            v5Encoded[0].should.equal(0x00);
            v5Encoded[1].should.equal(14);
            v5Encoded[2].should.equal(0x00);
            v5Encoded[3].should.equal(5);

            // v5 should be longer due to protocolType and protocolName fields
            v5Encoded.length.should.be.above(v4Encoded.length);
        });

        it('should parse SyncConsumerGroupResponseV5 with protocolType and protocolName', function () {
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
            // protocolType (NEW in v5)
            writer.compactNullableString('consumer');
            // protocolName (NEW in v5)
            writer.compactNullableString('roundrobin');
            // memberAssignment: null (UVarint(0) = compact bytes length 0)
            writer.UVarint(0);
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).SyncConsumerGroupResponseV5().result;
            result.correlationId.should.equal(1);
            result.protocolType.should.equal('consumer');
            result.protocolName.should.equal('roundrobin');
            (result.memberAssignment === null).should.equal(true);
        });
    });

    ////////////////////////////////////////
    // Phase 2: Admin APIs                //
    ////////////////////////////////////////

    describe('DescribeGroups v6', function () {
        it('should encode DescribeGroupRequestV6 with apiVersion=6', function () {
            var encoded = protocol.write().DescribeGroupRequestV6({
                correlationId: 1,
                clientId: 'test',
                groups: ['g1']
            }).result;

            // apiKey=15, apiVersion=6
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(15);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(6);
        });

        it('should parse DescribeGroupResponseV6 with errorMessage', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // groups: 1 group
            writer.compactArray([1], function () {
                // error
                this.Int16BE(0);
                // errorMessage (NEW in v6 — compactNullableString between errorCode and groupId)
                this.compactNullableString(null);
                // groupId
                this.compactString('g1');
                // state
                this.compactString('Stable');
                // protocolType
                this.compactString('consumer');
                // protocol
                this.compactString('roundrobin');
                // members: empty (consumer branch)
                this.compactArray([], function () {});
                // authorizedOperations
                this.Int32BE(-2147483648);
                // TaggedFields
                this.TaggedFields();
            });
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).DescribeGroupResponseV6().result;
            result.correlationId.should.equal(1);
            result.groups.length.should.equal(1);
            (result.groups[0].errorMessage === null).should.equal(true);
            result.groups[0].groupId.should.equal('g1');
            result.groups[0].state.should.equal('Stable');
        });
    });

    describe('ListGroups v4', function () {
        it('should encode ListGroupsRequestV4 with statesFilter', function () {
            var encoded = protocol.write().ListGroupsRequestV4({
                correlationId: 1,
                clientId: 'test',
                statesFilter: ['Stable']
            }).result;

            // apiKey=16, apiVersion=4
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(16);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(4);
        });

        it('should parse ListGroupResponseV4 with groupState', function () {
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
            // groups: 1 group
            writer.compactArray([1], function () {
                this.compactString('g1');
                this.compactString('consumer');
                // groupState (NEW in v4)
                this.compactString('Stable');
                this.TaggedFields();
            });
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).ListGroupResponseV4().result;
            result.correlationId.should.equal(1);
            result.groups.length.should.equal(1);
            result.groups[0].groupId.should.equal('g1');
            result.groups[0].groupState.should.equal('Stable');
        });
    });

    describe('ListGroups v5', function () {
        it('should encode ListGroupsRequestV5 with statesFilter and typesFilter', function () {
            var encoded = protocol.write().ListGroupsRequestV5({
                correlationId: 1,
                clientId: 'test',
                statesFilter: ['Stable'],
                typesFilter: ['consumer']
            }).result;

            // apiKey=16, apiVersion=5
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(16);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(5);
        });

        it('should parse ListGroupResponseV5 with groupState and groupType', function () {
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
            // groups: 1 group
            writer.compactArray([1], function () {
                this.compactString('g1');
                this.compactString('consumer');
                this.compactString('Stable');
                // groupType (NEW in v5)
                this.compactString('consumer');
                this.TaggedFields();
            });
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).ListGroupResponseV5().result;
            result.correlationId.should.equal(1);
            result.groups.length.should.equal(1);
            result.groups[0].groupId.should.equal('g1');
            result.groups[0].groupState.should.equal('Stable');
            result.groups[0].groupType.should.equal('consumer');
        });
    });

    ////////////////////////////////////////
    // Phase 3: Transaction APIs          //
    ////////////////////////////////////////

    describe('EndTxn v4', function () {
        it('should encode EndTxnRequestV4 with apiVersion=4', function () {
            var encoded = protocol.write().EndTxnRequestV4({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 4,
                transactionalId: 'txn1',
                producerId: 1,
                producerEpoch: 0,
                committed: true
            }).result;

            // apiKey=26, apiVersion=4
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(26);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(4);
        });
    });

    describe('EndTxn v5', function () {
        it('should encode EndTxnRequestV4 with apiVersion=5', function () {
            var encoded = protocol.write().EndTxnRequestV4({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 5,
                transactionalId: 'txn1',
                producerId: 1,
                producerEpoch: 0,
                committed: true
            }).result;

            // apiKey=26, apiVersion=5
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(26);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(5);
        });

        it('should parse EndTxnResponseV5 with producerId and producerEpoch', function () {
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
            // producerId (NEW in v5)
            writer.Int64BE(42);
            // producerEpoch (NEW in v5)
            writer.Int16BE(3);
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).EndTxnResponseV5().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
            result.producerId.toNumber().should.equal(42);
            result.producerEpoch.should.equal(3);
        });
    });

    describe('AddOffsetsToTxn v4', function () {
        it('should encode AddOffsetsToTxnRequestV4 with apiVersion=4', function () {
            var encoded = protocol.write().AddOffsetsToTxnRequestV4({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'txn1',
                producerId: 1,
                producerEpoch: 0,
                groupId: 'g1'
            }).result;

            // apiKey=25, apiVersion=4
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(25);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(4);
        });
    });

    describe('TxnOffsetCommit v4', function () {
        it('should encode TxnOffsetCommitRequestV4 with apiVersion=4', function () {
            var encoded = protocol.write().TxnOffsetCommitRequestV4({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 4,
                transactionalId: 'txn1',
                groupId: 'g1',
                producerId: 1,
                producerEpoch: 0,
                topics: [{
                    topic: 't1',
                    partitions: [{ partition: 0, offset: 100, metadata: null }]
                }]
            }).result;

            // apiKey=28, apiVersion=4
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(28);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(4);
        });
    });

    describe('TxnOffsetCommit v5', function () {
        it('should encode TxnOffsetCommitRequestV4 with apiVersion=5', function () {
            var encoded = protocol.write().TxnOffsetCommitRequestV4({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 5,
                transactionalId: 'txn1',
                groupId: 'g1',
                producerId: 1,
                producerEpoch: 0,
                topics: [{
                    topic: 't1',
                    partitions: [{ partition: 0, offset: 100, metadata: null }]
                }]
            }).result;

            // apiKey=28, apiVersion=5
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(28);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(5);
        });
    });

    describe('InitProducerId v6', function () {
        it('should encode InitProducerIdRequestV6 with enable2Pc and keepPreparedTxn', function () {
            var v5Encoded, v6Encoded;

            v5Encoded = protocol.write().InitProducerIdRequestV3({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 5,
                transactionalId: 'txn1',
                transactionTimeoutMs: 5000,
                producerId: -1,
                producerEpoch: -1
            }).result;

            v6Encoded = protocol.write().InitProducerIdRequestV6({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'txn1',
                transactionTimeoutMs: 5000,
                producerId: -1,
                producerEpoch: -1,
                enable2Pc: false,
                keepPreparedTxn: false
            }).result;

            // apiKey=22, apiVersion=6
            v6Encoded[0].should.equal(0x00);
            v6Encoded[1].should.equal(22);
            v6Encoded[2].should.equal(0x00);
            v6Encoded[3].should.equal(6);

            // v6 should be longer due to enable2Pc + keepPreparedTxn (2 extra Int8 bytes)
            v6Encoded.length.should.be.above(v5Encoded.length);
        });
    });

    ////////////////////////////////////////
    // Phase 4: Core APIs                 //
    ////////////////////////////////////////

    describe('Produce v12', function () {
        it('should encode ProduceRequestV12 with apiVersion=12', function () {
            var encoded = protocol.write().ProduceRequestV12({
                correlationId: 1,
                clientId: 'test',
                transactionalId: null,
                requiredAcks: -1,
                timeout: 30000,
                topics: [{
                    topicName: 'my-topic',
                    partitions: [{
                        partition: 0,
                        batch: {
                            baseOffset: 0,
                            records: [{
                                key: null,
                                value: Buffer.from('hello', 'utf8')
                            }],
                            codec: 0
                        }
                    }]
                }]
            }).result;

            // apiKey=0, apiVersion=12
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(0);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(12);
        });
    });

    describe('Produce v13', function () {
        it('should encode ProduceRequestV13 with topicId uuid', function () {
            var topicId = '550e8400-e29b-41d4-a716-446655440000';
            var encoded = protocol.write().ProduceRequestV13({
                correlationId: 1,
                clientId: 'test',
                transactionalId: null,
                requiredAcks: -1,
                timeout: 30000,
                topics: [{
                    topicId: topicId,
                    partitions: [{
                        partition: 0,
                        batch: {
                            baseOffset: 0,
                            records: [{
                                key: null,
                                value: Buffer.from('hello', 'utf8')
                            }],
                            codec: 0
                        }
                    }]
                }]
            }).result;

            // apiKey=0, apiVersion=13
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(0);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(13);
        });

        it('should parse ProduceResponseV13 with topicId uuid', function () {
            var topicId = '550e8400-e29b-41d4-a716-446655440000';
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // response header TaggedFields
            writer.TaggedFields();
            // topics: 1 topic
            writer.compactArray([1], function () {
                // topicId (uuid — replaces topicName in v13)
                this.uuid(topicId);
                // partitions: 1
                this.compactArray([1], function () {
                    this.Int32BE(0); // partition
                    this.Int16BE(0); // error
                    this.Int64BE(0); // offset
                    this.Int64BE(-1); // timestamp
                    this.Int64BE(0); // logStartOffset
                    // recordErrors: empty
                    this.compactArray([], function () {});
                    // errorMessage: null
                    this.compactNullableString(null);
                    // partition-level tagged fields (0 tags)
                    this.UVarint(0);
                });
                this.TaggedFields();
            });
            // throttleTime
            writer.Int32BE(0);
            // body-level tagged fields (0 tags)
            writer.UVarint(0);

            result = protocol.read(writer.result).ProduceResponseV13().result;
            result.correlationId.should.equal(1);
            result.topics.length.should.equal(1);
            result.topics[0].topicId.should.equal(topicId);
            result.topics[0].partitions.length.should.equal(1);
            result.topics[0].partitions[0].partition.should.equal(0);
        });
    });

    describe('Metadata v13', function () {
        it('should encode MetadataRequestV13 with apiVersion=13', function () {
            var encoded = protocol.write().MetadataRequestV13({
                correlationId: 1,
                clientId: 'test',
                topicNames: [{ topicId: null, name: 'my-topic' }]
            }).result;

            // apiKey=3, apiVersion=13
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(3);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(13);
        });
    });

    describe('ListOffsets v10', function () {
        it('should encode OffsetRequestV10 with timeoutMs', function () {
            var v9Encoded, v10Encoded;

            v9Encoded = protocol.write().OffsetRequestV9({
                correlationId: 1,
                clientId: 'test',
                isolationLevel: 0,
                topics: [{
                    topicName: 'my-topic',
                    partitions: [{ partition: 0, time: -1 }]
                }]
            }).result;

            v10Encoded = protocol.write().OffsetRequestV10({
                correlationId: 1,
                clientId: 'test',
                isolationLevel: 0,
                topics: [{
                    topicName: 'my-topic',
                    partitions: [{ partition: 0, time: -1 }]
                }],
                timeoutMs: 5000
            }).result;

            // apiKey=2, apiVersion=10
            v10Encoded[0].should.equal(0x00);
            v10Encoded[1].should.equal(2);
            v10Encoded[2].should.equal(0x00);
            v10Encoded[3].should.equal(10);

            // v10 should be longer due to timeoutMs (Int32BE = 4 bytes)
            v10Encoded.length.should.be.above(v9Encoded.length);
        });
    });

    describe('OffsetCommit v9', function () {
        it('should encode OffsetCommitRequestV9 with apiVersion=9', function () {
            var encoded = protocol.write().OffsetCommitRequestV9({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                generationId: 1,
                memberId: 'member-1',
                topics: [{
                    topicName: 't1',
                    partitions: [{ partition: 0, offset: 100, metadata: null }]
                }]
            }).result;

            // apiKey=8, apiVersion=9
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(8);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(9);
        });
    });

    describe('OffsetCommit v10', function () {
        it('should encode OffsetCommitRequestV10 with topicId uuid', function () {
            var topicId = '550e8400-e29b-41d4-a716-446655440000';
            var encoded = protocol.write().OffsetCommitRequestV10({
                correlationId: 1,
                clientId: 'test',
                groupId: 'g1',
                generationId: 1,
                memberId: 'member-1',
                topics: [{
                    topicId: topicId,
                    partitions: [{ partition: 0, offset: 100, metadata: null }]
                }]
            }).result;

            // apiKey=8, apiVersion=10
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(8);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(10);
        });

        it('should parse OffsetCommitResponseV10 with topicId uuid', function () {
            var topicId = '550e8400-e29b-41d4-a716-446655440000';
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // topics: 1 topic
            writer.compactArray([1], function () {
                // topicId (uuid — replaces topicName in v10)
                this.uuid(topicId);
                // partitions: 1
                this.compactArray([1], function () {
                    this.Int32BE(0); // partition
                    this.Int16BE(0); // error
                    this.TaggedFields();
                });
                this.TaggedFields();
            });
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).OffsetCommitResponseV10().result;
            result.correlationId.should.equal(1);
            result.topics.length.should.equal(1);
            result.topics[0].topicId.should.equal(topicId);
            result.topics[0].partitions.length.should.equal(1);
            result.topics[0].partitions[0].partition.should.equal(0);
        });
    });

    describe('OffsetFetch v10', function () {
        it('should encode OffsetFetchRequestV10 with topicId uuid', function () {
            var topicId = '550e8400-e29b-41d4-a716-446655440000';
            var encoded = protocol.write().OffsetFetchRequestV10({
                correlationId: 1,
                clientId: 'test',
                groups: [{
                    groupId: 'g1',
                    topics: [{
                        topicId: topicId,
                        partitions: [0]
                    }]
                }],
                requireStable: false
            }).result;

            // apiKey=9, apiVersion=10
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(9);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(10);
        });

        it('should parse OffsetFetchResponseV10 with topicId uuid', function () {
            var topicId = '550e8400-e29b-41d4-a716-446655440000';
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // groups: 1 group
            writer.compactArray([1], function () {
                // groupId
                this.compactString('g1');
                // topics: 1 topic
                this.compactArray([1], function () {
                    // topicId (uuid — replaces topicName in v10)
                    this.uuid(topicId);
                    // partitions: 1
                    this.compactArray([1], function () {
                        this.Int32BE(0); // partition
                        this.Int64BE(100); // offset
                        this.Int32BE(-1); // committedLeaderEpoch
                        this.compactNullableString(null); // metadata
                        this.Int16BE(0); // error
                        this.TaggedFields();
                    });
                    this.TaggedFields();
                });
                // error
                this.Int16BE(0);
                this.TaggedFields();
            });
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).OffsetFetchResponseV10().result;
            result.correlationId.should.equal(1);
            result.groups.length.should.equal(1);
            result.groups[0].groupId.should.equal('g1');
            result.groups[0].topics.length.should.equal(1);
            result.groups[0].topics[0].topicId.should.equal(topicId);
            result.groups[0].topics[0].partitions[0].partition.should.equal(0);
            result.groups[0].topics[0].partitions[0].offset.should.equal(100);
        });
    });

    ////////////////////////////////////////////////////////
    // Phase 5: Response Parser Gap Fill                //
    ////////////////////////////////////////////////////////

    describe('MetadataResponse v13', function () {
        it('should parse MetadataResponseV13 with topicId and topicName', function () {
            var topicId = '550e8400-e29b-41d4-a716-446655440000';
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // brokers: empty
            writer.compactArray([], function () {});
            // clusterId
            writer.compactString('test-cluster');
            // controllerId
            writer.Int32BE(1);
            // topicMetadata: 1 topic (TopicMetadataV12)
            writer.compactArray([1], function () {
                // error (ErrorCode = Int16BE)
                this.Int16BE(0);
                // topicName (compactNullableString)
                this.compactNullableString('my-topic');
                // topicId (uuid)
                this.uuid(topicId);
                // isInternal
                this.Int8(0);
                // partitionMetadata: empty (PartitionMetadataV9)
                this.compactArray([], function () {});
                // topicAuthorizedOperations
                this.Int32BE(-2147483648);
                // TaggedFields
                this.TaggedFields();
            });
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).MetadataResponseV13().result;
            result.correlationId.should.equal(1);
            result.clusterId.should.equal('test-cluster');
            result.controllerId.should.equal(1);
            result.topicMetadata.length.should.equal(1);
            result.topicMetadata[0].topicId.should.equal(topicId);
            result.topicMetadata[0].topicName.should.equal('my-topic');
        });
    });

    describe('FetchResponse v3', function () {
        it('should parse FetchResponseV3 with empty topics array', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // throttleTime
            writer.Int32BE(50);
            // topics: empty array (Int32BE count = 0)
            writer.array([], function () {});

            result = protocol.read(writer.result).FetchResponseV3().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(50);
            result.topics.length.should.equal(0);
        });
    });

    describe('DescribeGroupResponse v4', function () {
        it('should parse DescribeGroupResponseV4 with authorizedOperations', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // throttleTime
            writer.Int32BE(0);
            // groups: 1 group (non-flexible array)
            writer.array([1], function () {
                // error (ErrorCode = Int16BE)
                this.Int16BE(0);
                // groupId
                this.string('g1');
                // state
                this.string('Stable');
                // protocolType
                this.string('consumer');
                // protocol
                this.string('roundrobin');
                // members: empty array (consumer branch)
                this.array([], function () {});
                // authorizedOperations
                this.Int32BE(255);
            });

            result = protocol.read(writer.result).DescribeGroupResponseV4().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
            result.groups.length.should.equal(1);
            result.groups[0].groupId.should.equal('g1');
            result.groups[0].state.should.equal('Stable');
            result.groups[0].authorizedOperations.should.equal(255);
        });
    });

    describe('DescribeGroupResponse v5', function () {
        it('should parse DescribeGroupResponseV5 with compact encoding', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // groups: 1 group (compactArray)
            writer.compactArray([1], function () {
                // error (ErrorCode = Int16BE)
                this.Int16BE(0);
                // groupId
                this.compactString('g1');
                // state
                this.compactString('Stable');
                // protocolType
                this.compactString('consumer');
                // protocol
                this.compactString('roundrobin');
                // members: empty compactArray (consumer branch)
                this.compactArray([], function () {});
                // authorizedOperations
                this.Int32BE(255);
                // TaggedFields
                this.TaggedFields();
            });
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).DescribeGroupResponseV5().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
            result.groups.length.should.equal(1);
            result.groups[0].groupId.should.equal('g1');
            result.groups[0].state.should.equal('Stable');
            result.groups[0].authorizedOperations.should.equal(255);
        });
    });

    describe('OffsetFetchResponse v3', function () {
        it('should parse OffsetFetchResponseV3 with throttleTime and top-level error', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // throttleTime
            writer.Int32BE(10);
            // topics: 1 topic (non-flexible array)
            writer.array([1], function () {
                // topicName
                this.string('t1');
                // partitions: 1 partition
                this.array([1], function () {
                    // partition
                    this.Int32BE(0);
                    // offset (KafkaOffset = Int64BE)
                    this.Int64BE(42);
                    // metadata
                    this.string('');
                    // error (ErrorCode = Int16BE)
                    this.Int16BE(0);
                });
            });
            // top-level error (ErrorCode = Int16BE)
            writer.Int16BE(0);

            result = protocol.read(writer.result).OffsetFetchResponseV3().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(10);
            result.topics.length.should.equal(1);
            result.topics[0].topicName.should.equal('t1');
            result.topics[0].partitions.length.should.equal(1);
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].offset.should.equal(42);
        });
    });

    describe('OffsetResponse v2', function () {
        it('should parse OffsetResponseV2 with throttleTime and offset', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(1);
            // throttleTime
            writer.Int32BE(5);
            // topics: 1 topic (non-flexible array, uses OffsetResponseV1TopicItem)
            writer.array([1], function () {
                // topicName
                this.string('t1');
                // partitions: 1 partition (OffsetResponseV1PartitionItem)
                this.array([1], function () {
                    // partition
                    this.Int32BE(0);
                    // error (ErrorCode = Int16BE)
                    this.Int16BE(0);
                    // timestamp (KafkaOffset = Int64BE)
                    this.Int64BE(-1);
                    // offset (KafkaOffset = Int64BE)
                    this.Int64BE(100);
                });
            });

            result = protocol.read(writer.result).OffsetResponseV2().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(5);
            result.topics.length.should.equal(1);
            result.topics[0].topicName.should.equal('t1');
            result.topics[0].partitions.length.should.equal(1);
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].offset.should.equal(100);
        });
    });

    describe('ApiVersions v4', function () {
        it('should encode ApiVersionsRequestV4 with apiVersion=4', function () {
            var encoded = protocol.write().ApiVersionsRequestV4({
                correlationId: 1,
                clientId: 'test',
                clientSoftwareName: 'no-kafka',
                clientSoftwareVersion: '4.1.0'
            }).result;

            // apiKey=18, apiVersion=4
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(18);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(4);
        });
    });
});
