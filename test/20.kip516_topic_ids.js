'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');

describe('KIP-516 Topic IDs', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    //////////
    // UUID //
    //////////

    describe('uuid type', function () {
        it('should round-trip a standard UUID string', function () {
            var uuid = '550e8400-e29b-41d4-a716-446655440000';
            var encoded = protocol.write().uuid(uuid).result;
            var decoded;
            encoded.length.should.equal(16);
            decoded = protocol.read(encoded).uuid('value').result;
            decoded.value.should.equal(uuid);
        });

        it('should write null as 16 zero bytes and read back as null', function () {
            var encoded = protocol.write().uuid(null).result;
            var decoded, i;
            encoded.length.should.equal(16);
            for (i = 0; i < 16; i++) {
                encoded[i].should.equal(0);
            }
            decoded = protocol.read(encoded).uuid('value').result;
            should.equal(decoded.value, null);
        });

        it('should write undefined as 16 zero bytes and read back as null', function () {
            var encoded = protocol.write().uuid(undefined).result;
            var decoded = protocol.read(encoded).uuid('value').result;
            should.equal(decoded.value, null);
        });

        it('should round-trip various UUID values', function () {
            var uuid = 'ffffffff-ffff-ffff-ffff-ffffffffffff';
            var encoded = protocol.write().uuid(uuid).result;
            var decoded = protocol.read(encoded).uuid('value').result;
            decoded.value.should.equal(uuid);
        });
    });

    /////////////////////
    // Metadata v10    //
    /////////////////////

    describe('Metadata v10', function () {
        it('should encode MetadataRequestV10 with topic items', function () {
            var encoded = protocol.write().MetadataRequestV10({
                correlationId: 1,
                clientId: 'test',
                topicNames: [{ topicId: null, name: 'my-topic' }]
            }).result;
            encoded.length.should.be.above(0);
        });

        it('should encode MetadataRequestV10 with empty topics (all topics)', function () {
            var encoded = protocol.write().MetadataRequestV10({
                correlationId: 2,
                clientId: 'test',
                topicNames: []
            }).result;
            encoded.length.should.be.above(0);
        });

        it('should decode MetadataResponseV10 with topicId', function () {
            var topicId = '550e8400-e29b-41d4-a716-446655440000';
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(42);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // brokers: empty compact array
            writer.compactArray([], function () {});
            // clusterId
            writer.compactString('test-cluster');
            // controllerId
            writer.Int32BE(1);
            // topicMetadata: 1 topic
            writer.compactArray([1], function () {
                // error code
                this.Int16BE(0);
                // topicName
                this.compactString('my-topic');
                // topicId (uuid)
                this.uuid(topicId);
                // isInternal
                this.Int8(0);
                // partitions: empty
                this.compactArray([], function () {});
                // topicAuthorizedOperations
                this.Int32BE(-2147483648);
                // TaggedFields
                this.TaggedFields();
            });
            // clusterAuthorizedOperations
            writer.Int32BE(-2147483648);
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).MetadataResponseV10().result;
            result.correlationId.should.equal(42);
            result.topicMetadata.length.should.equal(1);
            result.topicMetadata[0].topicName.should.equal('my-topic');
            result.topicMetadata[0].topicId.should.equal(topicId);
        });
    });

    ////////////////////
    // ListOffsets v6 //
    ////////////////////

    describe('ListOffsets v6', function () {
        it('should encode OffsetRequestV6', function () {
            var encoded = protocol.write().OffsetRequestV6({
                correlationId: 10,
                clientId: 'test',
                isolationLevel: 0,
                topics: [{
                    topicName: 'my-topic',
                    partitions: [{ partition: 0, time: -1 }]
                }]
            }).result;
            encoded.length.should.be.above(0);
        });

        it('should decode OffsetResponseV6', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(10);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // topics: 1 topic
            writer.compactArray([1], function () {
                this.compactString('my-topic');
                this.compactArray([1], function () {
                    this.Int32BE(0); // partition
                    this.Int16BE(0); // error
                    this.Int64BE(-1); // timestamp
                    this.Int64BE(42); // offset
                    this.Int32BE(-1); // leaderEpoch
                    this.TaggedFields();
                });
                this.TaggedFields();
            });
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).OffsetResponseV6().result;
            result.correlationId.should.equal(10);
            result.topics.length.should.equal(1);
            result.topics[0].topicName.should.equal('my-topic');
            result.topics[0].partitions.length.should.equal(1);
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].offset.should.equal(42);
        });
    });

    //////////////////
    // Produce v9   //
    //////////////////

    describe('Produce v9', function () {
        it('should encode ProduceRequestV9', function () {
            var encoded = protocol.write().ProduceRequestV9({
                correlationId: 20,
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
                                value: new Buffer('hello', 'utf8')
                            }],
                            codec: 0
                        }
                    }]
                }]
            }).result;
            encoded.length.should.be.above(0);
        });

        it('should decode ProduceResponseV9', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(20);
            // response header TaggedFields
            writer.TaggedFields();
            // topics: 1 topic
            writer.compactArray([1], function () {
                this.compactString('my-topic');
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
                    this.TaggedFields();
                });
                this.TaggedFields();
            });
            // throttleTime
            writer.Int32BE(0);
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).ProduceResponseV9().result;
            result.correlationId.should.equal(20);
            result.topics.length.should.equal(1);
            result.topics[0].topicName.should.equal('my-topic');
            result.topics[0].partitions.length.should.equal(1);
            result.topics[0].partitions[0].partition.should.equal(0);
        });
    });

    ////////////////
    // Fetch v12  //
    ////////////////

    describe('Fetch v12', function () {
        it('should encode FetchRequestV12', function () {
            var encoded = protocol.write().FetchRequestV12({
                correlationId: 30,
                clientId: 'test',
                maxWaitTime: 500,
                minBytes: 1,
                maxBytes: 0x7fffffff,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [{
                    topicName: 'my-topic',
                    partitions: [{
                        partition: 0,
                        offset: 0,
                        maxBytes: 1048576
                    }]
                }],
                forgottenTopicsData: [],
                rackId: ''
            }).result;
            encoded.length.should.be.above(0);
        });

        it('should decode FetchResponseV12 with empty topics', function () {
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(30);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // errorCode
            writer.Int16BE(0);
            // sessionId
            writer.Int32BE(0);
            // topics: empty
            writer.compactArray([], function () {});
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).FetchResponseV12().result;
            result.correlationId.should.equal(30);
            result.throttleTime.should.equal(0);
            result.sessionId.should.equal(0);
            result.topics.length.should.equal(0);
        });
    });

    ////////////////
    // Fetch v13  //
    ////////////////

    describe('Fetch v13', function () {
        it('should encode FetchRequestV13 with topicId', function () {
            var topicId = '550e8400-e29b-41d4-a716-446655440000';
            var encoded = protocol.write().FetchRequestV13({
                correlationId: 40,
                clientId: 'test',
                maxWaitTime: 500,
                minBytes: 1,
                maxBytes: 0x7fffffff,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [{
                    topicId: topicId,
                    partitions: [{
                        partition: 0,
                        offset: 0,
                        maxBytes: 1048576
                    }]
                }],
                forgottenTopicsData: [],
                rackId: ''
            }).result;
            encoded.length.should.be.above(0);
        });

        it('should decode FetchResponseV13 with topicId', function () {
            var topicId = '550e8400-e29b-41d4-a716-446655440000';
            var writer = protocol.write();
            var result;

            // correlationId
            writer.Int32BE(40);
            // response header TaggedFields
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // errorCode
            writer.Int16BE(0);
            // sessionId
            writer.Int32BE(0);
            // topics: 1 topic
            writer.compactArray([1], function () {
                // topicId (uuid)
                this.uuid(topicId);
                // partitions: empty
                this.compactArray([], function () {});
                this.TaggedFields();
            });
            // TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).FetchResponseV13().result;
            result.correlationId.should.equal(40);
            result.topics.length.should.equal(1);
            result.topics[0].topicId.should.equal(topicId);
        });
    });
});
