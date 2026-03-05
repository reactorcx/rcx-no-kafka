'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');
var errors   = require('../lib/errors');
var crc32c   = require('../lib/protocol/misc/crc32c');
var globals  = require('../lib/protocol/globals');
var Kafka    = require('../lib/index');

describe('v0.11 Protocol Upgrade', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    //////////////////
    // CRC-32C      //
    //////////////////

    describe('CRC-32C', function () {
        it('should return 0 for empty buffer', function () {
            crc32c(Buffer.from('')).should.equal(0);
        });

        it('should match standard test vector for "123456789"', function () {
            crc32c(Buffer.from('123456789')).should.equal(0xE3069283);
        });

        it('should return unsigned 32-bit result', function () {
            var result = crc32c(Buffer.from('test'));
            result.should.be.a('number');
            result.should.be.at.least(0);
        });

        it('should produce different values for different inputs', function () {
            var a = crc32c(Buffer.from('hello'));
            var b = crc32c(Buffer.from('world'));
            a.should.not.equal(b);
        });

        it('should produce consistent results', function () {
            var a = crc32c(Buffer.from('kafka'));
            var b = crc32c(Buffer.from('kafka'));
            a.should.equal(b);
        });
    });

    //////////////////
    // Error Codes  //
    //////////////////

    describe('Error Codes', function () {
        it('should resolve error code 13 (NetworkException)', function () {
            var err = errors.byCode(13);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('NetworkException');
        });

        it('should resolve error code 33 (UnsupportedSaslMechanism)', function () {
            var err = errors.byCode(33);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('UnsupportedSaslMechanism');
        });

        it('should resolve error code 35 (UnsupportedVersion)', function () {
            var err = errors.byCode(35);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('UnsupportedVersion');
        });

        it('should resolve error code 36 (TopicAlreadyExists)', function () {
            var err = errors.byCode(36);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('TopicAlreadyExists');
        });

        it('should resolve error code 42 (InvalidRequest)', function () {
            var err = errors.byCode(42);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('InvalidRequest');
        });

        it('should resolve error code 51 (ConcurrentTransactions)', function () {
            var err = errors.byCode(51);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('ConcurrentTransactions');
        });

        it('should still resolve existing error codes', function () {
            errors.byCode(1).code.should.equal('OffsetOutOfRange');
            errors.byCode(3).code.should.equal('UnknownTopicOrPartition');
            errors.byCode(25).code.should.equal('UnknownMemberId');
        });
    });

    //////////////////
    // Globals      //
    //////////////////

    describe('API Keys', function () {
        it('should have ApiVersionsRequest key = 18', function () {
            globals.API_KEYS.ApiVersionsRequest.should.equal(18);
        });
    });

    describe('Exports', function () {
        it('should export COMPRESSION_LZ4 = 3', function () {
            Kafka.COMPRESSION_LZ4.should.equal(3);
        });
    });

    /////////////////////
    // MessageAttributes
    /////////////////////

    describe('MessageAttributes codec mask', function () {
        it('should read codec 3 (LZ4) correctly', function () {
            // Write a byte with value 3 (LZ4), then read it back
            var buf = protocol.write().MessageAttributes({ codec: 3 }).result;
            var result = protocol.read(buf).MessageAttributes().result;
            result.codec.should.equal(3);
        });

        it('should mask bits 0-2 for codec', function () {
            var buf = protocol.write().MessageAttributes({ codec: 7 }).result;
            var result = protocol.read(buf).MessageAttributes().result;
            result.codec.should.equal(7);
        });
    });

    /////////////////////
    // RecordHeader    //
    /////////////////////

    describe('RecordHeader', function () {
        it('should round-trip a header with key and value', function () {
            var buf = protocol.write().RecordHeader({
                key: 'content-type',
                value: Buffer.from('application/json')
            }).result;

            var result = protocol.read(buf).RecordHeader().result;
            result.key.should.equal('content-type');
            result.value.toString('utf8').should.equal('application/json');
        });

        it('should round-trip a header with empty value', function () {
            var buf = protocol.write().RecordHeader({
                key: 'trace-id',
                value: null
            }).result;

            var result = protocol.read(buf).RecordHeader().result;
            result.key.should.equal('trace-id');
            // null value writes as length 0
        });

        it('should round-trip a header with empty key', function () {
            var buf = protocol.write().RecordHeader({
                key: '',
                value: Buffer.from('val')
            }).result;

            var result = protocol.read(buf).RecordHeader().result;
            result.key.should.equal('');
            result.value.toString('utf8').should.equal('val');
        });
    });

    /////////////////////
    // Record          //
    /////////////////////

    describe('Record', function () {
        it('should round-trip a record with key and value', function () {
            var buf = protocol.write().Record({
                attributes: 0,
                timestampDelta: 100,
                offsetDelta: 5,
                key: Buffer.from('mykey'),
                value: Buffer.from('myvalue'),
                headers: []
            }).result;

            var result = protocol.read(buf).Record().result;
            result.attributes.should.equal(0);
            result.offsetDelta.should.equal(5);
            result.key.toString('utf8').should.equal('mykey');
            result.value.toString('utf8').should.equal('myvalue');
            result.headers.should.be.an('array').with.length(0);
        });

        it('should round-trip a record with null key', function () {
            var buf = protocol.write().Record({
                attributes: 0,
                timestampDelta: 0,
                offsetDelta: 0,
                key: null,
                value: Buffer.from('data'),
                headers: []
            }).result;

            var result = protocol.read(buf).Record().result;
            should.equal(result.key, null);
            result.value.toString('utf8').should.equal('data');
        });

        it('should round-trip a record with null value', function () {
            var buf = protocol.write().Record({
                attributes: 0,
                timestampDelta: 0,
                offsetDelta: 0,
                key: Buffer.from('k'),
                value: null,
                headers: []
            }).result;

            var result = protocol.read(buf).Record().result;
            result.key.toString('utf8').should.equal('k');
            should.equal(result.value, null);
        });

        it('should round-trip a record with headers', function () {
            var buf = protocol.write().Record({
                attributes: 0,
                timestampDelta: 0,
                offsetDelta: 0,
                key: null,
                value: Buffer.from('v'),
                headers: [
                    { key: 'h1', value: Buffer.from('v1') },
                    { key: 'h2', value: Buffer.from('v2') }
                ]
            }).result;

            var result = protocol.read(buf).Record().result;
            result.headers.should.be.an('array').with.length(2);
            result.headers[0].key.should.equal('h1');
            result.headers[0].value.toString('utf8').should.equal('v1');
            result.headers[1].key.should.equal('h2');
            result.headers[1].value.toString('utf8').should.equal('v2');
        });

        it('should handle string keys and values', function () {
            var buf = protocol.write().Record({
                attributes: 0,
                timestampDelta: 0,
                offsetDelta: 0,
                key: 'string-key',
                value: 'string-value',
                headers: []
            }).result;

            var result = protocol.read(buf).Record().result;
            result.key.toString('utf8').should.equal('string-key');
            result.value.toString('utf8').should.equal('string-value');
        });
    });

    /////////////////////
    // RecordBatch     //
    /////////////////////

    describe('RecordBatch', function () {
        it('should round-trip a batch with multiple records', function () {
            var timestamp = 1500000000000;
            var buf, batches, header, reader, rec0, rec1, rec2;
            buf = protocol.write().RecordBatch({
                baseOffset: 42,
                records: [
                    { key: Buffer.from('k1'), value: Buffer.from('v1'), timestamp: timestamp },
                    { key: null, value: Buffer.from('v2'), timestamp: timestamp + 100 },
                    { key: Buffer.from('k3'), value: Buffer.from('v3'), timestamp: timestamp + 200 }
                ],
                codec: 0,
                timestamp: timestamp
            }).result;

            batches = protocol.read(buf).RecordBatch(null, buf.length).result;
            batches.should.be.an('array').with.length(1);

            header = batches[0].header;
            header.baseOffset.should.equal(42);
            header.magic.should.equal(2);
            header.recordCount.should.equal(3);
            header.firstTimestamp.should.equal(timestamp);
            header.maxTimestamp.should.equal(timestamp + 200);
            header.codec.should.equal(0);
            header.lastOffsetDelta.should.equal(2);

            // parse records from raw
            reader = protocol.read(batches[0].recordsRaw);
            rec0 = reader.Record().result;
            rec0.offsetDelta.should.equal(0);
            rec0.key.toString('utf8').should.equal('k1');
            rec0.value.toString('utf8').should.equal('v1');

            rec1 = reader.Record().result;
            rec1.offsetDelta.should.equal(1);
            should.equal(rec1.key, null);
            rec1.value.toString('utf8').should.equal('v2');

            rec2 = reader.Record().result;
            rec2.offsetDelta.should.equal(2);
            rec2.key.toString('utf8').should.equal('k3');
            rec2.value.toString('utf8').should.equal('v3');
        });

        it('should round-trip a single record batch', function () {
            var buf, batches, rec;
            buf = protocol.write().RecordBatch({
                baseOffset: 0,
                records: [{ key: null, value: Buffer.from('hello') }],
                codec: 0
            }).result;

            batches = protocol.read(buf).RecordBatch(null, buf.length).result;
            batches.should.have.length(1);
            batches[0].header.recordCount.should.equal(1);

            rec = protocol.read(batches[0].recordsRaw).Record().result;
            rec.value.toString('utf8').should.equal('hello');
        });

        it('should round-trip records with headers', function () {
            var buf = protocol.write().RecordBatch({
                baseOffset: 0,
                records: [{
                    key: null,
                    value: Buffer.from('val'),
                    headers: [
                        { key: 'traceid', value: Buffer.from('abc123') }
                    ]
                }],
                codec: 0
            }).result;

            var batches = protocol.read(buf).RecordBatch(null, buf.length).result;
            var rec = protocol.read(batches[0].recordsRaw).Record().result;
            rec.headers.should.have.length(1);
            rec.headers[0].key.should.equal('traceid');
            rec.headers[0].value.toString('utf8').should.equal('abc123');
        });

        it('should compute valid CRC-32C', function () {
            var buf = protocol.write().RecordBatch({
                baseOffset: 0,
                records: [{ key: null, value: Buffer.from('crc-test') }],
                codec: 0
            }).result;

            // Read the CRC from the buffer and verify
            // CRC is at: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) = offset 17, 4 bytes
            var storedCrc = buf.readUInt32BE(17);

            // CRC covers from attributes (offset 21) to end
            var crcData = buf.slice(21);
            var computedCrc = crc32c(crcData);

            storedCrc.should.equal(computedCrc);
        });

        it('should set codec in attributes', function () {
            var buf = protocol.write().RecordBatch({
                baseOffset: 0,
                records: [{ key: null, value: Buffer.from('x') }],
                codec: 3 // LZ4
            }).result;

            var batches = protocol.read(buf).RecordBatch(null, buf.length).result;
            batches[0].header.codec.should.equal(3);
            (batches[0].header.attributes & 0x7).should.equal(3);
        });

        it('should handle empty size gracefully', function () {
            var result = protocol.read(Buffer.from([])).RecordBatch(null, 0).result;
            result.should.be.an('array').with.length(0);
        });
    });

    //////////////////////////
    // ApiVersions Protocol //
    //////////////////////////

    describe('ApiVersionsRequest', function () {
        it('should serialize a valid request', function () {
            var buf = protocol.write().ApiVersionsRequest({
                correlationId: 1,
                clientId: 'test-client'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.length.should.be.above(0);

            // apiKey should be 18 at offset 0 (Int16BE)
            buf.readInt16BE(0).should.equal(18);
            // apiVersion should be 0 at offset 2
            buf.readInt16BE(2).should.equal(0);
            // correlationId at offset 4
            buf.readInt32BE(4).should.equal(1);
        });
    });

    describe('ApiVersionsResponse', function () {
        it('should deserialize a response buffer', function () {
            // Build a mock ApiVersionsResponse buffer:
            // correlationId(4) + errorCode(2) + array_length(4) + [apiKey(2) + minVersion(2) + maxVersion(2)] * N
            var buf = Buffer.alloc(4 + 2 + 4 + 6 + 6); // 2 api version entries
            var offset = 0;
            var result;

            // correlationId = 1
            buf.writeInt32BE(1, offset); offset += 4;
            // errorCode = 0 (no error)
            buf.writeInt16BE(0, offset); offset += 2;
            // array length = 2
            buf.writeInt32BE(2, offset); offset += 4;

            // entry 1: Produce (apiKey=0, minVersion=0, maxVersion=3)
            buf.writeInt16BE(0, offset); offset += 2;
            buf.writeInt16BE(0, offset); offset += 2;
            buf.writeInt16BE(3, offset); offset += 2;

            // entry 2: Fetch (apiKey=1, minVersion=0, maxVersion=4)
            buf.writeInt16BE(1, offset); offset += 2;
            buf.writeInt16BE(0, offset); offset += 2;
            buf.writeInt16BE(4, offset); offset += 2;

            result = protocol.read(buf).ApiVersionsResponse().result;
            result.correlationId.should.equal(1);
            should.equal(result.error, null);
            result.apiVersions.should.be.an('array').with.length(2);
            result.apiVersions[0].apiKey.should.equal(0);
            result.apiVersions[0].minVersion.should.equal(0);
            result.apiVersions[0].maxVersion.should.equal(3);
            result.apiVersions[1].apiKey.should.equal(1);
            result.apiVersions[1].maxVersion.should.equal(4);
        });
    });

    //////////////////////////
    // Fetch v3/v4          //
    //////////////////////////

    describe('FetchRequest v3', function () {
        it('should serialize with maxBytes field', function () {
            var buf = protocol.write().FetchRequestV3({
                correlationId: 1,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 1048576,
                topics: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            // apiVersion should be 3 at offset 2
            buf.readInt16BE(2).should.equal(3);
        });
    });

    describe('FetchRequest v4', function () {
        it('should serialize with isolationLevel field', function () {
            var buf = protocol.write().FetchRequestV4({
                correlationId: 1,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 1048576,
                isolationLevel: 1,
                topics: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            // apiVersion should be 4 at offset 2
            buf.readInt16BE(2).should.equal(4);
        });
    });

    //////////////////////////
    // Produce v3           //
    //////////////////////////

    describe('ProduceRequest v3', function () {
        it('should serialize with transactionalId and RecordBatch', function () {
            var buf = protocol.write().ProduceRequestV3({
                correlationId: 1,
                clientId: 'test',
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
            // apiVersion should be 3 at offset 2
            buf.readInt16BE(2).should.equal(3);
            buf.length.should.be.above(50);
        });

        it('should set transactionalId to null by default', function () {
            var buf = protocol.write().ProduceRequestV3({
                correlationId: 1,
                clientId: 'test',
                requiredAcks: 1,
                timeout: 30000,
                topics: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
        });
    });

    describe('ProduceResponse v2', function () {
        it('should deserialize response with timestamp', function () {
            // Build mock ProduceResponseV2:
            // correlationId(4) + array_length(4) + topicName + array_length(4) + partition(4) + errorCode(2) + offset(8) + timestamp(8) + throttleTime(4)
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 2 + topicBuf.length + 4 + 4 + 2 + 8 + 8 + 4);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(1, offset); offset += 4; // topics array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2; // topic name length
            topicBuf.copy(buf, offset); offset += topicBuf.length; // topic name
            buf.writeInt32BE(1, offset); offset += 4; // partitions array length
            buf.writeInt32BE(0, offset); offset += 4; // partition
            buf.writeInt16BE(0, offset); offset += 2; // error code (no error)
            // offset (Int64BE = 0)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(42, offset); offset += 4;
            // timestamp (Int64BE)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(1500000000, offset); offset += 4;
            buf.writeInt32BE(0, offset); offset += 4; // throttleTime

            result = protocol.read(buf).ProduceResponseV2().result;
            result.correlationId.should.equal(1);
            result.topics.should.be.an('array').with.length(1);
            result.topics[0].topicName.should.equal('test-topic');
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].offset.should.equal(42);
            result.topics[0].partitions[0].timestamp.should.be.a('number');
        });
    });

    //////////////////////////
    // Metadata v1          //
    //////////////////////////

    describe('MetadataRequest v1', function () {
        it('should serialize with apiVersion 1', function () {
            var buf = protocol.write().MetadataRequestV1({
                correlationId: 1,
                clientId: 'test',
                topicNames: ['topic1']
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(1); // apiVersion
        });

        it('should serialize with empty topic list', function () {
            var buf = protocol.write().MetadataRequestV1({
                correlationId: 1,
                clientId: 'test',
                topicNames: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
        });
    });

    //////////////////////////
    // Offset v1            //
    //////////////////////////

    describe('OffsetRequest v1', function () {
        it('should serialize with apiVersion 1 and no maxNumberOfOffsets', function () {
            var buf = protocol.write().OffsetRequestV1({
                correlationId: 1,
                clientId: 'test',
                topics: [{
                    topicName: 'test-topic',
                    partitions: [{
                        partition: 0,
                        time: -1
                    }]
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(1); // apiVersion
        });
    });

    describe('OffsetResponse v1', function () {
        it('should deserialize response with single offset and timestamp', function () {
            // Build mock OffsetResponseV1:
            // correlationId(4) + array_length(4) + topicName + array_length(4) + partition(4) + errorCode(2) + timestamp(8) + offset(8)
            var topicBuf = Buffer.from('test-topic', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 2 + topicBuf.length + 4 + 4 + 2 + 8 + 8);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4; // correlationId
            buf.writeInt32BE(1, offset); offset += 4; // topics array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4; // partitions array length
            buf.writeInt32BE(0, offset); offset += 4; // partition
            buf.writeInt16BE(0, offset); offset += 2; // error code
            // timestamp (Int64BE = -1)
            buf.writeInt32BE(-1, offset); offset += 4;
            buf.writeInt32BE(-1, offset); offset += 4;
            // offset (Int64BE = 100)
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt32BE(100, offset); offset += 4;

            result = protocol.read(buf).OffsetResponseV1().result;
            result.correlationId.should.equal(1);
            result.topics.should.be.an('array').with.length(1);
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].offset.should.equal(100);
        });
    });

    //////////////////////////
    // Version Negotiation  //
    //////////////////////////

    describe('Version Negotiation', function () {
        var Client = require('../lib/client');

        it('_negotiateVersion should return 0 when no versions available', function () {
            var client = Object.create(Client.prototype);
            client._negotiateVersion({ apiVersions: null }, 1, 4).should.equal(0);
        });

        it('_negotiateVersion should return 0 when apiKey not in version map', function () {
            var client = Object.create(Client.prototype);
            client._negotiateVersion({ apiVersions: {} }, 1, 4).should.equal(0);
        });

        it('_negotiateVersion should return min of clientMax and broker max', function () {
            var client = Object.create(Client.prototype);
            var conn = { apiVersions: { 1: { min: 0, max: 3 } } };
            client._negotiateVersion(conn, 1, 4).should.equal(3);
            client._negotiateVersion(conn, 1, 2).should.equal(2);
            client._negotiateVersion(conn, 1, 3).should.equal(3);
        });

        it('_negotiateVersion should return 0 for unknown api key', function () {
            var client = Object.create(Client.prototype);
            var conn = { apiVersions: { 1: { min: 0, max: 4 } } };
            client._negotiateVersion(conn, 99, 4).should.equal(0);
        });
    });

    //////////////////////////
    // Connection           //
    //////////////////////////

    describe('Connection apiVersions field', function () {
        var Connection = require('../lib/connection');

        it('should initialize apiVersions to null', function () {
            var conn = new Connection({ host: '127.0.0.1', port: 9092 });
            should.equal(conn.apiVersions, null);
        });
    });
});
