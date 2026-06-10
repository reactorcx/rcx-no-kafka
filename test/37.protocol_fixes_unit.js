'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');

function i16(n) { var b = Buffer.alloc(2); b.writeInt16BE(n, 0); return b; }
function i32(n) { var b = Buffer.alloc(4); b.writeInt32BE(n, 0); return b; }
function i64(n) {
    var b = Buffer.alloc(8);
    b.writeUInt32BE(Math.floor(n / 0x100000000), 0);
    b.writeUInt32BE(n % 0x100000000, 4);
    return b;
}
function str(s) { return Buffer.concat([i16(s.length), Buffer.from(s)]); }

// ConsumerProtocolSubscription envelope as sent by modern Java/librdkafka clients:
// version + subscriptions + userData + (v1+: ownedPartitions) + (v2+: generationId)
function subscriptionMetadataV2(topics, generationId) {
    var parts = [i16(2), i32(topics.length)];
    topics.forEach(function (t) { parts.push(str(t)); });
    parts.push(i32(-1)); // null userData
    parts.push(i32(0));  // empty ownedPartitions
    parts.push(i32(generationId)); // v2 field this client does not model
    return Buffer.concat(parts);
}

function memberEntry(id, metadata) {
    return Buffer.concat([str(id), i32(metadata.length), metadata]);
}

describe('Protocol fixes (unit)', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    describe('JoinGroup member metadata envelope (mixed-client groups)', function () {
        it('should skip unconsumed v2+ subscription fields so the NEXT member parses correctly', function () {
            var buf = Buffer.concat([
                memberEntry('member-1', subscriptionMetadataV2(['topic-a'], 7)),
                memberEntry('member-2', subscriptionMetadataV2(['topic-b'], 7))
            ]);
            var result = protocol.read(buf)
                .JoinConsumerGroupResponse_Member('m1')
                .JoinConsumerGroupResponse_Member('m2')
                .result;

            result.m1.id.should.equal('member-1');
            result.m1.subscriptions.should.eql(['topic-a']);
            result.m2.id.should.equal('member-2');
            result.m2.subscriptions.should.eql(['topic-b']);
        });

        it('should skip unconsumed fields in the V5 member reader too', function () {
            var entry = Buffer.concat([
                str('member-1'), str('instance-1'),
                i32(subscriptionMetadataV2(['topic-a'], 7).length), subscriptionMetadataV2(['topic-a'], 7),
                str('member-2'), str('instance-2'),
                i32(subscriptionMetadataV2(['topic-b'], 7).length), subscriptionMetadataV2(['topic-b'], 7)
            ]);
            var result = protocol.read(entry)
                .JoinConsumerGroupResponse_MemberV5('m1')
                .JoinConsumerGroupResponse_MemberV5('m2')
                .result;

            result.m1.subscriptions.should.eql(['topic-a']);
            result.m2.id.should.equal('member-2');
            result.m2.subscriptions.should.eql(['topic-b']);
        });
    });

    describe('RecordBatch CRC verification', function () {
        function writeBatch() {
            return protocol.write().RecordBatch({
                baseOffset: 0,
                records: [{ key: null, value: 'hello-crc', timestamp: 1700000000000 }],
                codec: 0,
                timestamp: 1700000000000
            }).result;
        }

        it('should read back an intact batch', function () {
            var buf = writeBatch();
            var result = protocol.read(buf).RecordBatch('batches', buf.length).result;
            result.batches.should.have.length(1);
            result.batches[0].header.recordCount.should.equal(1);
        });

        it('should reject a batch whose records bytes were corrupted', function () {
            var buf = writeBatch();
            buf[buf.length - 3] ^= 0xFF; // flip bits inside the records section
            (function () {
                protocol.read(buf).RecordBatch('batches', buf.length);
            }).should.throw(/CRC/i);
        });
    });

    describe('legacy magic-1 message format', function () {
        it('should parse the v1 timestamp instead of consuming it as the key length', function () {
            var value = Buffer.from('hi');
            var buf = Buffer.concat([
                i32(0),              // crc (not verified on legacy read path)
                Buffer.from([1]),    // magicByte = 1
                Buffer.from([0]),    // attributes
                i64(1700000000123),  // v1 timestamp
                i32(-1),             // null key
                i32(value.length), value
            ]);
            var result = protocol.read(buf).Message('m').result;

            should.equal(result.m.key, null);
            result.m.value.toString().should.equal('hi');
            result.m.timestamp.should.equal(1700000000123);
        });
    });

    describe('fetch v0-v3 negative messageSetSize', function () {
        it('should treat a negative messageSetSize as an empty message set', function () {
            var buf = Buffer.concat([
                i32(0),   // partition
                i16(0),   // no error
                i64(100), // highwaterMark
                i32(-1)   // messageSetSize claimed negative by a broken/malicious broker
            ]);
            var result = protocol.read(buf).FetchResponsePartitionItem('p').result;

            result.p.partition.should.equal(0);
            result.p.messageSet.should.eql([]);
        });
    });

    describe('OffsetCommitRequestV1 generationId encoding', function () {
        it('should encode generationId as Int32, not string', function () {
            var buf = protocol.write().OffsetCommitRequestV1({
                correlationId: 1,
                clientId: 'c',
                groupId: 'g',
                generationId: 5,
                memberId: 'm',
                topics: []
            }).result;
            // header: apiKey(2) + apiVersion(2) + correlationId(4) + clientId(2+1)
            // + groupId(2+1) → generationId starts at offset 14
            buf.readInt32BE(14).should.equal(5);
            // memberId string follows immediately
            buf.readInt16BE(18).should.equal(1);
            buf.toString('utf8', 20, 21).should.equal('m');
        });
    });

    describe('uuid write validation', function () {
        it('should round-trip a valid uuid', function () {
            var id = '01234567-89ab-cdef-0123-456789abcdef';
            var buf = protocol.write().uuid(id).result;
            protocol.read(buf).uuid('u').result.u.should.equal(id);
        });

        it('should throw on a malformed uuid instead of corrupting the frame', function () {
            (function () {
                protocol.write().uuid('not-a-uuid');
            }).should.throw(/uuid/i);
        });
    });
});
