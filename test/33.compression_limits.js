'use strict';

/* global describe, it, afterEach, sinon, should */

var compression = require('../lib/protocol/misc/compression');
var zlib        = require('zlib');
var Protocol    = require('../lib/protocol');
var Kafka       = require('../lib/index');
var promiseUtils = require('../lib/promise-utils');
var protocol    = new Protocol({ bufferSize: 262144 });
var requireSafe = function (moduleName) {
    try {
        require.resolve(moduleName);
        return require(moduleName);
    } catch (err) {
        return undefined;
    }
};
var snappy = requireSafe('snappy');
var zstd   = requireSafe('zstd-napi');
var lz4    = requireSafe('lz4');

describe('Compression limits (unit)', function () {
    var defaultLimit = compression.maxOutputSize;

    afterEach(function () {
        compression.maxOutputSize = defaultLimit;
    });

    it('should export a finite default maxOutputSize', function () {
        compression.maxOutputSize.should.be.a('number').and.be.above(0);
    });

    it('should reject gzip output exceeding maxOutputSize (sync)', function () {
        var bomb = zlib.gzipSync(Buffer.alloc(100000));
        compression.maxOutputSize = 1024;
        return compression.decompress(bomb, 1).should.be.rejected;
    });

    it('should reject gzip output exceeding maxOutputSize (async)', function () {
        var bomb = zlib.gzipSync(Buffer.alloc(100000));
        compression.maxOutputSize = 1024;
        return compression.decompressAsync(bomb, 1).should.be.rejected;
    });

    it('should reject snappy output exceeding maxOutputSize (sync)', function () {
        var bomb;
        if (!snappy) { return this.skip(); }
        bomb = snappy.compressSync(Buffer.alloc(100000));
        compression.maxOutputSize = 1024;
        return compression.decompress(bomb, 2).should.be.rejected;
    });

    it('should reject snappy output exceeding maxOutputSize (async)', function () {
        var bomb;
        if (!snappy) { return this.skip(); }
        bomb = snappy.compressSync(Buffer.alloc(100000));
        compression.maxOutputSize = 1024;
        return compression.decompressAsync(bomb, 2).should.be.rejected;
    });

    it('should reject zstd output exceeding maxOutputSize', function () {
        var bomb;
        if (!zstd) { return this.skip(); }
        bomb = zstd.compress(Buffer.alloc(100000));
        compression.maxOutputSize = 1024;
        return compression.decompress(bomb, 4).should.be.rejected;
    });

    it('should reject lz4 output exceeding maxOutputSize', function () {
        var bomb;
        if (!lz4) { return this.skip(); }
        bomb = lz4.encode(Buffer.alloc(100000));
        compression.maxOutputSize = 1024;
        return compression.decompress(bomb, 3).should.be.rejected;
    });

    // the largest single allocation made by fn(): a decompressor must stop near the cap,
    // not allocate the whole output first and check it afterwards
    function largestAllocation(fn) {
        var spy = sinon.spy(Buffer, 'allocUnsafe');
        return Promise.resolve().then(fn).finally(function () {
            spy.restore();
        }).then(function () {
            throw new Error('expected rejection');
        }, function (err) {
            err.message.should.match(/maxOutputSize/);
            return Math.max.apply(null, spy.args.map(function (a) { return a[0]; }).concat(0));
        });
    }

    // a zstd frame written in streaming mode, as Java's producer writes them: no content size
    // in its header (the input is fed before the frame is ended)
    function zstdStreamed(data) {
        var binding = require('zstd-napi/binding');
        var cctx = new binding.CCtx(), out = Buffer.alloc(1 << 16), parts = [], src = data, r;
        do {
            r = cctx.compressStream2(out, src, binding.EndDirective.continue);
            parts.push(Buffer.from(out.subarray(0, r[1])));
            src = src.subarray(r[2]);
        } while (src.length > 0);
        do {
            r = cctx.compressStream2(out, Buffer.alloc(0), binding.EndDirective.end);
            parts.push(Buffer.from(out.subarray(0, r[1])));
        } while (r[0] !== 0);
        return Buffer.concat(parts);
    }

    [
        ['zstd', 4, function (b) { return zstd.compress(b); }],
        ['zstd (no content size)', 4, function (b) { return zstdStreamed(b); }],
        ['lz4', 3, function (b) { return lz4.encode(b); }]
    ].forEach(function (c) {
        it('should stop decompressing ' + c[0] + ' near maxOutputSize instead of inflating it all', function () {
            var bomb;
            if (!(c[1] === 4 ? zstd : lz4)) { return this.skip(); }
            bomb = c[2](Buffer.alloc(32 * 1024 * 1024));
            compression.maxOutputSize = 1024 * 1024;
            return largestAllocation(function () { return compression.decompress(bomb, c[1]); }).then(function (max) {
                max.should.be.at.most(8 * 1024 * 1024); // lz4: two 4 MB blocks per output buffer, not the 32 MB
            });
        });

        it('should reject a truncated ' + c[0] + ' frame, and still decode the next one', function () {
            var data = Buffer.from(require('crypto').randomBytes(256 * 1024).toString('hex')), frame;
            if (!(c[1] === 4 ? zstd : lz4)) { return this.skip(); }
            frame = c[2](data);
            return compression.decompress(frame.slice(0, frame.length >> 1), c[1]).then(function () {
                throw new Error('expected rejection');
            }, function (err) {
                err.message.should.not.equal('expected rejection');
                return promiseUtils.delay(10); // an unheard decoder 'error' event would crash the process here
            }).then(function () {
                // a decoder left mid-frame failed every later decode in the process
                return compression.decompress(frame, c[1]);
            }).then(function (out) {
                out.equals(data).should.equal(true);
            });
        });

        it('should still decompress a whole ' + c[0] + ' frame', function () {
            var data = Buffer.from(require('crypto').randomBytes(3 * 1024 * 1024).toString('hex'));
            if (!(c[1] === 4 ? zstd : lz4)) { return this.skip(); }
            return compression.decompress(c[2](data), c[1]).then(function (out) {
                out.equals(data).should.equal(true);
            });
        });
    });

    // the bytes Buffer.alloc/allocUnsafe hand out while fn() runs
    function bytesAllocated(fn) {
        var a = sinon.spy(Buffer, 'alloc'), u = sinon.spy(Buffer, 'allocUnsafe');
        function sum(spy) { return spy.args.reduce(function (s, x) { return s + x[0]; }, 0); }
        return Promise.resolve().then(fn).finally(function () {
            a.restore();
            u.restore();
        }).then(function (out) {
            return { out: out, bytes: sum(a) + sum(u) };
        });
    }

    [['lz4', 3], ['zstd', 4]].forEach(function (c) {
        it('should allocate an incompressible ' + c[0] + ' batch\'s size once, not twice', function () {
            var data = require('crypto').randomBytes(8 * 1024 * 1024), frame;
            if (!(c[1] === 4 ? zstd : lz4)) { return this.skip(); }
            frame = c[1] === 4 ? zstd.compress(data) : lz4.encode(data); // stored blocks / a frame larger than its output
            return bytesAllocated(function () { return compression.decompress(frame, c[1]); }).then(function (r) {
                r.out.equals(data).should.equal(true);
                r.bytes.should.be.below(9 * 1024 * 1024); // was 16 MB
            });
        });
    });

    it('should not decode a size-less zstd frame past the cap, however large its input', function () {
        var frame;
        if (!zstd) { return this.skip(); }
        frame = zstdStreamed(require('crypto').randomBytes(2 * 1024 * 1024)); // incompressible: input as large as output
        compression.maxOutputSize = 64 * 1024;
        return largestAllocation(function () { return compression.decompress(frame, 4); }).then(function (max) {
            max.should.be.at.most(64 * 1024 + 1); // was the 2 MB input's size
        });
    });

    it('should decode a zstd frame whose window fits a raised maxOutputSize', function () {
        var binding, cctx, out = Buffer.alloc(1 << 16), parts = [], r, src = Buffer.from('hello'), frame;
        if (!zstd) { return this.skip(); }
        binding = require('zstd-napi/binding');
        cctx = new binding.CCtx();
        cctx.setParameter(binding.CParameter.windowLog, 28); // a 256 MB window, no content size
        do {
            r = cctx.compressStream2(out, src, binding.EndDirective.continue);
            parts.push(Buffer.from(out.subarray(0, r[1])));
            src = src.subarray(r[2]);
        } while (src.length > 0);
        do {
            r = cctx.compressStream2(out, Buffer.alloc(0), binding.EndDirective.end);
            parts.push(Buffer.from(out.subarray(0, r[1])));
        } while (r[0] !== 0);
        frame = Buffer.concat(parts);
        compression.maxOutputSize = 300 * 1024 * 1024; // zstd's own default limit is 128 MB
        return compression.decompress(frame, 4).then(function (o) {
            o.toString().should.equal('hello');
        });
    });

    it('should not allocate per zstd frame when a batch holds many tiny frames', function () {
        var frames = [], i;
        if (!zstd) { return this.skip(); }
        for (i = 0; i < 2000; i++) { frames.push(zstd.compress(Buffer.from('A'))); }
        return bytesAllocated(function () { return compression.decompress(Buffer.concat(frames), 4); }).then(function (r) {
            r.out.equals(Buffer.alloc(2000, 'A')).should.equal(true);
            r.bytes.should.be.below(1024 * 1024); // a 128 KB buffer per frame was 256 MB
        });
    });

    function lz4Frame(blockMaxByte, blocks) { // FLG 0x60: version 1, independent blocks, no checksums
        var header = Buffer.from([0x04, 0x22, 0x4d, 0x18, 0x60, blockMaxByte, 0]);
        header[6] = lz4.utils.descriptorChecksum(header.slice(4, 6));
        return Buffer.concat([header].concat(blocks).concat([Buffer.alloc(4)]));
    }

    it('should not allocate per lz4 block when a frame holds many tiny blocks', function () {
        var blocks = [], i;
        if (!lz4) { return this.skip(); }
        for (i = 0; i < 2000; i++) { blocks.push(Buffer.from('020000001041', 'hex')); } // 2-byte block decoding to 'A'
        return bytesAllocated(function () { return compression.decompress(lz4Frame(0x70, blocks), 3); }).then(function (r) {
            r.out.equals(Buffer.alloc(2000, 'A')).should.equal(true);
            r.bytes.should.be.below(16 * 1024 * 1024); // a 4 MB buffer per block was 8 GB
        });
    });

    it('should decode an lz4 frame with an uncompressed block', function () {
        var payload = Buffer.from('incompressible'), size = Buffer.alloc(4);
        if (!lz4) { return this.skip(); }
        size.writeUInt32LE((0x80000000 | payload.length) >>> 0, 0);
        return compression.decompress(lz4Frame(0x40, [size, payload]), 3).then(function (out) {
            out.equals(payload).should.equal(true);
        });
    });

    it('should keep nothing for empty stored lz4 blocks', function () {
        var blocks = [], i, empty = Buffer.alloc(4), spy, frame;
        if (!lz4) { return this.skip(); }
        empty.writeUInt32LE(0x80000000, 0); // a stored block of 0 bytes
        for (i = 0; i < 100000; i++) { blocks.push(empty); }
        frame = lz4Frame(0x40, blocks);
        spy = sinon.spy(Buffer, 'concat');
        return compression.decompress(frame, 3).finally(function () {
            spy.restore();
        }).then(function (out) {
            out.length.should.equal(0);
            spy.args.some(function (a) { return a[0].length >= 100000; }).should.equal(false); // no slice kept per empty block
        });
    });

    it('should reject lz4 skippable frames and bad checksums', function () {
        var frame, corrupt;
        if (!lz4) { return this.skip(); }
        frame = lz4.encode(Buffer.alloc(1000, 'q')); // content checksum on
        corrupt = Buffer.from(frame);
        corrupt[corrupt.length - 2] ^= 1;
        return Promise.all([
            // the module's decoder looped forever on these 9 bytes, alone or after a frame
            compression.decompress(Buffer.from('502A4D1801000000AA', 'hex'), 3),
            compression.decompress(Buffer.concat([frame, Buffer.from('502A4D1801000000AA', 'hex')]), 3),
            compression.decompress(corrupt, 3)
        ].map(function (p) {
            return p.then(function () { return 'resolved'; }, function (err) { return err.message; });
        })).then(function (r) {
            r[0].should.match(/Not an LZ4 frame/);
            r[1].should.match(/Not an LZ4 frame/);
            r[2].should.match(/checksum/);
        });
    });

    it('should decode a zstd frame that declares its size into one buffer of that size', function () {
        var data = Buffer.alloc(1000, 'kafka '), frame, spy; // compresses to far less than 1000 bytes
        if (!zstd) { return this.skip(); }
        frame = zstd.compress(data);
        spy = sinon.spy(Buffer, 'allocUnsafe');
        return compression.decompress(frame, 4).finally(function () {
            spy.restore();
        }).then(function (out) {
            out.equals(data).should.equal(true);
            spy.args.map(function (a) { return a[0]; }).should.eql([data.length]);
        });
    });

    it('should still decompress data within the limit', function () {
        var payload = Buffer.from('hello kafka');
        var compressed = zlib.gzipSync(payload);
        return compression.decompress(compressed, 1).then(function (out) {
            out.toString().should.equal('hello kafka');
        });
    });

    it('should reject (not throw) on malformed input to the sync decompress path', function () {
        var result;
        (function () {
            result = compression.decompress(Buffer.from('definitely not gzip data'), 1);
        }).should.not.throw();
        return result.should.be.rejected;
    });

    describe('fetch decoding of a batch over the limit', function () {
        // a 2 KB record behind gzip, against a 1 KB cap
        function bigBatch(base) {
            var records = Buffer.from(protocol.write().Record({
                attributes: 0, timestampDelta: 0, offsetDelta: 0, key: null, value: Buffer.alloc(2048), headers: []
            }).result);
            return Buffer.from(protocol.write().RecordBatch({
                baseOffset: base, codec: 1, compressedRecords: zlib.gzipSync(records), recordCount: 1
            }).result);
        }
        function smallBatch(base) {
            return Buffer.from(protocol.write().RecordBatch({ baseOffset: base, records: [{ value: 'ok' }] }).result);
        }
        function bigMessage(offset) {
            var inner = Buffer.from(protocol.write().MessageSet([{ offset: 0, message: { value: Buffer.alloc(2048) } }]).result);
            return { offset: offset, message: { attributes: { codec: 1 }, value: zlib.gzipSync(inner) } };
        }
        function smallMessage(offset) {
            return { offset: offset, message: { value: Buffer.from('ok') } };
        }
        function legacy(messages) {
            return Buffer.from(protocol.write().MessageSet(messages).result);
        }

        // FetchResponse v0 (legacy MessageSet) or v4 (RecordBatch) for t:0
        function fetch(version, records) {
            var c = new Kafka.Producer({ connectionString: '127.0.0.1:1', logger: { logLevel: 0 } }).client;
            var w = protocol.write().Int32BE(1), buf;
            if (version === 4) { w.Int32BE(0); } // throttleTime
            w.Int32BE(1).string('t').Int32BE(1).Int32BE(0).Int16BE(0).Int64BE(1000);
            if (version === 4) { w.Int64BE(1000).Int32BE(0); } // lastStableOffset, no aborted transactions
            buf = Buffer.from(w.Int32BE(records.length).raw(records).result);
            c.brokerConnections = { 0: {
                apiVersions: version === 4 ? { 1: { min: 4, max: 4 } } : null,
                send: function () { return Promise.resolve(buf); }
            } };
            return c.fetchRequest({ 0: [{ topicName: 't', partitions: [{ partition: 0, offset: 0, maxBytes: 1 << 20 }] }] })
            .then(function (r) { return r[0]; });
        }

        function offsets(p) {
            return p.messageSet.map(function (m) { return m.offset; });
        }

        it('should not skip a legacy compressed message it cannot decompress', function () {
            compression.maxOutputSize = 1024;
            return fetch(0, legacy([bigMessage(0), smallMessage(1)])).then(function (p) {
                offsets(p).should.eql([]); // delivering offset 1 would move the consumer past offset 0
                should.exist(p.error);
                return fetch(0, legacy([smallMessage(0), bigMessage(1), smallMessage(2)]));
            }).then(function (p) {
                offsets(p).should.eql([0]);
                should.not.exist(p.error);
            });
        });

        it('should skip an undecodable batch in a share fetch and take it out of acquiredRecords', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var raw = Buffer.concat([smallBatch(0), bigBatch(1), smallBatch(2)]);
            var batches = protocol.read(raw).RecordBatch(null, raw.length).result;
            compression.maxOutputSize = 1024;
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: batches, acquiredRecords: [{ firstOffset: 0, lastOffset: 2, deliveryCount: 1 }]
            }]).then(function (r) {
                offsets(r[0]).should.eql([0, 2]);
                // a handler acking these ranges can't accept offset 1, which it never received
                r[0].acquiredRecords.should.eql([
                    { firstOffset: 0, lastOffset: 0, deliveryCount: 1 },
                    { firstOffset: 2, lastOffset: 2, deliveryCount: 1 }
                ]);
            });
        });

        it('should log skipped share batches once per partition', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var corrupt = smallBatch(1), raw, batches;
            corrupt[corrupt.length - 2] ^= 0xFF;
            raw = Buffer.concat([smallBatch(0), corrupt, bigBatch(2), corrupt, smallBatch(5)]);
            batches = protocol.read(raw).RecordBatch(null, raw.length).result;
            compression.maxOutputSize = 1024;
            sinon.spy(c, 'error');
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: batches, acquiredRecords: [{ firstOffset: 0, lastOffset: 5, deliveryCount: 1 }]
            }]).then(function (r) {
                offsets(r[0]).should.eql([0, 5]);
                c.error.callCount.should.equal(1);
                c.error.firstCall.args[1].should.equal(3); // skipped
            });
        });

        it('should deliver only the share records this member acquired', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var raw = Buffer.from(protocol.write().RecordBatch({ baseOffset: 10, records: [0, 1, 2, 3, 4, 5].map(function (i) {
                return { value: 'v' + i };
            }) }).result);
            var batches = protocol.read(raw).RecordBatch(null, raw.length).result;
            return c._decompressShareFetchResults([{
                // a batch is returned whole; the broker gave this member 12-13 and 15 only
                topic: 't', partition: 0, messageSet: batches, acquiredRecords: [
                    { firstOffset: 15, lastOffset: 15, deliveryCount: 1 },
                    { firstOffset: 12, lastOffset: 13, deliveryCount: 2 }
                ]
            }]).then(function (r) {
                offsets(r[0]).should.eql([12, 13, 15]);
            });
        });

        it('should not trust the header of a CRC-failed share batch for acquiredRecords', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var corrupt = Buffer.from(protocol.write().RecordBatch({ baseOffset: 0, records: [{ value: 'a' }, { value: 'b' }] }).result);
            var raw, batches;
            corrupt.writeInt32BE(0, 23); // lastOffsetDelta 1 -> 0, CRC left as it was
            raw = Buffer.concat([corrupt, smallBatch(2)]);
            batches = protocol.read(raw).RecordBatch(null, raw.length).result;
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: batches, acquiredRecords: [{ firstOffset: 0, lastOffset: 2, deliveryCount: 1 }]
            }]).then(function (r) {
                offsets(r[0]).should.eql([2]);
                // offset 1 was never delivered, so it must not be acknowledgeable
                r[0].acquiredRecords.should.eql([{ firstOffset: 2, lastOffset: 2, deliveryCount: 1 }]);
            });
        });

        it('should fail a share batch holding a record outside its offset span', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client, recs, crafted, raw, batches;
            recs = Buffer.concat([0, 100].map(function (d) {
                return Buffer.from(protocol.write().Record({
                    attributes: 0, timestampDelta: 0, offsetDelta: d, key: null, value: Buffer.from('x'), headers: []
                }).result);
            }));
            // a writer's own batch, its CRC valid, spanning 0-1 but with a record claiming offset 100
            crafted = Buffer.from(protocol.write().RecordBatch({ baseOffset: 0, compressedRecords: recs, recordCount: 2, lastOffsetDelta: 1 }).result);
            raw = Buffer.concat([crafted, smallBatch(2)]);
            batches = protocol.read(raw).RecordBatch(null, raw.length).result;
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: batches, acquiredRecords: [{ firstOffset: 0, lastOffset: 2, deliveryCount: 1 }]
            }]).then(function (r) {
                offsets(r[0]).should.eql([2]); // another producer's record, not dropped (and GAP-acked) behind offset 100
                r[0].acquiredRecords.should.eql([{ firstOffset: 2, lastOffset: 2, deliveryCount: 1 }]);
            });
        });

        it('should keep a legacy share MessageSet acknowledgeable up to its last complete message', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0,
                messageSet: [{ offset: 5, message: { attributes: { codec: 0 }, value: Buffer.from('a') } }, { offset: 6, _partial: true }],
                acquiredRecords: [{ firstOffset: 5, lastOffset: 6, deliveryCount: 1 }]
            }]).then(function (r) {
                r[0].messageSet.map(function (m) { return m.offset; }).should.eql([5]);
                r[0].acquiredRecords.should.eql([{ firstOffset: 5, lastOffset: 5, deliveryCount: 1 }]);
            });
        });

        // a magic-1 gzip wrapper at offset `at` holding values with relative inner offsets 0..n-1
        function legacyWrapper(at, values) {
            var inner = Buffer.from(protocol.write().MessageSet(values.map(function (v, i) {
                return { offset: i, message: { magicByte: 0, value: Buffer.from(v) } };
            })).result);
            return { offset: at, message: { magicByte: 1, attributes: { codec: 1, _raw: 1 }, value: zlib.gzipSync(inner) } };
        }

        it('should expand a compressed legacy share wrapper to its records at their absolute offsets', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: [legacyWrapper(11, ['a', 'b'])],
                acquiredRecords: [{ firstOffset: 10, lastOffset: 11, deliveryCount: 1 }]
            }]).then(function (r) {
                offsets(r[0]).should.eql([10, 11]); // not the wrapper alone: offset 10 is a record, not a gap
                r[0].acquiredRecords.should.eql([{ firstOffset: 10, lastOffset: 11, deliveryCount: 1 }]);
            });
        });

        it('should not take a corrupt legacy message (CRC mismatch) as decoded, in a share fetch or a fetch', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var raw = legacy([bigMessage(1)]), messageSet;
            raw[17] ^= 0x01; // attributes (after offset, size, crc, magic): gzip read as uncompressed, at its last offset
            messageSet = protocol.read(raw).MessageSet(null, raw.length).result;
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: messageSet, acquiredRecords: [{ firstOffset: 0, lastOffset: 1, deliveryCount: 1 }]
            }]).then(function (r) {
                offsets(r[0]).should.eql([]);
                r[0].acquiredRecords.should.eql([]); // offset 0 not GAP-acked: it is a real record, redelivered
                return fetch(0, raw);
            }).then(function (p) {
                offsets(p).should.eql([]);
                should.exist(p.error); // the consumer stops at it, as at a corrupt RecordBatch
            });
        });

        it('should take a legacy message with a corrupt key length as corrupt, not fail the whole fetch', function () {
            var raw = legacy([smallMessage(0), smallMessage(1), smallMessage(2)]);
            raw.writeInt32BE(1 << 20, 28 + 18); // message 1's key length (after offset, size, crc, magic, attributes)
            return fetch(0, raw).then(function (p) {
                offsets(p).should.eql([0]); // stops at it, as at a CRC mismatch
                should.not.exist(p.error);
                raw.writeInt32BE(3, 18); // message 0's, too short: its value length would be misread
                return fetch(0, raw);
            }).then(function (p) {
                offsets(p).should.eql([]);
                should.exist(p.error);
            });
        });

        it('should read magic-1 legacy messages, and take one with bytes past its value as corrupt', function () {
            function crc32(buf) { // CRC-32 (ISO 3309); zlib.crc32 needs Node 22
                var c = -1, i, k;
                for (i = 0; i < buf.length; i++) {
                    c ^= buf[i];
                    for (k = 0; k < 8; k++) { c = (c >>> 1) ^ (0xEDB88320 & -(c & 1)); }
                }
                return c ^ -1;
            }
            function magic1(offset, extra) { // null key, value 'ok', CRC valid; `extra` bytes after the value
                var body = Buffer.concat([Buffer.from([1, 0]), Buffer.alloc(8), Buffer.from([0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 2]), Buffer.from('ok')]);
                var item = Buffer.alloc(16 + body.length + extra);
                item.writeInt32BE(offset, 4);
                item.writeInt32BE(4 + body.length + extra, 8);
                item.writeInt32BE(crc32(body), 12);
                body.copy(item, 16);
                return item;
            }
            return fetch(0, Buffer.concat([magic1(0, 0), magic1(1, 0), magic1(2, 1)])).then(function (p) {
                offsets(p).should.eql([0, 1]);
                should.not.exist(p.error);
            });
        });

        it('should stop at a RecordBatch after legacy messages (a segment written across a format upgrade)', function () {
            // read as a legacy message, this timestamp makes the batch's value length 8 MB
            var batch = Buffer.from(protocol.write().RecordBatch({ baseOffset: 2, timestamp: 0x7FFFFFFF, records: [{ value: 'ok' }] }).result);
            return fetch(4, Buffer.concat([legacy([smallMessage(0), smallMessage(1)]), batch])).then(function (p) {
                offsets(p).should.eql([0, 1]); // the next fetch starts at the batch, and reads it as one
                should.not.exist(p.error);
            });
        });

        it('should make one Error, not one per message, for a fetch of corrupt legacy messages', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client, raw, i, msgs = [], messageSet, spy;
            for (i = 0; i < 2000; i++) { msgs.push(smallMessage(i)); }
            raw = legacy(msgs);
            for (i = 0; i < 2000; i++) { raw[28 * i + 26] ^= 0xFF; } // each message is 28 bytes, its value at 26: corrupt each
            spy = sinon.spy(global, 'Error');
            messageSet = protocol.read(raw).MessageSet(null, raw.length).result;
            spy.callCount.should.be.below(10); // marked while parsing (sinon makes one of its own), was one per message
            spy.restore();
            messageSet.every(function (m) { return m.message.crcError === true; }).should.equal(true);
            return c._decompressShareFetchResults([{ topic: 't', partition: 0, messageSet: messageSet, acquiredRecords: [{ firstOffset: 0, lastOffset: 1999 }] }])
            .then(function (r) {
                r[0].acquiredRecords.should.eql([]);
            });
        });

        it('should not trust a failed legacy wrapper\'s offset to start the next run', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var bad = { offset: 5, message: { magicByte: 1, attributes: { codec: 1, _raw: 1 }, value: Buffer.from('not gzip') } }; // offset corrupted down
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: [legacyWrapper(11, ['a', 'b']), bad, legacyWrapper(15, ['c', 'd'])],
                acquiredRecords: [{ firstOffset: 10, lastOffset: 15, deliveryCount: 1 }]
            }]).then(function (r) {
                r[0].acquiredRecords.should.eql([ // 12-13 (the failed wrapper's real records) still not acknowledgeable
                    { firstOffset: 10, lastOffset: 11, deliveryCount: 1 },
                    { firstOffset: 14, lastOffset: 15, deliveryCount: 1 }
                ]);
            });
        });

        it('should deliver the legacy share wrappers on either side of one that fails to decode', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var bad = { offset: 13, message: { magicByte: 1, attributes: { codec: 1, _raw: 1 }, value: Buffer.from('not gzip') } };
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: [legacyWrapper(11, ['a', 'b']), bad, legacyWrapper(15, ['c', 'd'])],
                acquiredRecords: [{ firstOffset: 10, lastOffset: 15, deliveryCount: 1 }]
            }]).then(function (r) {
                offsets(r[0]).should.eql([10, 11, 14, 15]); // not held back with the failed one
                r[0].acquiredRecords.should.eql([ // 12-13 (the failed wrapper's) left out: redelivered
                    { firstOffset: 10, lastOffset: 11, deliveryCount: 1 },
                    { firstOffset: 14, lastOffset: 15, deliveryCount: 1 }
                ]);
            });
        });

        it('should acknowledge nothing a share fetch acquired when no batch in it is complete', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var raw = smallBatch(0).slice(0, 40); // its only batch, cut by maxBytes
            var batches = protocol.read(raw).RecordBatch(null, raw.length).result;
            batches.should.have.length(0);
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: batches, acquiredRecords: [{ firstOffset: 0, lastOffset: 0, deliveryCount: 1 }]
            }]).then(function (r) {
                r[0].acquiredRecords.should.eql([]); // not gaps: unseen records, redelivered
            });
        });

        it('should leave a truncated trailing share batch out of acquiredRecords', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var raw = Buffer.concat([smallBatch(0), smallBatch(1).slice(0, 40)]); // cut by maxBytes
            var batches = protocol.read(raw).RecordBatch(null, raw.length).result;
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: batches, acquiredRecords: [{ firstOffset: 0, lastOffset: 1, deliveryCount: 1 }]
            }]).then(function (r) {
                offsets(r[0]).should.eql([0]);
                r[0].acquiredRecords.should.eql([{ firstOffset: 0, lastOffset: 0, deliveryCount: 1 }]);
            });
        });

        it('should keep compaction gaps between decoded share batches acknowledgeable', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var raw = Buffer.concat([smallBatch(3), smallBatch(5), smallBatch(9), bigBatch(10), smallBatch(14)]);
            var batches = protocol.read(raw).RecordBatch(null, raw.length).result;
            compression.maxOutputSize = 1024;
            return c._decompressShareFetchResults([{
                topic: 't', partition: 0, messageSet: batches, acquiredRecords: [{ firstOffset: 0, lastOffset: 14, deliveryCount: 1 }]
            }]).then(function (r) {
                offsets(r[0]).should.eql([3, 5, 9, 14]);
                // 0-2 and 4, 6-8 were compacted away: the handler may ack them. 10-13 hold
                // the batch that failed (its real span unknown): it may not
                r[0].acquiredRecords.should.eql([
                    { firstOffset: 0, lastOffset: 9, deliveryCount: 1 },
                    { firstOffset: 14, lastOffset: 14, deliveryCount: 1 }
                ]);
            });
        });

        it('should stay linear when a broker sends overlapping acquiredRecords', function () {
            var c = new Kafka.Producer({ logger: { logLevel: 0 } }).client;
            var raw = Buffer.concat([smallBatch(0), bigBatch(1), smallBatch(2)]), acquired = [], i, t;
            var batches = protocol.read(raw).RecordBatch(null, raw.length).result;
            for (i = 0; i < 100000; i++) { acquired.push({ firstOffset: 0, lastOffset: 2, deliveryCount: 1 }); }
            compression.maxOutputSize = 1024;
            t = Date.now();
            return c._decompressShareFetchResults([{ topic: 't', partition: 0, messageSet: batches, acquiredRecords: acquired }])
            .then(function (r) {
                r[0].acquiredRecords.length.should.equal(2); // before eql: diffing 200k entries hangs
                r[0].acquiredRecords.should.eql([
                    { firstOffset: 0, lastOffset: 0, deliveryCount: 1 },
                    { firstOffset: 2, lastOffset: 2, deliveryCount: 1 }
                ]);
                (Date.now() - t).should.be.below(1000);
            });
        });

        it('should stop at a batch that fails its CRC check, for that partition only', function () {
            var corrupt = smallBatch(1);
            corrupt[corrupt.length - 2] ^= 0xFF;
            return fetch(4, Buffer.concat([smallBatch(0), corrupt, smallBatch(2)])).then(function (p) {
                offsets(p).should.eql([0]);
                should.not.exist(p.error);
                return fetch(4, Buffer.concat([corrupt, smallBatch(2)]));
            }).then(function (p) {
                offsets(p).should.eql([]);
                p.error.message.should.match(/CRC/);
            });
        });

        it('should not skip a RecordBatch it cannot decompress', function () {
            compression.maxOutputSize = 1024;
            return fetch(4, Buffer.concat([bigBatch(0), smallBatch(1)])).then(function (p) {
                offsets(p).should.eql([]);
                should.exist(p.error);
                return fetch(4, Buffer.concat([smallBatch(0), bigBatch(1), smallBatch(2)]));
            }).then(function (p) {
                offsets(p).should.eql([0]);
                should.not.exist(p.error);
            });
        });
    });
});
