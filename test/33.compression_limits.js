'use strict';

/* global describe, it, afterEach, should */

var compression = require('../lib/protocol/misc/compression');
var zlib        = require('zlib');
var Protocol    = require('../lib/protocol');
var Kafka       = require('../lib/index');
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
