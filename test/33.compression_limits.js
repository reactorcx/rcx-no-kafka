'use strict';

/* global describe, it, afterEach */

var compression = require('../lib/protocol/misc/compression');
var zlib        = require('zlib');
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
});
