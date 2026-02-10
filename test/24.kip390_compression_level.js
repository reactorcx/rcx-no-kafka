'use strict';

/* global describe, it */

var compression = require('../lib/protocol/misc/compression');

describe('KIP-390: Compression Level Support', function () {
    var testBuf = Buffer.alloc(4096);
    var i;

    // fill with repetitive but non-trivial data for compressibility
    for (i = 0; i < testBuf.length; i++) {
        testBuf[i] = i % 128;
    }

    describe('Gzip', function () {
        it('should compress with default level when level is -1', function () {
            return compression.compress(testBuf, 1, -1).then(function (compressed) {
                compressed.length.should.be.greaterThan(0);
                compressed.length.should.be.lessThan(testBuf.length);
            });
        });

        it('should compress with default level when level is undefined', function () {
            return compression.compress(testBuf, 1).then(function (compressed) {
                compressed.length.should.be.greaterThan(0);
                compressed.length.should.be.lessThan(testBuf.length);
            });
        });

        it('level 1 (fastest) should produce larger output than level 9 (best)', function () {
            var level1Size, level9Size;

            return compression.compress(testBuf, 1, 1).then(function (compressed) {
                level1Size = compressed.length;
                return compression.compress(testBuf, 1, 9);
            }).then(function (compressed) {
                level9Size = compressed.length;
                level1Size.should.be.greaterThan(level9Size);
            });
        });

        it('async should respect compression level', function () {
            var syncSize;

            return compression.compress(testBuf, 1, 1).then(function (compressed) {
                syncSize = compressed.length;
                return compression.compressAsync(testBuf, 1, 1);
            }).then(function (compressed) {
                compressed.length.should.equal(syncSize);
            });
        });

        it('compressed data should decompress correctly regardless of level', function () {
            return compression.compress(testBuf, 1, 9).then(function (compressed) {
                return compression.decompress(compressed, 1);
            }).then(function (decompressed) {
                Buffer.compare(decompressed, testBuf).should.equal(0);
            });
        });
    });

    describe('Snappy', function () {
        it('should ignore level parameter (no level support)', function () {
            return compression.compress(testBuf, 2, 5).then(function (compressed) {
                compressed.length.should.be.greaterThan(0);
                return compression.decompress(compressed, 2);
            }).then(function (decompressed) {
                Buffer.compare(decompressed, testBuf).should.equal(0);
            });
        });
    });
});
