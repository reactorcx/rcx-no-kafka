'use strict';

var requireSafe = function requireSafe(moduleName) {
    try {
        require.resolve(moduleName);
        return require(moduleName);
    } catch (err) {
        return undefined;
    }
};
var util    = require('util');
var snappy  = requireSafe('snappy');
var zlib    = require('zlib');

var SNAPPY_MAGIC_HEADER = Buffer.from([-126, 83, 78, 65, 80, 80, 89, 0]); // '\x82SNAPPY\00'

// Detect if snappy uses Promise-based API (v7+) or callback-based (v6)
var _snappyPromise = snappy && (function _detect() {
    try { return !!(snappy.compress(Buffer.from('t')).then); } catch (e) { return false; }
}());
/*var SNAPPY_BLOCK_SIZE = 32 * 1024;
var SNAPPY_DEFAULT_VERSION = 1;
var SNAPPY_MINIMUM_COMPATIBLE_VERSION = 1;*/

var lz4 = requireSafe('lz4');
var zstd = requireSafe('zstd-napi');
var zstdBinding = zstd && require('zstd-napi/binding');
var zstdDctx = zstdBinding && new zstdBinding.DCtx(); // shared: decompression is synchronous
var ZSTD_CHUNK = 128 * 1024;

var Snappy, Gzip, LZ4, Zstd;

// Compressed batch bytes come from the broker (and originally from any producer
// on the topic), so decompressed output must be bounded to prevent
// decompression-bomb memory exhaustion. Reads module.exports.maxOutputSize
// dynamically so it can be tuned at runtime.
function checkSize(declared) {
    if (declared > module.exports.maxOutputSize) {
        throw new Error('Decompressed size ' + declared + ' exceeds maxOutputSize of '
            + module.exports.maxOutputSize + ' bytes');
    }
}

// Runs fn inside a promise executor so synchronous throws become rejections
// (the decompress/compress API contract is "always returns a Promise").
function attempt(fn) {
    return new Promise(function (resolve) {
        resolve(fn());
    });
}

Snappy = snappy ? {
    _chunks: function (buffer) {
        var offset = 16, size, chunks = [];
        if (buffer.toString('hex', 0, 8) === SNAPPY_MAGIC_HEADER.toString('hex')) {
            // var defaultVersion = buffer.readUInt32BE(offset); offset += 4;
            // var minimumVersion = buffer.readUInt32BE(offset); offset += 4;
            while (offset < buffer.length) {
                size = buffer.readUInt32BE(offset); offset += 4;
                chunks.push(buffer.slice(offset, offset + size));
                offset += size;
            }
        } else {
            /* istanbul ignore next */
            chunks = [buffer];
        }
        return chunks;
    },

    _uncompressAsync: _snappyPromise ? function (a) { return snappy.uncompress(a); } : function (a) { return util.promisify(snappy.uncompress)(a); },

    // Raw snappy data starts with the uncompressed length as a uvarint —
    // checking it up front rejects bombs before any allocation happens.
    _declaredLength: function (chunk) {
        var len = 0, shift = 0, i = 0, b;
        do {
            b = chunk[i++];
            len += (b & 0x7F) * Math.pow(2, shift);
            shift += 7;
        } while (b >= 0x80 && i < chunk.length && i < 10);
        return len;
    },

    _checkedChunks: function (buffer) {
        var total = 0;
        return Snappy._chunks(buffer).map(function (a) {
            total += Snappy._declaredLength(a);
            checkSize(total);
            return a;
        });
    },

    decompress: function (buffer) {
        return Buffer.concat(Snappy._checkedChunks(buffer).map(function (a) { return snappy.uncompressSync(a); }));
    },

    decompressAsync: function (buffer) {
        return Promise.all(Snappy._checkedChunks(buffer).map(Snappy._uncompressAsync)).then(Buffer.concat);
    },

    compress: snappy.compressSync,

    compressAsync: _snappyPromise ? function (a) { return snappy.compress(a); } : function (a) { return util.promisify(snappy.compress)(a); }
} : undefined;

// An LZ4 frame (the format Kafka writes, one per batch), parsed here and each block decoded by
// the module's native decoder straight into output buffers of two blocks each, so output past
// maxOutputSize is rejected before it is all held. The module's stream decoder allocated a
// zero-filled buffer of the frame's block size (up to 4 MB) per block, so a batch of one-byte
// blocks held gigabytes; it looped forever on a skippable frame (Kafka never writes one:
// rejected here); and it reported a truncated frame by an event that crashed the process.
// Checksums are checked as it did
function lz4Decompress(buffer) {
    var chunks = [], out = null, used = 0, total = 0, pos = 0, flags, blockMax, start, size, block, n, sum;

    function take(len) {
        if (pos + len > buffer.length) {
            throw new Error('Unexpected end of LZ4 data');
        }
        pos += len;
        return buffer.subarray(pos - len, pos);
    }

    while (pos < buffer.length) {
        if (take(4).readUInt32LE(0) !== lz4.MAGICNUMBER) {
            throw new Error('Not an LZ4 frame');
        }
        start = pos;
        flags = take(2); // FLG, BD
        blockMax = lz4.blockMaxSizes[(flags[1] >> 4) & 0x7];
        if (flags[0] >> 6 !== 1 || (flags[0] & 0x02) || !blockMax) {
            throw new Error('Unsupported LZ4 frame descriptor');
        }
        take(((flags[0] & 0x08) ? 8 : 0) + ((flags[0] & 0x01) ? 4 : 0)); // content size, dictionary id: unused
        if (take(1)[0] !== lz4.utils.descriptorChecksum(buffer.subarray(start, pos - 1))) {
            throw new Error('Invalid LZ4 descriptor checksum');
        }
        sum = null;
        while ((size = take(4).readUInt32LE(0)) !== 0) { // 0: end mark
            if ((size & 0x7FFFFFFF) > blockMax) {
                throw new Error('Invalid LZ4 block size');
            }
            block = take(size & 0x7FFFFFFF);
            if ((flags[0] & 0x10) && take(4).readUInt32LE(0) !== lz4.utils.blockChecksum(block)) {
                throw new Error('Invalid LZ4 block checksum');
            }
            if (size & 0x80000000) { // stored uncompressed: kept as a slice of the input, copied once at the end
                if (out && used > 0) {
                    chunks.push(out.subarray(0, used));
                    out = out.subarray(used);
                    used = 0;
                }
                if (block.length > 0) { // an empty one would be kept for nothing, unbounded by the cap
                    chunks.push(block);
                }
                if (flags[0] & 0x04) {
                    sum = lz4.utils.streamChecksum(block, sum);
                }
                total += block.length;
                checkSize(total);
                continue;
            }
            if (!out || out.length - used < blockMax) {
                if (out && used > 0) {
                    chunks.push(out.subarray(0, used));
                }
                out = Buffer.allocUnsafe(2 * blockMax); // room for several small blocks: a buffer per block multiplied tiny ones
                used = 0;
            }
            n = lz4.decodeBlock(block, out.subarray(used, used + blockMax));
            if (n < 0) {
                throw new Error('Invalid LZ4 block');
            }
            if (flags[0] & 0x04) {
                sum = lz4.utils.streamChecksum(out.subarray(used, used + n), sum);
            }
            used += n;
            total += n;
            checkSize(total);
        }
        if ((flags[0] & 0x04) && take(4).readUInt32LE(0) !== lz4.utils.streamChecksum(null, sum || lz4.utils.streamChecksum(Buffer.alloc(0), null))) {
            throw new Error('Invalid LZ4 content checksum');
        }
    }
    if (out && used > 0) {
        chunks.push(out.subarray(0, used));
    }
    return Buffer.concat(chunks, total); // a copy: spare capacity isn't kept alive by the records
}

// Streamed, so a frame declaring or inflating to more than maxOutputSize is rejected before
// that much is allocated (a 32 KB frame can declare 1 GB). Output goes into one buffer at an
// offset, a new one only once it is full: each frame ends a decode call, and a buffer per call
// let a batch of tiny frames hold gigabytes. The decoder's window is still bounded only by
// zstd's default windowLogMax (128 MB), as in Java's client
function zstdDecompress(buffer) {
    var chunks = [], total = 0, used = 0, src = buffer, ret = 0, r, out;
    // a size in the frame header (zstd-napi's compress writes one; Java's streaming writer
    // doesn't) is checked up front and, under the cap, sizes the buffer
    var declared = zstdBinding.getFrameContentSize(buffer);
    if (declared !== null) {
        checkSize(declared);
    }
    // every buffer is bounded by what is left of the cap (plus a byte, to see it overflow): a frame
    // without a size, whose buffer follows the input's, must not decode past the cap first
    // a declared size (checked above) sizes it exactly, so a lone frame is returned without a copy
    out = Buffer.allocUnsafe(declared !== null ? declared : Math.min(module.exports.maxOutputSize + 1, Math.max(ZSTD_CHUNK, src.length)));
    // a frame's window (what it needs in memory) may be up to maxOutputSize, as the output may: zstd's
    // default limit (2^27) rejected valid frames when the cap was raised above 128 MB
    zstdDctx.setParameter(zstdBinding.DParameter.windowLogMax,
        Math.min(31, Math.max(27, Math.ceil(Math.log2(module.exports.maxOutputSize)))));
    try {
        for (;;) {
            if (used === out.length) {
                chunks.push(out);
                out = Buffer.allocUnsafe(Math.min(ZSTD_CHUNK, module.exports.maxOutputSize - total + 1));
                used = 0;
            }
            r = zstdDctx.decompressStream(out.subarray(used), src); // [hint (0 = frame done), produced, consumed]
            if (r[1] === 0 && r[2] === 0) {
                break; // nothing left to flush, or no progress
            }
            ret = r[0];
            src = src.subarray(r[2]);
            used += r[1];
            total += r[1];
            checkSize(total);
            if (src.length === 0 && (ret === 0 || used < out.length)) {
                break; // input used up, and the frame ended (or more output wasn't waiting)
            }
        }
    } finally {
        // an abandoned frame would otherwise corrupt the next decode
        zstdDctx.reset(zstdBinding.ResetDirective.sessionOnly);
    }
    if (ret !== 0 || src.length > 0) {
        throw new Error('Truncated or corrupt zstd data');
    }
    if (chunks.length === 0 && used === out.length) {
        return out;
    }
    chunks.push(out.subarray(0, used));
    return Buffer.concat(chunks, total); // a copy: spare capacity isn't kept alive by the records
}

LZ4 = lz4 ? {
    decompress: lz4Decompress,

    decompressAsync: function (buffer) {
        return attempt(function () { return lz4Decompress(buffer); });
    },

    compress: function (buffer, level) {
        var opts = (level !== undefined && level >= 0) ? { highCompression: true } : undefined;
        return lz4.encode(buffer, opts);
    },

    compressAsync: function (buffer, level) {
        var opts = (level !== undefined && level >= 0) ? { highCompression: true } : undefined;
        return Promise.resolve(lz4.encode(buffer, opts));
    }
} : undefined;

Zstd = zstd ? {
    decompress: zstdDecompress,

    decompressAsync: function (buffer) {
        return attempt(function () { return zstdDecompress(buffer); });
    },

    compress: function (buffer, level) {
        return (level !== undefined && level >= 0) ? zstd.compress(buffer, level) : zstd.compress(buffer);
    },

    compressAsync: function (buffer, level) {
        return Promise.resolve((level !== undefined && level >= 0) ? zstd.compress(buffer, level) : zstd.compress(buffer));
    }
} : undefined;

Gzip = {
    decompress: function (a) { return zlib.gunzipSync(a, { maxOutputLength: module.exports.maxOutputSize }); },

    decompressAsync: function (a) { return util.promisify(zlib.gunzip)(a, { maxOutputLength: module.exports.maxOutputSize }); },

    compress: function (buffer, level) {
        var opts = (level !== undefined && level >= 0) ? { level: level } : undefined;
        return zlib.gzipSync(buffer, opts);
    },

    compressAsync: function (buffer, level) {
        var opts = (level !== undefined && level >= 0) ? { level: level } : undefined;
        return util.promisify(zlib.gzip)(buffer, opts);
    },
};

module.exports = {
    // Upper bound for decompressed batch output (decompression-bomb protection).
    maxOutputSize: 128 * 1024 * 1024,

    decompress: function (buffer, codec) {
        if (codec === 2) {
            if (!Snappy) {
                return Promise.reject(new Error('Snappy is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return Snappy.decompress(buffer); });
        } else if (codec === 1 && typeof zlib.gunzipSync === 'function') {
            return attempt(function () { return Gzip.decompress(buffer); });
        } else if (codec === 3) {
            if (!LZ4) {
                return Promise.reject(new Error('LZ4 is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return LZ4.decompress(buffer); });
        } else if (codec === 4) {
            if (!Zstd) {
                return Promise.reject(new Error('Zstd is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return Zstd.decompress(buffer); });
        }
        /* istanbul ignore next */
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    },
    decompressAsync: function (buffer, codec) {
        if (codec === 2) {
            if (!Snappy) {
                return Promise.reject(new Error('Snappy is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return Snappy.decompressAsync(buffer); });
        } else if (codec === 1) {
            return attempt(function () { return Gzip.decompressAsync(buffer); });
        } else if (codec === 3) {
            if (!LZ4) {
                return Promise.reject(new Error('LZ4 is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return LZ4.decompressAsync(buffer); });
        } else if (codec === 4) {
            if (!Zstd) {
                return Promise.reject(new Error('Zstd is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return Zstd.decompressAsync(buffer); });
        }
        /* istanbul ignore next */
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    },
    compress: function (buffer, codec, level) {
        if (codec === 2) {
            if (!Snappy) {
                return Promise.reject(new Error('Snappy is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return Snappy.compress(buffer); });
        } else if (codec === 1 && typeof zlib.gzipSync === 'function') {
            return attempt(function () { return Gzip.compress(buffer, level); });
        } else if (codec === 3) {
            if (!LZ4) {
                return Promise.reject(new Error('LZ4 is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return LZ4.compress(buffer, level); });
        } else if (codec === 4) {
            if (!Zstd) {
                return Promise.reject(new Error('Zstd is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return Zstd.compress(buffer, level); });
        }
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    },
    compressAsync: function (buffer, codec, level) {
        if (codec === 2) {
            if (!Snappy) {
                return Promise.reject(new Error('Snappy is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return Snappy.compressAsync(buffer); });
        } else if (codec === 1) {
            return attempt(function () { return Gzip.compressAsync(buffer, level); });
        } else if (codec === 3) {
            if (!LZ4) {
                return Promise.reject(new Error('LZ4 is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return LZ4.compressAsync(buffer, level); });
        } else if (codec === 4) {
            if (!Zstd) {
                return Promise.reject(new Error('Zstd is not installed. Please install it to use this codec.'));
            }
            return attempt(function () { return Zstd.compressAsync(buffer, level); });
        }
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    }
};
