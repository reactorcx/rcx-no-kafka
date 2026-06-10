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

function checkOutput(buffer) {
    checkSize(buffer.length);
    return buffer;
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

LZ4 = lz4 ? {
    decompress: function (buffer) {
        return checkOutput(lz4.decode(buffer));
    },

    decompressAsync: function (buffer) {
        return attempt(function () { return checkOutput(lz4.decode(buffer)); });
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
    decompress: function (buffer) {
        return checkOutput(zstd.decompress(buffer));
    },

    decompressAsync: function (buffer) {
        return attempt(function () { return checkOutput(zstd.decompress(buffer)); });
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
