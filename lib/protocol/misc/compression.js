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

    decompress: function (buffer) {
        return Buffer.concat(Snappy._chunks(buffer).map(function (a) { return snappy.uncompressSync(a); }));
    },

    decompressAsync: function (buffer) {
        return Promise.all(Snappy._chunks(buffer).map(Snappy._uncompressAsync)).then(Buffer.concat);
    },

    compress: snappy.compressSync,

    compressAsync: _snappyPromise ? function (a) { return snappy.compress(a); } : function (a) { return util.promisify(snappy.compress)(a); }
} : undefined;

LZ4 = lz4 ? {
    decompress: function (buffer) {
        return lz4.decode(buffer);
    },

    decompressAsync: function (buffer) {
        return Promise.resolve(lz4.decode(buffer));
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
        return zstd.decompress(buffer);
    },

    decompressAsync: function (buffer) {
        return Promise.resolve(zstd.decompress(buffer));
    },

    compress: function (buffer, level) {
        return (level !== undefined && level >= 0) ? zstd.compress(buffer, level) : zstd.compress(buffer);
    },

    compressAsync: function (buffer, level) {
        return Promise.resolve((level !== undefined && level >= 0) ? zstd.compress(buffer, level) : zstd.compress(buffer));
    }
} : undefined;

Gzip = {
    decompress: zlib.gunzipSync,

    decompressAsync: function (a) { return util.promisify(zlib.gunzip)(a); },

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
    decompress: function (buffer, codec) {
        if (codec === 2) {
            if (!Snappy) {
                return Promise.reject(new Error('Snappy is not installed. Please install it to use this codec.'));
            }
            return Promise.resolve(Snappy.decompress(buffer));
        } else if (codec === 1 && typeof zlib.gunzipSync === 'function') {
            return Promise.resolve(Gzip.decompress(buffer));
        } else if (codec === 3) {
            if (!LZ4) {
                return Promise.reject(new Error('LZ4 is not installed. Please install it to use this codec.'));
            }
            return Promise.resolve(LZ4.decompress(buffer));
        } else if (codec === 4) {
            if (!Zstd) {
                return Promise.reject(new Error('Zstd is not installed. Please install it to use this codec.'));
            }
            return Promise.resolve(Zstd.decompress(buffer));
        }
        /* istanbul ignore next */
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    },
    decompressAsync: function (buffer, codec) {
        if (codec === 2) {
            if (!Snappy) {
                return Promise.reject(new Error('Snappy is not installed. Please install it to use this codec.'));
            }
            return Snappy.decompressAsync(buffer);
        } else if (codec === 1) {
            return Gzip.decompressAsync(buffer);
        } else if (codec === 3) {
            if (!LZ4) {
                return Promise.reject(new Error('LZ4 is not installed. Please install it to use this codec.'));
            }
            return LZ4.decompressAsync(buffer);
        } else if (codec === 4) {
            if (!Zstd) {
                return Promise.reject(new Error('Zstd is not installed. Please install it to use this codec.'));
            }
            return Zstd.decompressAsync(buffer);
        }
        /* istanbul ignore next */
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    },
    compress: function (buffer, codec, level) {
        if (codec === 2) {
            if (!Snappy) {
                return Promise.reject(new Error('Snappy is not installed. Please install it to use this codec.'));
            }
            return Promise.resolve(Snappy.compress(buffer));
        } else if (codec === 1 && typeof zlib.gzipSync === 'function') {
            return Promise.resolve(Gzip.compress(buffer, level));
        } else if (codec === 3) {
            if (!LZ4) {
                return Promise.reject(new Error('LZ4 is not installed. Please install it to use this codec.'));
            }
            return Promise.resolve(LZ4.compress(buffer, level));
        } else if (codec === 4) {
            if (!Zstd) {
                return Promise.reject(new Error('Zstd is not installed. Please install it to use this codec.'));
            }
            return Promise.resolve(Zstd.compress(buffer, level));
        }
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    },
    compressAsync: function (buffer, codec, level) {
        if (codec === 2) {
            if (!Snappy) {
                return Promise.reject(new Error('Snappy is not installed. Please install it to use this codec.'));
            }
            return Snappy.compressAsync(buffer);
        } else if (codec === 1) {
            return Gzip.compressAsync(buffer, level);
        } else if (codec === 3) {
            if (!LZ4) {
                return Promise.reject(new Error('LZ4 is not installed. Please install it to use this codec.'));
            }
            return LZ4.compressAsync(buffer, level);
        } else if (codec === 4) {
            if (!Zstd) {
                return Promise.reject(new Error('Zstd is not installed. Please install it to use this codec.'));
            }
            return Zstd.compressAsync(buffer, level);
        }
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    }
};
