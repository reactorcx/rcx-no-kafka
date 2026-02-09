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
var _ = require('lodash');
var SNAPPY_MAGIC_HEADER = new Buffer([-126, 83, 78, 65, 80, 80, 89, 0]); // '\x82SNAPPY\00'

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

    _uncompressAsync: _snappyPromise ? _.ary(snappy.uncompress, 1) : _.ary(util.promisify(snappy.uncompress), 1),

    decompress: function (buffer) {
        return Buffer.concat(Snappy._chunks(buffer).map(_.ary(snappy.uncompressSync, 1)));
    },

    decompressAsync: function (buffer) {
        return Promise.all(Snappy._chunks(buffer).map(Snappy._uncompressAsync)).then(Buffer.concat);
    },

    compress: snappy.compressSync,

    compressAsync: _snappyPromise ? _.ary(snappy.compress, 1) : _.ary(util.promisify(snappy.compress), 1)
} : undefined;

LZ4 = lz4 ? {
    decompress: function (buffer) {
        return lz4.decode(buffer);
    },

    decompressAsync: function (buffer) {
        return Promise.resolve(lz4.decode(buffer));
    },

    compress: function (buffer) {
        return lz4.encode(buffer);
    },

    compressAsync: function (buffer) {
        return Promise.resolve(lz4.encode(buffer));
    }
} : undefined;

Zstd = zstd ? {
    decompress: function (buffer) {
        return zstd.decompress(buffer);
    },

    decompressAsync: function (buffer) {
        return Promise.resolve(zstd.decompress(buffer));
    },

    compress: function (buffer) {
        return zstd.compress(buffer);
    },

    compressAsync: function (buffer) {
        return Promise.resolve(zstd.compress(buffer));
    }
} : undefined;

Gzip = {
    decompress: zlib.gunzipSync,

    decompressAsync: _.ary(util.promisify(zlib.gunzip), 1),

    compress: zlib.gzipSync,

    compressAsync: _.ary(util.promisify(zlib.gzip), 1),
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
    compress: function (buffer, codec) {
        if (codec === 2) {
            if (!Snappy) {
                return Promise.reject(new Error('Snappy is not installed. Please install it to use this codec.'));
            }
            return Promise.resolve(Snappy.compress(buffer));
        } else if (codec === 1 && typeof zlib.gzipSync === 'function') {
            return Promise.resolve(Gzip.compress(buffer));
        } else if (codec === 3) {
            if (!LZ4) {
                return Promise.reject(new Error('LZ4 is not installed. Please install it to use this codec.'));
            }
            return Promise.resolve(LZ4.compress(buffer));
        } else if (codec === 4) {
            if (!Zstd) {
                return Promise.reject(new Error('Zstd is not installed. Please install it to use this codec.'));
            }
            return Promise.resolve(Zstd.compress(buffer));
        }
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    },
    compressAsync: function (buffer, codec) {
        if (codec === 2) {
            if (!Snappy) {
                return Promise.reject(new Error('Snappy is not installed. Please install it to use this codec.'));
            }
            return Snappy.compressAsync(buffer);
        } else if (codec === 1) {
            return Gzip.compressAsync(buffer);
        } else if (codec === 3) {
            if (!LZ4) {
                return Promise.reject(new Error('LZ4 is not installed. Please install it to use this codec.'));
            }
            return LZ4.compressAsync(buffer);
        } else if (codec === 4) {
            if (!Zstd) {
                return Promise.reject(new Error('Zstd is not installed. Please install it to use this codec.'));
            }
            return Zstd.compressAsync(buffer);
        }
        return Promise.reject(new Error('Unsupported compression codec ' + codec));
    }
};
