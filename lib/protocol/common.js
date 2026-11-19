'use strict';

var Protocol = require('./index');
var errors   = require('../errors');
var crc32c   = require('./misc/crc32c');
var zlib     = require('zlib');

// CRC-32 (ISO 3309) — use native zlib.crc32 (Node 22+) or pure-JS fallback
var crc32iso = (typeof zlib.crc32 === 'function') ? function (buf) { return zlib.crc32(buf); } : (function () {
    var TABLE = new Int32Array(256);
    var i, j, c;
    for (i = 0; i < 256; i++) {
        c = i;
        for (j = 0; j < 8; j++) {
            c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
        }
        TABLE[i] = c;
    }
    return function (buf) {
        var crc = -1, k;
        for (k = 0; k < buf.length; k++) {
            crc = TABLE[(crc ^ buf[k]) & 0xFF] ^ (crc >>> 8);
        }
        return (crc ^ -1);
    };
}());

/* jshint bitwise: false */

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

//////////////////////////
// PRIMITIVE DATA TYPES //
//////////////////////////

// variable length primitives, bytes
Protocol.define('bytes', {
    read: function () {
        this.Int32BE('length');
        if (this.context.length < 0) {
            return null;
        }
        this.raw('value', this.context.length);
        return this.context.value;
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.Int32BE(-1);
        } else {
            if (!Buffer.isBuffer(value)) {
                value = Buffer.from(String(value), 'utf8');
            }
            this
                .Int32BE(value.length)
                .raw(value);
        }
    }
});

// variable length primitives, string
Protocol.define('string', {
    read: function () {
        this.Int16BE('length');
        if (this.context.length < 0) {
            return null;
        }
        this.raw('value', this.context.length);
        return this.context.value.toString('utf8');
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.Int16BE(-1);
        } else {
            value = Buffer.from(String(value), 'utf8');
            this
                .Int16BE(value.length)
                .raw(value);
        }
    }
});

// array
Protocol.define('array', {
    read: function (fn) {
        this.Int32BE('length');
        if (this.context.length < 0) {
            return null;
        } else if (this.context.length === 0) {
            return [];
        }
        this.loop('items', fn, this.context.length);
        return this.context.items;
    },
    write: function (value, fn) {
        if (value === null || value === undefined) {
            this.Int32BE(-1);
        } else {
            this
                .Int32BE(value.length)
                .loop(value, fn);
        }
    }
});

// compact string (KIP-482 flexible versions) — non-nullable
Protocol.define('compactString', {
    read: function () {
        var n;
        this.UVarint('length');
        if (this.context.length === 0) {
            // Non-nullable compactString should not receive UVarint(0) from broker.
            // Return empty string as defensive fallback.
            return '';
        }
        n = this.context.length - 1;
        if (n === 0) {
            return '';
        }
        this.raw('value', n);
        return this.context.value.toString('utf8');
    },
    write: function (value) {
        if (value === undefined || value === null) {
            // non-nullable: encode as empty string rather than null
            this.UVarint(1);
        } else {
            value = Buffer.from(String(value), 'utf8');
            this
                .UVarint(value.length + 1)
                .raw(value);
        }
    }
});

// compact nullable string (identical wire format, semantic distinction)
Protocol.define('compactNullableString', {
    read: function () {
        var n;
        this.UVarint('length');
        if (this.context.length === 0) {
            return null;
        }
        n = this.context.length - 1;
        if (n === 0) {
            return '';
        }
        this.raw('value', n);
        return this.context.value.toString('utf8');
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.UVarint(0);
        } else {
            value = Buffer.from(String(value), 'utf8');
            this
                .UVarint(value.length + 1)
                .raw(value);
        }
    }
});

// compact bytes (KIP-482 flexible versions)
Protocol.define('compactBytes', {
    read: function () {
        var n;
        this.UVarint('length');
        if (this.context.length === 0) {
            return null;
        }
        n = this.context.length - 1;
        if (n === 0) {
            return Buffer.alloc(0);
        }
        this.raw('value', n);
        return this.context.value;
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.UVarint(0);
        } else {
            if (!Buffer.isBuffer(value)) {
                value = Buffer.from(String(value), 'utf8');
            }
            this
                .UVarint(value.length + 1)
                .raw(value);
        }
    }
});

// compact nullable bytes (identical wire format, semantic distinction)
Protocol.define('compactNullableBytes', {
    read: function () {
        var n;
        this.UVarint('length');
        if (this.context.length === 0) {
            return null;
        }
        n = this.context.length - 1;
        if (n === 0) {
            return Buffer.alloc(0);
        }
        this.raw('value', n);
        return this.context.value;
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.UVarint(0);
        } else {
            if (!Buffer.isBuffer(value)) {
                value = Buffer.from(String(value), 'utf8');
            }
            this
                .UVarint(value.length + 1)
                .raw(value);
        }
    }
});

// compact array (KIP-482 flexible versions)
Protocol.define('compactArray', {
    read: function (fn) {
        var n;
        this.UVarint('length');
        if (this.context.length === 0) {
            return null;
        }
        n = this.context.length - 1;
        if (n === 0) {
            return [];
        }
        this.loop('items', fn, n);
        return this.context.items;
    },
    write: function (value, fn) {
        if (value === null || value === undefined) {
            this.UVarint(0);
        } else {
            this
                .UVarint(value.length + 1)
                .loop(value, fn);
        }
    }
});

// compact nullable array (identical wire format, semantic distinction)
Protocol.define('compactNullableArray', {
    read: function (fn) {
        var n;
        this.UVarint('length');
        if (this.context.length === 0) {
            return null;
        }
        n = this.context.length - 1;
        if (n === 0) {
            return [];
        }
        this.loop('items', fn, n);
        return this.context.items;
    },
    write: function (value, fn) {
        if (value === null || value === undefined) {
            this.UVarint(0);
        } else {
            this
                .UVarint(value.length + 1)
                .loop(value, fn);
        }
    }
});

// tagged fields (KIP-482 flexible versions)
Protocol.define('TaggedFields', {
    read: function () {
        var i;
        this.UVarint('tagCount');
        for (i = 0; i < this.context.tagCount; i++) {
            this.UVarint('_tag');
            this.UVarint('_size');
            this.skip(this.context._size);
        }
    },
    write: function () {
        this.UVarint(0);
    }
});

// uuid — 128-bit UUID, 16 bytes on wire, standard string format in JS (KIP-516)
Protocol.define('uuid', {
    read: function () {
        var hex;
        this.raw('value', 16);
        hex = this.context.value.toString('hex');
        if (hex === '00000000000000000000000000000000') {
            return null;
        }
        return hex.slice(0, 8) + '-' + hex.slice(8, 12) + '-' + hex.slice(12, 16) + '-' + hex.slice(16, 20) + '-' + hex.slice(20);
    },
    write: function (value) {
        var buf;
        if (value === null || value === undefined) {
            buf = Buffer.alloc(16);
        } else {
            buf = Buffer.from(String(value).replace(/-/g, ''), 'hex');
            if (buf.length !== 16) {
                // a short raw write would silently corrupt the whole request frame
                throw new Error('Invalid UUID value: ' + value);
            }
        }
        this.raw(buf);
    }
});

// return Error instance
Protocol.define('ErrorCode', {
    read: function () {
        this.Int16BE('error');
        return errors.byCode(this.context.error);
    }
});

// 64 bit offset, convert from Long (https://github.com/dcodeIO/long.js) to double, 53 bit
Protocol.define('KafkaOffset', {
    read: function () {
        this.Int64BE('offset');
        return this.context.offset.toNumber();
    },
    write: function (value) {
        this.Int64BE(value);
    }
});

Protocol.define('RequestHeader', {
    write: function (header) {
        this
            .Int16BE(header.apiKey)
            .Int16BE(header.apiVersion)
            .Int32BE(header.correlationId)
            .string(header.clientId);
    }
});

// flexible request header v2 (KIP-482)
// NOTE: clientId stays as regular nullable string (Int16 prefix) — NOT compact.
// The Kafka RequestHeader.json spec has flexibleVersions: "none" for the ClientId field.
// Header v2 only adds TaggedFields at the end compared to header v1.
Protocol.define('FlexibleRequestHeader', {
    write: function (header) {
        this
            .Int16BE(header.apiKey)
            .Int16BE(header.apiVersion)
            .Int32BE(header.correlationId)
            .string(header.clientId)
            .TaggedFields();
    }
});

////////////////////////////
// MESSAGE AND MESSAGESET //
////////////////////////////

Protocol.define('MessageAttributes', {
    read: function () {
        this.Int8('_raw');
        this.context.codec = this.context._raw & 0x7;
    },
    write: function (attributes) {
        this.Int8((attributes.codec || 0) & 0x7);
    }
});

Protocol.define('Message', {
    read: function () {
        var start = this.offset;
        this
            .Int32BE('crc')
            .Int8('magicByte')
            .MessageAttributes('attributes');
        if (this.context.magicByte >= 1) {
            // magic 1 (Kafka 0.10 log format) carries an Int64 timestamp
            // between attributes and key
            this.Int64BE('timestamp');
            this.context.timestamp = this.context.timestamp.toNumber();
        }
        this
            .bytes('key')
            .bytes('value');
        // CRC-32 over magic..value, as for RecordBatches (CRC-32C): a corrupt message (a flipped
        // codec bit, say) must not be taken as decoded, its offsets as gaps
        // marked, not an Error per message (a corrupt response may hold many): its reader makes one
        if ((crc32iso(this.buffer.slice(start + 4, this.offset)) | 0) !== this.context.crc) {
            this.context.crcError = true;
        }
    },
    write: function (value) { // {attributes, magicByte, key, value}
        var _o1 = this.offset, _o2;
        this
            .skip(4)
            .Int8(value.magicByte || 0)
            .MessageAttributes(value.attributes || {})
            .bytes(value.key)
            .bytes(value.value);

        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(crc32iso(this.buffer.slice(_o1 + 4, _o2)) | 0);
        this.offset = _o2;
    }
});

Protocol.define('MessageSetItem', {
    read: function (size, end) {
        var start, at, i;
        if (size < 8 + 4) {
            this.skip(size);
            return end();
        }
        this
            .KafkaOffset('offset')
            .Int32BE('messageSize');

        if (size < 8 + 4 + this.context.messageSize) {
            this.skip(size - 8 - 4);
            this.context._partial = true;
            return end();
        }

        // key and value lengths must fill messageSize exactly; a corrupt length, or a RecordBatch
        // after legacy entries, is marked crcError and skipped, as a CRC mismatch is
        start = this.offset;
        at = start + 6 + (this.buffer[start + 4] >= 1 ? 8 : 0);
        for (i = 0; i < 2 && at + 4 <= start + this.context.messageSize; i++) {
            at += 4 + Math.max(this.buffer.readInt32BE(at), 0);
        }
        if (i < 2 || at !== start + this.context.messageSize || this.buffer[start + 4] > 1) {
            this.skip(Math.max(this.context.messageSize, 0)); // negative: every entry after it is corrupt too
            this.context.message = { crcError: true, attributes: {} };
            return undefined;
        }

        this.Message('message');

        return undefined;
    },
    write: function (value) { // {offset, message}
        var _o1, _o2;
        this.KafkaOffset(value.offset);
        _o1 = this.offset;
        this
            .skip(4)
            .Message(value.message);
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

Protocol.define('MessageSet', {
    read: function (size) {
        var _o1 = this.offset;
        if (!size) {
            return [];
        }
        this.loop('items', function (end) {
            this.MessageSetItem(null, size - this.offset + _o1, end);
        });

        return this.context.items;
    },
    write: function (items) {
        this.loop(items, this.MessageSetItem);
    }
});

///////////////////////////////////
// RECORD BATCH v2 (Kafka 0.11+) //
///////////////////////////////////

// Record.write helpers: a zigzag varint of an int32, and a length-prefixed field (null: length -1)
function _zigzag(n) {
    return ((n << 1) ^ (n >> 31)) >>> 0;
}

function _varintSize(v) {
    var n = 1;
    while (v > 0x7F) {
        v >>>= 7;
        n++;
    }
    return n;
}

function _putVarint(b, o, v) {
    while (v > 0x7F) {
        b[o++] = (v & 0x7F) | 0x80;
        v >>>= 7;
    }
    b[o++] = v;
    return o;
}

function _bytesOrNull(v) {
    if (v === undefined || v === null) { return null; }
    return Buffer.isBuffer(v) ? v : Buffer.from(String(v), 'utf8');
}

function _fieldSize(buf) {
    return buf === null ? 1 : _varintSize(_zigzag(buf.length)) + buf.length;
}

function _putField(b, o, buf) {
    if (buf === null) { return _putVarint(b, o, 1); }
    o = _putVarint(b, o, _zigzag(buf.length));
    return o + buf.copy(b, o);
}

// Record.read and RecordBatch helpers, reading the buffer directly: bin-protocol's path lookups
// cost ~2 us a record and ~8 us a batch header. Values and errors are its SVarint's and raw's:
// a truncated record throws
function _readSVarint(rd) {
    var buf = rd.buffer, v = 0, shift = 0, b;
    do {
        b = buf[rd.offset++];
        if (b === undefined) { throw new RangeError('ProtocolBuffer: Truncated message'); }
        if (shift < 35) { v |= (b & 0x7F) << shift; }
        shift += 7;
        if (shift > 70) { throw new Error('ProtocolBuffer: Malformed varint'); }
    } while (b & 0x80);
    v >>>= 0;
    return (v >>> 1) ^ -(v & 1);
}

// SVarint64 as a Number (Long#toNumber): up to 7 bytes are exact in a double, longer ones go
// through bin-protocol's SVarint64
function _readSVarint64(rd) {
    var buf = rd.buffer, start = rd.offset, v = 0, mul = 1, b, n;
    for (n = 0; n < 7; n++) {
        b = buf[rd.offset++];
        if (b === undefined) { throw new RangeError('ProtocolBuffer: Truncated message'); }
        v += (b & 0x7F) * mul;
        if (!(b & 0x80)) { return (v % 2) ? -(v + 1) / 2 : v / 2; }
        mul *= 128;
    }
    rd.offset = start;
    return rd.SVarint64().context.toNumber();
}

function _readBytes(rd, n) {
    var out;
    rd.demand(n);
    out = Buffer.allocUnsafe(n); // every byte copied over
    rd.buffer.copy(out, 0, rd.offset, rd.offset + n);
    rd.offset += n;
    return out;
}

function _readInt64(b, o) { // as Long#toNumber
    return b.readInt32BE(o) * 4294967296 + b.readUInt32BE(o + 4);
}

function _readBatchHeader(rd) { // the 61 bytes before a batch's records
    var b = rd.buffer, o = rd.offset, h;
    h = {
        baseOffset: _readInt64(b, o),
        batchLength: b.readInt32BE(o + 8),
        partitionLeaderEpoch: b.readInt32BE(o + 12),
        magic: b.readInt8(o + 16),
        crc: b.readUInt32BE(o + 17),
        attributes: b.readInt16BE(o + 21),
        lastOffsetDelta: b.readInt32BE(o + 23),
        firstTimestamp: _readInt64(b, o + 27),
        maxTimestamp: _readInt64(b, o + 35),
        producerId: _readInt64(b, o + 43),
        producerEpoch: b.readInt16BE(o + 51),
        baseSequence: b.readInt32BE(o + 53),
        recordCount: b.readInt32BE(o + 57)
    };
    h.codec = h.attributes & 0x7;
    rd.offset += 61;
    return h;
}

Protocol.define('Record', {
    read: function () {
        var r = { length: _readSVarint(this), attributes: this.buffer.readInt8(this.offset++) }, h, i;
        r.timestampDelta = _readSVarint64(this);
        r.offsetDelta = _readSVarint(this);
        r.keyLength = _readSVarint(this);
        r.key = r.keyLength >= 0 ? _readBytes(this, r.keyLength) : null;
        r.valueLength = _readSVarint(this);
        r.value = r.valueLength >= 0 ? _readBytes(this, r.valueLength) : null;
        r.headerCount = _readSVarint(this);
        r.headers = [];
        for (i = 0; i < r.headerCount; i++) { // a header's key is a string, '' if empty or null
            h = { keyLength: _readSVarint(this) };
            h.key = h.keyLength > 0 ? _readBytes(this, h.keyLength).toString('utf8') : '';
            h.valueLength = _readSVarint(this);
            h.value = h.valueLength >= 0 ? _readBytes(this, h.valueLength) : null;
            r.headers.push(h);
        }
        return r;
    },
    write: function (record) {
        // the body's length is computed first, so it is written once: no copy to fit its varint
        // length in front. Varints as bin-protocol's SVarint (zigzag); a timestampDelta outside
        // int32 goes through its SVarint64
        var ts = record.timestampDelta || 0, od = record.offsetDelta || 0, headers = record.headers || [];
        var key = _bytesOrNull(record.key), value = _bytesOrNull(record.value);
        var hk = [], hv = [], len, tsBytes, b, o, i, start;
        if (ts !== (ts | 0)) {
            start = this.offset;
            this.SVarint64(ts);
            tsBytes = Buffer.from(this.buffer.slice(start, this.offset));
            this.offset = start;
        }
        len = 1 + (tsBytes ? tsBytes.length : _varintSize(_zigzag(ts))) + _varintSize(_zigzag(od)) +
            _fieldSize(key) + _fieldSize(value) + _varintSize(_zigzag(headers.length));
        for (i = 0; i < headers.length; i++) {
            hk[i] = Buffer.from(headers[i].key || '', 'utf8');
            hv[i] = _bytesOrNull(headers[i].value);
            len += _fieldSize(hk[i]) + _fieldSize(hv[i]);
        }
        this.demand(5 + len);
        b = this.buffer;
        o = _putVarint(b, this.offset, _zigzag(len));
        o = b.writeInt8(record.attributes || 0, o);
        o = tsBytes ? o + tsBytes.copy(b, o) : _putVarint(b, o, _zigzag(ts));
        o = _putVarint(b, o, _zigzag(od));
        o = _putField(b, o, key);
        o = _putField(b, o, value);
        o = _putVarint(b, o, _zigzag(headers.length));
        for (i = 0; i < headers.length; i++) {
            o = _putField(b, _putField(b, o, hk[i]), hv[i]);
        }
        this.offset = o;
    }
});

Protocol.define('RecordBatch', {
    read: function (size) {
        var _o1 = this.offset;
        var results, remaining, header, recordsSize, recordsRaw, _batchStart, crcComputed, crcError;
        if (!size) {
            return [];
        }

        results = [];

        while (this.offset - _o1 < size) {
            remaining = size - (this.offset - _o1);
            // need at least baseOffset(8) + batchLength(4) + full header (61 bytes total)
            if (remaining < 61) {
                this.skip(remaining);
                break;
            }

            _batchStart = this.offset;
            header = _readBatchHeader(this);

            // records data size = batchLength - (header fields after batchLength = 49 bytes)
            recordsSize = header.batchLength - 49;

            // check for truncated batch: full batch = 12 + batchLength bytes
            if (recordsSize < 0 || (this.offset - _o1) + recordsSize > size) {
                this.skip(size - (this.offset - _o1));
                break;
            }

            recordsRaw = _readBytes(this, recordsSize);

            // verify CRC-32C over attributes..end (the crc field ends 21 bytes
            // into the batch); corrupted batches must not reach application code.
            // Marked, not thrown: the fetch stops at it for this partition only.
            // One Error per read, so thousands of corrupt batches cost one stack trace
            crcComputed = crc32c(this.buffer.slice(_batchStart + 21, this.offset));
            if (crcComputed !== header.crc && !crcError) {
                crcError = new Error('Corrupt record batch (CRC32C mismatch) at baseOffset ' + header.baseOffset);
            }

            results.push({
                header: header,
                recordsRaw: recordsRaw,
                error: crcComputed !== header.crc ? crcError : undefined
            });
        }

        return results;
    },
    write: function (data) { // { baseOffset, records, codec, timestamp, producerId, producerEpoch, baseSequence, compressedRecords }
        var self = this;
        var records = data.records || [];
        var codec = data.codec || 0;
        var baseOffset = data.baseOffset || 0;
        var preCompressed = !!data.compressedRecords;
        var firstTimestamp = preCompressed ? (data.firstTimestamp || Date.now()) : (data.timestamp || Date.now());
        var maxTimestamp = preCompressed ? (data.maxTimestamp || firstTimestamp) : firstTimestamp;
        var recordCount = preCompressed ? (data.recordCount || 0) : records.length;
        var lastOffsetDelta = preCompressed ? (data.lastOffsetDelta || 0) : 0;
        var batchLengthOffset, crcOffset, crcStartOffset, attributes, lastOffsetDeltaOffset;
        var i, ts, batchEnd, crcData, crcValue;

        // serialize all records into the buffer after the header, then fix up lengths/CRC
        self.KafkaOffset(baseOffset);

        // placeholder for batchLength
        batchLengthOffset = self.offset;
        self.skip(4);

        self.Int32BE(data.partitionLeaderEpoch || 0); // partitionLeaderEpoch
        self.Int8(2); // magic = 2

        // CRC placeholder
        crcOffset = self.offset;
        self.skip(4);

        crcStartOffset = self.offset;

        // attributes: low 3 bits = codec, bit 4 = isTransactional
        attributes = codec & 0x7;
        if (data.isTransactional) {
            attributes |= 0x10;
        }
        self.Int16BE(attributes);

        // lastOffsetDelta placeholder
        lastOffsetDeltaOffset = self.offset;
        self.skip(4);

        self
            .Int64BE(firstTimestamp)
            .Int64BE(maxTimestamp) // placeholder for non-compressed, final for pre-compressed
            .Int64BE(data.producerId !== null && data.producerId !== undefined ? data.producerId : -1)
            .Int16BE(data.producerEpoch !== null && data.producerEpoch !== undefined ? data.producerEpoch : -1)
            .Int32BE(data.baseSequence !== null && data.baseSequence !== undefined ? data.baseSequence : -1)
            .Int32BE(recordCount);

        if (preCompressed) {
            // write pre-compressed records data directly
            self.raw(data.compressedRecords);
        } else {
            // write individual records inline
            for (i = 0; i < records.length; i++) {
                ts = records[i].timestamp || firstTimestamp;
                if (ts > maxTimestamp) { maxTimestamp = ts; }
                self.Record({
                    attributes: 0,
                    timestampDelta: ts - firstTimestamp,
                    offsetDelta: i,
                    key: records[i].key,
                    value: records[i].value,
                    headers: records[i].headers || []
                });
            }
            lastOffsetDelta = records.length > 0 ? records.length - 1 : 0;
        }

        batchEnd = self.offset;

        // fix lastOffsetDelta
        self.offset = lastOffsetDeltaOffset;
        self.Int32BE(lastOffsetDelta);

        // fix maxTimestamp (at crcStartOffset + 2(attributes) + 4(lastOffsetDelta) + 8(firstTimestamp))
        self.offset = crcStartOffset + 2 + 4 + 8;
        self.Int64BE(maxTimestamp);

        // batchLength = everything after batchLength field
        self.offset = batchLengthOffset;
        self.Int32BE(batchEnd - batchLengthOffset - 4);

        // compute CRC-32C over everything from attributes to end
        crcData = self.buffer.slice(crcStartOffset, batchEnd);
        crcValue = crc32c(crcData);
        self.offset = crcOffset;
        self.UInt32BE(crcValue);

        self.offset = batchEnd;
    }
});
