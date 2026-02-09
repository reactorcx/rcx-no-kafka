'use strict';

var Protocol = require('./index');
var errors   = require('../errors');
var crc32    = require('buffer-crc32');
var crc32c   = require('./misc/crc32c');
var _        = require('lodash');

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
                value = new Buffer(_(value).toString(), 'utf8');
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
            value = new Buffer(_(value).toString(), 'utf8');
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

// compact string (KIP-482 flexible versions)
Protocol.define('compactString', {
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
            value = new Buffer(_(value).toString(), 'utf8');
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
            value = new Buffer(_(value).toString(), 'utf8');
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
            return new Buffer(0);
        }
        this.raw('value', n);
        return this.context.value;
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.UVarint(0);
        } else {
            if (!Buffer.isBuffer(value)) {
                value = new Buffer(_(value).toString(), 'utf8');
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
            return new Buffer(0);
        }
        this.raw('value', n);
        return this.context.value;
    },
    write: function (value) {
        if (value === undefined || value === null) {
            this.UVarint(0);
        } else {
            if (!Buffer.isBuffer(value)) {
                value = new Buffer(_(value).toString(), 'utf8');
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
        // var _o1 = this.offset;
        this
            .Int32BE('crc')
            .Int8('magicByte')
            .MessageAttributes('attributes')
            .bytes('key')
            .bytes('value');
        // crc32.signed(this.buffer.slice(_o1+4, this.offset));
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
        this.Int32BE(crc32.signed(this.buffer.slice(_o1 + 4, _o2)));
        this.offset = _o2;
    }
});

Protocol.define('MessageSetItem', {
    read: function (size, end) {
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

Protocol.define('RecordHeader', {
    read: function () {
        this.SVarint('keyLength');
        if (this.context.keyLength > 0) {
            this.raw('key', this.context.keyLength);
            this.context.key = this.context.key.toString('utf8');
        } else {
            this.context.key = '';
        }
        this.SVarint('valueLength');
        if (this.context.valueLength > 0) {
            this.raw('value', this.context.valueLength);
        } else {
            this.context.value = null;
        }
    },
    write: function (header) {
        var keyBuf = new Buffer(header.key || '', 'utf8');
        var valueBuf = header.value;
        if (valueBuf !== undefined && valueBuf !== null && !Buffer.isBuffer(valueBuf)) {
            valueBuf = new Buffer(String(valueBuf), 'utf8');
        }
        this.SVarint(keyBuf.length).raw(keyBuf);
        if (valueBuf === undefined || valueBuf === null) {
            this.SVarint(0);
        } else {
            this.SVarint(valueBuf.length).raw(valueBuf);
        }
    }
});

Protocol.define('Record', {
    read: function () {
        this
            .SVarint('length')
            .Int8('attributes')
            .SVarint64('timestampDelta')
            .SVarint('offsetDelta')
            .SVarint('keyLength');

        if (this.context.keyLength >= 0) {
            this.raw('key', this.context.keyLength);
        } else {
            this.context.key = null;
        }

        this.SVarint('valueLength');
        if (this.context.valueLength >= 0) {
            this.raw('value', this.context.valueLength);
        } else {
            this.context.value = null;
        }

        this.SVarint('headerCount');
        this.context.headers = [];
        if (this.context.headerCount > 0) {
            this.loop('headers', this.RecordHeader, this.context.headerCount);
        }
    },
    write: function (record) {
        // write record body first, then prepend varint length
        // reserve max 5 bytes for varint length, write body, then fix up
        var lengthOffset = this.offset;
        var bodyStart, bodyEnd, bodyLength, keyBuf, valBuf, headers, i;
        var zigzag, varintBytes, tmp, bodyBuf, newBodyStart;
        this.skip(5); // max varint size

        bodyStart = this.offset;
        this
            .Int8(record.attributes || 0)
            .SVarint64(record.timestampDelta || 0)
            .SVarint(record.offsetDelta || 0);

        // key
        if (record.key === undefined || record.key === null) {
            this.SVarint(-1);
        } else {
            keyBuf = Buffer.isBuffer(record.key) ? record.key : new Buffer(String(record.key), 'utf8');
            this.SVarint(keyBuf.length).raw(keyBuf);
        }

        // value
        if (record.value === undefined || record.value === null) {
            this.SVarint(-1);
        } else {
            valBuf = Buffer.isBuffer(record.value) ? record.value : new Buffer(String(record.value), 'utf8');
            this.SVarint(valBuf.length).raw(valBuf);
        }

        // headers
        headers = record.headers || [];
        this.SVarint(headers.length);
        for (i = 0; i < headers.length; i++) {
            this.RecordHeader(headers[i]);
        }

        bodyEnd = this.offset;
        bodyLength = bodyEnd - bodyStart;

        // encode the length as zigzag varint to figure out how many bytes it needs
        zigzag = bodyLength >= 0 ? bodyLength * 2 : ((-bodyLength) * 2 - 1);
        varintBytes = 0;
        tmp = zigzag;
        do { varintBytes++; tmp = tmp >>> 7; } while (tmp > 0);

        // shift body to be right after the varint length bytes
        bodyBuf = this.buffer.slice(bodyStart, bodyEnd);
        newBodyStart = lengthOffset + varintBytes;
        bodyBuf.copy(this.buffer, newBodyStart);
        this.offset = lengthOffset;
        this.SVarint(bodyLength);
        this.offset = newBodyStart + bodyLength;
    }
});

Protocol.define('RecordBatchHeader', {
    read: function () {
        this
            .KafkaOffset('baseOffset')
            .Int32BE('batchLength')
            .Int32BE('partitionLeaderEpoch')
            .Int8('magic')
            .UInt32BE('crc')
            .Int16BE('attributes')
            .Int32BE('lastOffsetDelta')
            .Int64BE('firstTimestamp')
            .Int64BE('maxTimestamp')
            .Int64BE('producerId')
            .Int16BE('producerEpoch')
            .Int32BE('baseSequence')
            .Int32BE('recordCount');

        // convert Long timestamps to numbers
        this.context.firstTimestamp = this.context.firstTimestamp.toNumber();
        this.context.maxTimestamp = this.context.maxTimestamp.toNumber();
        this.context.producerId = this.context.producerId.toNumber();
        this.context.codec = this.context.attributes & 0x7;
    }
});

Protocol.define('RecordBatch', {
    read: function (size) {
        var _o1 = this.offset;
        var results, remaining, header, recordsSize;
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

            this.RecordBatchHeader('_batchHeader');
            header = this.context._batchHeader;

            // records data size = batchLength - (header fields after batchLength = 49 bytes)
            recordsSize = header.batchLength - 49;

            // check for truncated batch: full batch = 12 + batchLength bytes
            if (recordsSize < 0 || (this.offset - _o1) + recordsSize > size) {
                this.skip(size - (this.offset - _o1));
                break;
            }

            if (recordsSize > 0) {
                this.raw('_recordsRaw', recordsSize);
            } else {
                this.context._recordsRaw = new Buffer(0);
            }

            results.push({
                header: _.assign({}, header),
                recordsRaw: this.context._recordsRaw
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

        // attributes: low 3 bits = codec
        attributes = codec & 0x7;
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
