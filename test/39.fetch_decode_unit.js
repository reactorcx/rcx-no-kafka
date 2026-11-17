'use strict';

/* global describe, it, should */

var Protocol    = require('../lib/protocol');
var Client      = require('../lib/client');
var crc32c      = require('../lib/protocol/misc/crc32c');
var compression = require('../lib/protocol/misc/compression');
var zlib        = require('zlib');

var protocol = new Protocol({ bufferSize: 262144 });

function records(n) {
    var r = [], i;
    for (i = 0; i < n; i++) { r.push({ value: 'v' + i }); }
    return r;
}

// o: { base, recs, pid, txn, attrs, lastDelta, raw }
function batch(o) {
    var b = Buffer.from(protocol.write().RecordBatch({
        baseOffset: o.base, records: o.recs, timestamp: 1e12,
        producerId: o.pid === undefined ? -1 : o.pid, producerEpoch: 0, baseSequence: 0, isTransactional: o.txn
    }).result);
    if (o.attrs !== undefined) { b.writeInt16BE(o.attrs, 21); }
    if (o.lastDelta !== undefined) { b.writeInt32BE(o.lastDelta, 23); }
    if (o.raw) { b = Buffer.concat([b.slice(0, 61), o.raw]); b.writeInt32BE(b.length - 12, 8); }
    b.writeUInt32BE(crc32c(b.slice(21)), 17);
    return b;
}

// a control batch holding one marker: key = version(0) + type (0 abort, 1 commit)
function marker(base, pid, type) {
    var key = Buffer.alloc(4);
    key.writeInt16BE(type, 2);
    return batch({ base: base, recs: [{ key: key, value: Buffer.alloc(6) }], pid: pid, txn: true, attrs: 0x30 });
}

// FetchResponse v4 for topic 't' partition 0
function response(batches, aborted) {
    var w = protocol.write().Int32BE(1).Int32BE(0).Int32BE(1).string('t').Int32BE(1)
        .Int32BE(0).Int16BE(0).Int64BE(1000).Int64BE(1000).Int32BE((aborted || []).length);
    (aborted || []).forEach(function (a) { w.Int64BE(a[0]).Int64BE(a[1]); });
    return Buffer.from(w.Int32BE(Buffer.concat(batches).length).raw(Buffer.concat(batches)).result);
}

function fetch(buf, offset) {
    var c = new Client({ connectionString: '127.0.0.1:1', logger: { logLevel: 0 } });
    c.brokerConnections = { 0: { apiVersions: { 1: { min: 4, max: 4 } }, send: function () { return Promise.resolve(buf); } } };
    return c.fetchRequest({ 0: [{ topicName: 't', partitions: [{ partition: 0, offset: offset, maxBytes: 1 << 20 }] }] })
    .then(function (r) { return r[0]; });
}

function offsets(p) {
    return p.messageSet.map(function (m) { return m.offset; });
}

describe('Fetch RecordBatch decoding (unit)', function () {
    it('should stop at a batch that fails to decode instead of skipping it', function () {
        var garbage = Buffer.from([0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f]);
        return fetch(response([
            batch({ base: 100, recs: records(2) }),
            batch({ base: 102, recs: records(2), raw: garbage }),
            batch({ base: 104, recs: records(2) })
        ]), 100).then(function (p) {
            offsets(p).should.eql([100, 101]);
            p.nextOffset.should.equal(102);
            should.not.exist(p.error);
        });
    });

    it('should report the failure as the partition error when the first batch fails', function () {
        return fetch(response([
            batch({ base: 102, recs: records(2), raw: Buffer.from([0x7f, 0x7f, 0x7f]) }),
            batch({ base: 104, recs: records(2) })
        ]), 102).then(function (p) {
            p.messageSet.should.eql([]);
            should.exist(p.error);
            p.nextOffset.should.equal(102);
        });
    });

    it('should not deliver records below the fetch offset', function () {
        return fetch(response([batch({ base: 100, recs: records(10) })]), 105).then(function (p) {
            offsets(p).should.eql([105, 106, 107, 108, 109]);
            p.nextOffset.should.equal(110);
        });
    });

    it('should not deliver transaction markers', function () {
        return fetch(response([
            batch({ base: 200, recs: records(2), pid: 7, txn: true }),
            marker(202, 7, 1),
            batch({ base: 203, recs: records(1) })
        ]), 200).then(function (p) {
            offsets(p).should.eql([200, 201, 203]);
            p.nextOffset.should.equal(204);
        });
    });

    it('should drop aborted transactions listed by the broker', function () {
        return fetch(response([
            batch({ base: 300, recs: records(2), pid: 9, txn: true }),
            marker(302, 9, 0),
            batch({ base: 303, recs: records(2), pid: 9, txn: true }), // later txn by the same producer
            marker(305, 9, 1)
        ], [[9, 300]]), 300).then(function (p) {
            offsets(p).should.eql([303, 304]);
            p.nextOffset.should.equal(306);
        });
    });

    it('should drop an aborted batch without decompressing it, but still check its CRC', function () {
        var limit = compression.maxOutputSize;
        var big = Buffer.from(protocol.write().Record({
            attributes: 0, timestampDelta: 0, offsetDelta: 0, key: null, value: Buffer.alloc(2048), headers: []
        }).result);
        var aborted = Buffer.from(protocol.write().RecordBatch({
            baseOffset: 0, codec: 1, compressedRecords: zlib.gzipSync(big), recordCount: 1, timestamp: 1e12,
            producerId: 7, producerEpoch: 0, baseSequence: 0, isTransactional: true
        }).result);
        var corrupt = Buffer.from(aborted);
        corrupt[corrupt.length - 1] ^= 0xff;
        compression.maxOutputSize = 1024; // the aborted batch inflates past it
        return fetch(response([aborted, marker(1, 7, 0), batch({ base: 2, recs: records(1) })], [[7, 0]]), 0).then(function (p) {
            should.not.exist(p.error);
            offsets(p).should.eql([2]);
            p.nextOffset.should.equal(3);
            return fetch(response([corrupt, marker(1, 7, 0), batch({ base: 2, recs: records(1) })], [[7, 0]]), 0);
        }).then(function (p) {
            p.error.message.should.match(/CRC/i);
            offsets(p).should.eql([]);
        }).finally(function () {
            compression.maxOutputSize = limit;
        });
    });

    it('should advance past a batch that ends in a marker or compacted-away records', function () {
        return fetch(response([
            batch({ base: 400, recs: records(5), lastDelta: 9 }),
            marker(410, 7, 1)
        ]), 400).then(function (p) {
            offsets(p).should.eql([400, 401, 402, 403, 404]);
            p.nextOffset.should.equal(411);
        });
    });

    it('should skip an undecodable batch in a share fetch and deliver the rest', function () {
        var c = new Client({ connectionString: '127.0.0.1:1', logger: { logLevel: 0 } });
        var bad = { header: { baseOffset: 5, lastOffsetDelta: 0, attributes: 1, codec: 1, recordCount: 1 }, recordsRaw: Buffer.from('not gzip') };
        var raw = batch({ base: 6, recs: records(2) });
        var good = protocol.read(raw).RecordBatch(null, raw.length).result[0];
        return c._decompressShareFetchResults([{ topic: 't', partition: 0, messageSet: [bad, good], acquiredRecords: [] }]).then(function (r) {
            offsets(r[0]).should.eql([6, 7]);
            should.not.exist(r[0].error);
        });
    });

    it('should never move the position backwards, whatever the batch headers say', function () {
        return fetch(response([
            batch({ base: 1000, recs: records(2) }),
            batch({ base: 0, recs: records(1) })
        ]), 1000).then(function (p) {
            offsets(p).should.eql([1000, 1001]);
            p.nextOffset.should.equal(1002);
        });
    });

    it('should match aborted transactions in linear time', function () {
        var c = new Client({ connectionString: '127.0.0.1:1', logger: { logLevel: 0 } });
        var aborted = [], batches = [], i, t;
        for (i = 0; i < 200000; i++) { aborted.push({ producerId: i, firstOffset: i }); }
        for (i = 0; i < 1000; i++) {
            batches.push({ header: { baseOffset: i * 200, lastOffsetDelta: 199, attributes: 0, codec: 0, recordCount: 0 }, recordsRaw: Buffer.alloc(0) });
        }
        t = Date.now();
        return c._decompressRecordBatches(batches, 0, aborted).then(function (r) {
            r.nextOffset.should.equal(200000);
            (Date.now() - t).should.be.below(2000); // shift() per entry was quadratic: ~3 s here
        });
    });

    it('should handle a response with more batches than fit in a call stack', function () {
        var c = new Client({ connectionString: '127.0.0.1:1', logger: { logLevel: 0 } });
        var batches = [], i;
        for (i = 0; i < 200000; i++) {
            batches.push({ header: { baseOffset: i, lastOffsetDelta: 0, attributes: 0, codec: 0, recordCount: 0 }, recordsRaw: Buffer.alloc(0) });
        }
        return c._decompressRecordBatches(batches, 0).then(function (r) {
            r.nextOffset.should.equal(200000);
        });
    });
});
