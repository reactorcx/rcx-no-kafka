'use strict';

/* global describe, it, should */

var Protocol = require('../lib/protocol');
var Client   = require('../lib/client');
var crc32c   = require('../lib/protocol/misc/crc32c');

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

    it('should advance past a batch that ends in a marker or compacted-away records', function () {
        return fetch(response([
            batch({ base: 400, recs: records(5), lastDelta: 9 }),
            marker(410, 7, 1)
        ]), 400).then(function (p) {
            offsets(p).should.eql([400, 401, 402, 403, 404]);
            p.nextOffset.should.equal(411);
        });
    });

    it('should report share-fetch decode failures as the partition error', function () {
        var c = new Client({ connectionString: '127.0.0.1:1', logger: { logLevel: 0 } });
        var bad = { header: { baseOffset: 5, lastOffsetDelta: 0, attributes: 1, codec: 1, recordCount: 1 }, recordsRaw: Buffer.from('not gzip') };
        return c._decompressShareFetchResults([{ topic: 't', partition: 0, messageSet: [bad], acquiredRecords: [] }]).then(function (r) {
            r[0].messageSet.should.eql([]);
            should.exist(r[0].error);
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
