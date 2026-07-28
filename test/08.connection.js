'use strict';

/* global describe, it, before, sinon, after, expect  */

var path = require('path');
var fs = require('fs');
var promiseUtils = require('../lib/promise-utils');
var zlib    = require('zlib');
var Kafka   = require('../lib/index');
var Connection = require('../lib/connection');

// CRC-32 (ISO 3309) — use native zlib.crc32 (Node 22+) or pure-JS fallback
var crc32 = (typeof zlib.crc32 === 'function') ? zlib.crc32 : (function () {
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

describe('Connection receive buffer', function () {
    // Feed a single framed response through _receive in TCP-sized chunks.
    // Returns the reassembled frame plus how much the buffer growth cost.
    function receiveFramed(frameBytes, chunkBytes, connectionOptions) {
        var conn = new Connection(connectionOptions || {});
        var frame = Buffer.allocUnsafe(4 + frameBytes);
        var grows = 0, copied = 0;
        var origGrow = conn._growBuffer;
        var received = null;
        var off;

        conn.connected = true;

        frame.writeInt32BE(frameBytes, 0);
        frame.writeInt32BE(77, 4); // correlationId
        frame.fill(0xAB, 8);

        conn._growBuffer = function (n) {
            grows++;
            copied += this.offset;
            return origGrow.call(this, n);
        };

        conn.queue[77] = { resolve: function (buf) { received = buf; } };

        for (off = 0; off < frame.length; off += chunkBytes) {
            conn._receive(frame.slice(off, Math.min(off + chunkBytes, frame.length)));
        }

        return { received: received, grows: grows, copied: copied, conn: conn };
    }

    it('should reassemble a frame split across many chunks', function () {
        var r = receiveFramed(3 * 1024 * 1024, 64 * 1024);
        expect(r.received).to.not.equal(null);
        r.received.length.should.be.eql(3 * 1024 * 1024);
        r.received.readInt32BE(0).should.be.eql(77);
        r.received[4].should.be.eql(0xAB);
        r.received[r.received.length - 1].should.be.eql(0xAB);
        r.conn.offset.should.be.eql(0); // fully consumed, nothing left buffered
    });

    it('should grow the receive buffer geometrically, not to an exact fit', function () {
        // Exact-fit growth reallocates once per chunk, making reassembly O(n^2) in
        // memcpy — ~400 reallocs and ~5GB copied for a 25MB response. Geometric
        // growth keeps it near-linear. Guards the consumer's 25MB maxBytes default.
        var frameBytes = 25 * 1024 * 1024;
        var chunkBytes = 64 * 1024;
        var initialBufferSize = 256 * 1024;
        // doubling from the initial buffer needs ceil(log2(frame / initial)) steps;
        // allow a couple spare, but far below the one-per-chunk of exact-fit growth
        var maxGrows = Math.ceil(Math.log2(frameBytes / initialBufferSize)) + 2;

        var r = receiveFramed(frameBytes, chunkBytes);

        r.received.length.should.be.eql(frameBytes);
        r.grows.should.be.at.most(maxGrows);
        // total bytes re-copied must stay a small multiple of the frame, not O(n^2)
        r.copied.should.be.below(frameBytes * 4);
        // doubling overshoots, but never past 2x — guards steady-state memory per connection
        r.conn.buffer.length.should.be.below(frameBytes * 2);
    });

    it('should still fit a frame larger than the initial buffer size', function () {
        var r = receiveFramed(512 * 1024, 64 * 1024, { initialBufferSize: 8 * 1024 });
        r.received.length.should.be.eql(512 * 1024);
        r.conn.buffer.length.should.be.at.least(512 * 1024);
    });

    it('should not loop forever if the initial buffer size is zero', function () {
        var r = receiveFramed(128 * 1024, 32 * 1024, { initialBufferSize: 0 });
        r.received.length.should.be.eql(128 * 1024);
    });

    it('should handle several frames arriving in a single chunk', function () {
        var conn = new Connection({});
        var seen = [];

        function frameFor(id, payloadBytes) {
            var f = Buffer.allocUnsafe(4 + 4 + payloadBytes);
            f.writeInt32BE(4 + payloadBytes, 0);
            f.writeInt32BE(id, 4);
            f.fill(0x01, 8);
            return f;
        }

        conn.connected = true;

        [11, 12].forEach(function (id) {
            conn.queue[id] = { resolve: function (buf) { seen.push(buf.readInt32BE(0)); } };
        });

        conn._receive(Buffer.concat([frameFor(11, 100), frameFor(12, 200)]));

        seen.should.be.eql([11, 12]);
        conn.offset.should.be.eql(0);
    });
});

describe('Connection', function () {
    var producer = new Kafka.Producer({ requiredAcks: 0, clientId: 'producer' });
    var consumer = new Kafka.SimpleConsumer({ idleTimeout: 100, clientId: 'simple-consumer' });

    var dataHandlerSpy = sinon.spy(function () {});

    before(function () {
        return Promise.all([
            producer.init(),
            consumer.init()
        ])
        .then(function () {
            return consumer.subscribe('kafka-test-topic', 0, dataHandlerSpy);
        });
    });

    after(function () {
        return Promise.all([
            producer.end(),
            consumer.end()
        ]);
    });

    it('should be able to grow receive buffer', function () {
        var buf = Buffer.alloc(384 * 1024), crc = (crc32(buf) | 0);

        dataHandlerSpy.reset();

        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: buf }
        })
        .then(promiseUtils.delayChain(300))
        .then(function () {
            dataHandlerSpy.should.have.been.called; // eslint-disable-line
            dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
            dataHandlerSpy.lastCall.args[1].should.be.a('string', 'kafka-test-topic');
            dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

            dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
            dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
            dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
            (crc32(dataHandlerSpy.lastCall.args[0][0].message.value) | 0).should.be.eql(crc);
        });
    });

    it('should parse connection string with protocol', function () {
        var p = new Kafka.Producer({ connectionString: 'kafka://127.0.0.1:9092', ssl: { cert: null, key: null } });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(1);
            p.client.initialBrokers[0].server().should.be.eql('127.0.0.1:9092');
        });
    });

    it('should parse connection string without protocol', function () {
        var p = new Kafka.Producer({ connectionString: '127.0.0.1:9092', ssl: { cert: null, key: null } });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(1);
            p.client.initialBrokers[0].server().should.be.eql('127.0.0.1:9092');
        });
    });

    it('should parse connection string with multiple hosts with and without protocol', function () {
        var p = new Kafka.Producer({ connectionString: 'kafka://127.0.0.1:9092,127.0.0.1:9092', ssl: { cert: null, key: null } });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(2);
            p.client.initialBrokers[0].server().should.be.eql('127.0.0.1:9092');
            p.client.initialBrokers[1].server().should.be.eql('127.0.0.1:9092');
        });
    });

    it('should parse connection string with multiple hosts without protocol', function () {
        var p = new Kafka.Producer({ connectionString: '127.0.0.1:9092,127.0.0.1:9092', ssl: { cert: null, key: null } });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(2);
            p.client.initialBrokers[0].server().should.be.eql('127.0.0.1:9092');
            p.client.initialBrokers[1].server().should.be.eql('127.0.0.1:9092');
        });
    });

    it('should strip whitespaces in connectionString', function () {
        var p = new Kafka.Producer({ connectionString: ' kafka://127.0.0.1:9092, localhost:9092 ', ssl: { cert: null, key: null } });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(2);
            p.client.initialBrokers[0].server().should.be.eql('127.0.0.1:9092');
            p.client.initialBrokers[1].server().should.be.eql('localhost:9092');
        });
    });

    it('should parse connection string with + in the protocol', function () {
        var p = new Kafka.Producer({ connectionString: 'kafka+ssl://127.0.0.1:9092', ssl: { cert: null, key: null } });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(1);
            p.client.initialBrokers[0].server().should.be.eql('127.0.0.1:9092');
        });
    });

    it('should parse connection string with multiple hosts with + in the protocol', function () {
        var p = new Kafka.Producer({ connectionString: 'kafka+ssl://127.0.0.1:9092,kafka+ssl://127.0.0.1:9092', ssl: { cert: null, key: null } });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(2);
            p.client.initialBrokers[0].server().should.be.eql('127.0.0.1:9092');
            p.client.initialBrokers[1].server().should.be.eql('127.0.0.1:9092');
        });
    });

    it('should parse connection string with hosts with and without + in the protocol', function () {
        var p = new Kafka.Producer({ connectionString: 'kafka+ssl://127.0.0.1:9092,kafka://127.0.0.1:9092,127.0.0.1:9092', ssl: { cert: null, key: null } });

        return p.init().then(function () {
            p.client.initialBrokers.should.be.an('array').and.have.length(3);
            p.client.initialBrokers[0].server().should.be.eql('127.0.0.1:9092');
            p.client.initialBrokers[1].server().should.be.eql('127.0.0.1:9092');
            p.client.initialBrokers[2].server().should.be.eql('127.0.0.1:9092');
        });
    });

    it('should throw an error when clientId is invalid', function () {
        (function () {
            var p = new Kafka.Producer({ clientId: 'client:1' });
            p.init();
        }).should.throw('Invalid clientId');
    });

    describe('when configuring SSL CA', function () {
        var configuredCert, configuredKey;

        before(function () {
            configuredCert = process.env.KAFKA_CLIENT_CERT;
            configuredKey = process.env.KAFKA_CLIENT_CERT_KEY;
            delete process.env.KAFKA_CLIENT_CERT;
            delete process.env.KAFKA_CLIENT_CERT_KEY;
        });

        after(function () {
            process.env.KAFKA_CLIENT_CERT = configuredCert;
            process.env.KAFKA_CLIENT_CERT_KEY = configuredKey;
        });

        it('should load from file', function () {
            var caPath = path.join(__dirname, './ssl/client.crt');
            var p = new Kafka.Producer({ connectionString: 'kafka://127.0.0.1:9093', ssl: { ca: caPath } });

            return p.init().then(function () {
                p.client.options.ssl.ca.should.be.eql(fs.readFileSync(caPath));
            });
        });

        it('should load from string', function () {
            var caPath = path.join(__dirname, './ssl/client.crt');
            var caContent = fs.readFileSync(caPath);
            var p = new Kafka.Producer({ connectionString: 'kafka://127.0.0.1:9093', ssl: { ca: caContent } });

            return p.init().then(function () {
                p.client.options.ssl.ca.should.be.eql(caContent);
            });
        });
    });
});
