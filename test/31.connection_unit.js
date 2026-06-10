'use strict';

/* global describe, it, afterEach, sinon, should */

var Connection = require('../lib/connection');
var tls        = require('tls');
var net        = require('net');

function fakeSocket() {
    return {
        destroyed: false,
        destroy: function () { this.destroyed = true; },
        end: function () {},
        write: function () {},
        setNoDelay: function () {},
        setKeepAlive: function () {},
        setTimeout: function () {},
        on: function () {}
    };
}

function connectedConnection(options) {
    var c = new Connection(options || {});
    c.socket = fakeSocket();
    c.connected = true;
    return c;
}

describe('Connection (unit)', function () {
    describe('_receive frame validation', function () {
        it('should reject pending requests and disconnect on negative frame length', function () {
            var c = connectedConnection();
            var rejected = null;
            var buf = Buffer.alloc(8);

            c.queue[1] = { resolve: function () {}, reject: function (err) { rejected = err; } };

            buf.writeInt32BE(-4, 0);
            buf.writeInt32BE(1, 4);
            c._receive(buf);

            c.connected.should.equal(false);
            should.exist(rejected);
        });

        it('should not throw when a negative length frame is truncated', function () {
            var c = connectedConnection();
            var buf = Buffer.alloc(4);

            buf.writeInt32BE(-1, 0);
            (function () { c._receive(buf); }).should.not.throw();
            c.connected.should.equal(false);
        });

        it('should disconnect when frame length exceeds maxFrameSize', function () {
            var c = connectedConnection({ maxFrameSize: 1024 });
            var buf = Buffer.alloc(8);

            buf.writeInt32BE(2048, 0);
            buf.writeInt32BE(1, 4);
            c._receive(buf);

            c.connected.should.equal(false);
        });

        it('should resolve a pending request for a valid frame', function () {
            var c = connectedConnection();
            var resolved = null;
            var buf = Buffer.alloc(4 + 10);

            c.queue[7] = { resolve: function (b) { resolved = b; }, reject: function () {} };

            buf.writeInt32BE(10, 0);
            buf.writeInt32BE(7, 4);
            c._receive(buf);

            should.exist(resolved);
            resolved.length.should.equal(10);
            c.connected.should.equal(true);
        });

        it('should shrink the receive buffer back to initialBufferSize after a large frame completes', function () {
            var c = connectedConnection({ initialBufferSize: 64 });
            var resolved = null;
            var frame = Buffer.alloc(4 + 1000);

            c.queue[7] = { resolve: function (b) { resolved = b; }, reject: function () {} };

            frame.writeInt32BE(1000, 0);
            frame.writeInt32BE(7, 4);
            c._receive(frame.slice(0, 500));
            c._receive(frame.slice(500));

            should.exist(resolved);
            c.offset.should.equal(0);
            c.buffer.length.should.equal(64);
        });
    });

    describe('request lifecycle', function () {
        it('should reject stranded queue entries even when already disconnected', function () {
            var c = new Connection({});
            var rejected = null;
            c.socket = fakeSocket();
            c.connected = false; // a disconnect already ran; a racing send() strands an entry
            c.queue[9] = { resolve: function () {}, reject: function (err) { rejected = err; } };

            c._disconnect(new Error('gone'));

            should.exist(rejected);
            Object.keys(c.queue).should.have.length(0);
        });

        it('should reject a pending request after requestTimeout', function () {
            var c = connectedConnection({ requestTimeout: 20 });
            return c.send(1, Buffer.from('x')).then(function () {
                throw new Error('expected timeout rejection');
            }, function (err) {
                String(err.message).should.match(/timed out/i);
                Object.keys(c.queue).should.have.length(0);
            });
        });
    });

    describe('close() racing connect()', function () {
        var netStub;

        afterEach(function () {
            if (netStub) { netStub.restore(); netStub = null; }
        });

        it('should not leave a connected socket behind when close() lands mid-connect', function () {
            var connectCb;
            var socket = fakeSocket();
            var c;
            var p;

            netStub = sinon.stub(net, 'connect').callsFake(function (port, host, cb) {
                connectCb = cb;
                return socket;
            });

            c = new Connection({ connectionTimeout: 1000 });
            p = c.connect();
            c.close();
            connectCb(); // TCP handshake completes after close()

            socket.destroyed.should.equal(true);
            c.connected.should.equal(false);
            return p.then(function () { return null; }, function () { return null; });
        });
    });

    describe('TLS selection (fail closed)', function () {
        var tlsStub, netStub;

        afterEach(function () {
            if (tlsStub) { tlsStub.restore(); tlsStub = null; }
            if (netStub) { netStub.restore(); netStub = null; }
        });

        function stubSockets() {
            tlsStub = sinon.stub(tls, 'connect').returns(fakeSocket());
            netStub = sinon.stub(net, 'connect').returns(fakeSocket());
        }

        it('should use TLS when ssl is true', function () {
            var c;
            stubSockets();
            c = new Connection({ ssl: true, connectionTimeout: 10 });
            c.connect().catch(function () {});
            tlsStub.callCount.should.equal(1);
            netStub.callCount.should.equal(0);
        });

        it('should use TLS when ssl.enabled is true', function () {
            var c;
            stubSockets();
            c = new Connection({ ssl: { enabled: true }, connectionTimeout: 10 });
            c.connect().catch(function () {});
            tlsStub.callCount.should.equal(1);
            netStub.callCount.should.equal(0);
        });

        it('should use TLS when ssl has cert and key', function () {
            var c;
            stubSockets();
            c = new Connection({ ssl: { cert: 'CERT', key: 'KEY' }, connectionTimeout: 10 });
            c.connect().catch(function () {});
            tlsStub.callCount.should.equal(1);
            netStub.callCount.should.equal(0);
        });

        it('should use plaintext when ssl fields are empty (back-compat default)', function () {
            var c;
            stubSockets();
            c = new Connection({ ssl: { cert: null, key: null }, connectionTimeout: 10 });
            c.connect().catch(function () {});
            tlsStub.callCount.should.equal(0);
            netStub.callCount.should.equal(1);
        });
    });
});
