'use strict';

/* global describe, it, sinon, should */

var path         = require('path');
var Kafka        = require('../lib/index');
var errors       = require('../lib/errors');
var promiseUtils = require('../lib/promise-utils');

function makeClient() {
    return new Kafka.Producer({ clientId: 'client-unit' }).client;
}

function fakeConn(name, send) {
    return {
        server: function () { return name; },
        send: send,
        cancelPending: function () {},
        equal: function () { return false; },
        close: function () {},
        apiVersions: null
    };
}

describe('Client (unit)', function () {
    describe('init() ssl file loading', function () {
        it('should propagate cert/key read failures even when a ca file is also configured', function () {
            var caPath = path.join(__dirname, './ssl/client.crt');
            var p = new Kafka.Producer({
                connectionString: 'kafka://127.0.0.1:9093',
                ssl: { cert: '/nonexistent/cert.pem', key: '/nonexistent/key.pem', ca: caPath }
            });

            return p.init().then(function () {
                throw new Error('expected init to reject');
            }, function (err) {
                String(err.message || err).should.match(/ENOENT/);
            });
        });
    });

    describe('apiVersionsRequest failure handling', function () {
        it('should leave apiVersions null after a transport failure so discovery is retried', function () {
            var c = makeClient();
            var conn = fakeConn('x:1', function () {
                return Promise.reject(new errors.NoKafkaConnectionError('x:1', 'connect ECONNREFUSED'));
            });

            return c.apiVersionsRequest(conn).then(function () {
                should.equal(conn.apiVersions, null);
            });
        });

        it('should fall back to v0 when the broker closes the connection on ApiVersions', function () {
            var c = makeClient();
            var conn = fakeConn('x:1', function () {
                return Promise.reject(new errors.NoKafkaConnectionError('x:1', 'Kafka server has closed connection'));
            });

            return c.apiVersionsRequest(conn).then(function () {
                conn.apiVersions.should.eql({});
            });
        });
    });

    describe('produceRequest with a vanished leader', function () {
        it('should resolve with LeaderNotAvailable instead of throwing', function () {
            var c = makeClient();
            c.brokerConnections = {};

            return c.produceRequest([{ topic: 't', partition: 0, leader: 5, message: { value: 'x' } }], 0)
            .then(function (result) {
                result.should.have.length(1);
                result[0].should.have.property('error');
                result[0].error.code.should.equal('LeaderNotAvailable');
            });
        });
    });

    describe('_sendToAnyBroker fallback', function () {
        it('should fall back to live broker connections when all initial brokers fail', function () {
            var c = makeClient();
            var dead = fakeConn('dead:1', function () {
                return Promise.reject(new errors.NoKafkaConnectionError('dead:1', 'refused'));
            });
            var alive = fakeConn('alive:1', function () {
                return Promise.resolve(Buffer.alloc(0));
            });

            c.initialBrokers = [dead];
            c.brokerConnections = { 0: alive };

            return c._sendToAnyBroker(1, Buffer.alloc(0), function () {
                return 'parsed';
            }).then(function (result) {
                result.should.equal('parsed');
            });
        });
    });

    describe('updateMetadata coalescing', function () {
        it('should issue a follow-up request for topics not covered by the in-flight refresh', function () {
            var c = makeClient();
            var seq = [];

            sinon.stub(c, 'metadataRequest').callsFake(function (topics) {
                seq.push(topics || []);
                return promiseUtils.delay(20).then(function () {
                    return { broker: [{ nodeId: 0, host: 'h', port: 1 }], topicMetadata: [] };
                });
            });
            sinon.stub(c, 'apiVersionsRequest').returns(Promise.resolve({}));

            return Promise.all([
                c.updateMetadata(['A']),
                c.updateMetadata(['B'])
            ]).then(function () {
                var requested = [].concat.apply([], seq);
                requested.should.include('A');
                requested.should.include('B');
            });
        });
    });

    describe('connection option plumbing', function () {
        it('should pass requestTimeout and maxFrameSize through to broker connections', function () {
            var c = new Kafka.Producer({ clientId: 'client-unit', requestTimeout: 1234, maxFrameSize: 5678 }).client;
            var conn = c._createConnection('h', 1);
            conn.options.requestTimeout.should.equal(1234);
            conn.options.maxFrameSize.should.equal(5678);
        });
    });

    describe('coordinator cache invalidation', function () {
        it('should remove the cached group coordinator synchronously', function () {
            var c = makeClient();
            var u;

            c.groupCoordinators.g = Promise.resolve({ close: function () {} });
            u = c.updateGroupCoordinator('g');

            should.not.exist(c.groupCoordinators.g);
            return u;
        });
    });
});
