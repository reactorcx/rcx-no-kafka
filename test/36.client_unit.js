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

        it('should read only the values that are file paths, never a PEM key', function () {
            var certPath = path.join(__dirname, './ssl/client.crt');
            var key = '\n-----BEGIN PRIVATE KEY-----\nc2VjcmV0\n-----END PRIVATE KEY-----\n'; // template-literal style
            var c = new Kafka.Producer({
                connectionString: 'kafka://127.0.0.1:9093',
                ssl: { cert: certPath, key: key }
            }).client;

            sinon.stub(c, 'apiVersionsRequest').returns(Promise.resolve({}));
            sinon.stub(c, 'updateMetadata').returns(Promise.resolve());

            return c.init().then(function () {
                c.options.ssl.cert.toString().should.match(/^-----BEGIN CERTIFICATE-----/);
                c.options.ssl.key.should.equal(key);
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

        it('should send the next refresh after a request that threw synchronously', function () {
            var c = makeClient();
            var calls = 0;

            sinon.stub(c, 'metadataRequest').callsFake(function () {
                if (++calls === 1) { throw new TypeError('topicNames.map is not a function'); }
                return Promise.resolve({ broker: [{ nodeId: 0, host: 'h', port: 1 }], topicMetadata: [] });
            });
            sinon.stub(c, 'apiVersionsRequest').returns(Promise.resolve({}));

            return Promise.resolve().then(function () {
                return c.updateMetadata(['A']);
            }).then(function () {
                throw new Error('expected the first refresh to reject');
            }, function (err) {
                err.should.be.instanceOf(TypeError);
                return c.updateMetadata(['A']);
            }).then(function () {
                calls.should.equal(2);
            });
        });

        it('should not fail requests waiting on a refresh that fails', function () {
            var c = makeClient();
            var refresh, offsets;

            sinon.stub(c, 'metadataRequest').callsFake(function () {
                return promiseUtils.delay(10).then(function () { throw new errors.NoKafkaConnectionError('b:9092', 'down'); });
            });
            refresh = c.updateMetadata(['A']).catch(function (err) { return err; });
            offsets = c.offsetRequest({}); // no leaders: resolves with nothing once metadata is settled

            return Promise.all([refresh, offsets]).then(function (r) {
                r[0].should.be.instanceOf(errors.NoKafkaConnectionError); // the refresh's own caller sees it
                r[1].should.eql([]);
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

        function lostConn() {
            var conn = fakeConn('coord:1', function () {
                return Promise.reject(new errors.NoKafkaConnectionError('coord:1', 'down'));
            });
            conn.close = sinon.spy();
            return conn;
        }

        function rejected(p) {
            return p.then(function () { throw new Error('expected rejection'); }, function (err) { return err; });
        }

        it('should drop and close the group coordinator when TxnOffsetCommit loses it', function () {
            var c = makeClient(), conn = lostConn();
            c.groupCoordinators.g = Promise.resolve(conn);

            return rejected(c.txnOffsetCommitRequest('tx', 'g', 1, 0, [{ topic: 't', partition: 0, offset: 1 }])).then(function () {
                should.not.exist(c.groupCoordinators.g);
                return promiseUtils.delay(0);
            }).then(function () {
                conn.close.callCount.should.equal(1);
            });
        });

        it('should drop the group coordinator when OffsetFetch loses it (GroupAdmin.fetchConsumerLag)', function () {
            var c = makeClient(), conn = lostConn();
            c.groupCoordinators.g = Promise.resolve(conn);

            return rejected(c.offsetFetchRequestV1('g', [{ topicName: 't', partitions: [0] }])).then(function () {
                should.not.exist(c.groupCoordinators.g);
            });
        });

        it('should drop the group coordinator when DescribeGroups answers NotCoordinatorForGroup', function () {
            var c = makeClient();
            var conn = fakeConn('coord:1', function () { return Promise.resolve(Buffer.alloc(0)); });
            c.groupCoordinators.g = Promise.resolve(conn);
            sinon.stub(c.protocol, 'read').returns({
                DescribeGroupResponse: function () { return { result: { groups: [{ error: errors.byName('NotCoordinatorForGroup'), groupId: 'g' }] } }; }
            });

            return c.describeGroupRequest('g').then(function (group) {
                group.error.code.should.equal('NotCoordinatorForGroup'); // still returned to the caller
                should.not.exist(c.groupCoordinators.g);
            });
        });

        it('should close the transaction coordinator it drops after an error', function () {
            var c = makeClient(), conn = lostConn();
            c.transactionCoordinators.tx = Promise.resolve(conn);

            return rejected(c.endTxnRequest('tx', 1, 0, true)).then(function () {
                should.not.exist(c.transactionCoordinators.tx);
                return promiseUtils.delay(0);
            }).then(function () {
                conn.close.callCount.should.equal(1);
            });
        });

        it('should not drop a newer coordinator entry when an older lookup fails', function () {
            var c = makeClient();
            var lookup, newer = Promise.resolve(fakeConn('new:1'));

            sinon.stub(c, '_sendToAnyBroker').returns(promiseUtils.delay(5).then(function () {
                throw errors.byName('GroupCoordinatorNotAvailable');
            }));
            lookup = rejected(c._findGroupCoordinator('g'));
            c.updateGroupCoordinator('g');
            c.groupCoordinators.g = newer; // a request after the invalidation started its own lookup

            return lookup.then(function () {
                c.groupCoordinators.g.should.equal(newer);
            });
        });
    });

    describe('topic-level metadata errors', function () {
        it('should not cache a topic that came back with an error (first send to an auto-created topic)', function () {
            var p = new Kafka.Producer({ clientId: 'client-unit' });
            var c = p.client, calls = 0;

            sinon.stub(c, 'metadataRequest').callsFake(function () {
                var topic = ++calls === 1
                    ? { error: errors.byName('LeaderNotAvailable'), topicName: 't', partitionMetadata: [] } // still being created
                    : { error: null, topicName: 't', partitionMetadata: [{ error: null, partitionId: 0, leader: 0 }] };
                return Promise.resolve({ broker: [{ nodeId: 0, host: 'h', port: 1 }], topicMetadata: [topic] });
            });
            sinon.stub(c, 'apiVersionsRequest').returns(Promise.resolve({}));
            sinon.stub(c, 'produceRequest').callsFake(function (requests) {
                return Promise.resolve(requests.map(function (r) { return { topic: r.topic, partition: r.partition, offset: 0 }; }));
            });

            return p.send({ topic: 't', message: { value: 'x' } }, { retries: { attempts: 3, delay: { min: 1, max: 2 } } }).then(function (r) {
                r.should.eql([{ topic: 't', partition: 0, offset: 0 }]);
            });
        });

        [12, 13].forEach(function (v) {
            it('should ask Metadata v' + v + ' by name, never by a cached topic ID (stale once the topic is recreated)', function () {
                var c = makeClient();
                var expected;

                c.initialBrokers = [{ apiVersions: { 3: { min: 0, max: v } } }];
                c.topicIds.A = '550e8400-e29b-41d4-a716-446655440000';
                sinon.stub(c, '_sendToAnyBroker').returns(Promise.resolve({}));

                expected = Buffer.from(c.protocol.write()['MetadataRequestV' + v]({
                    correlationId: c.correlationId, clientId: c.options.clientId, topicNames: [{ topicId: null, name: 'A' }]
                }).result);
                return c.metadataRequest(['A']).then(function () {
                    c._sendToAnyBroker.args[0][1].equals(expected).should.equal(true);
                });
            });
        });

        it('should send InitProducerId at most v5, the newest version whose response it reads', function () {
            var c = makeClient();
            c.initialBrokers = [{ apiVersions: { 22: { min: 0, max: 6 } } }]; // v6 only with unstable APIs enabled
            sinon.stub(c, '_sendToAnyBroker').returns(Promise.resolve({}));

            return c.initProducerIdRequest(null, 1000).then(function () {
                c._sendToAnyBroker.args[0][1].readInt16BE(2).should.equal(5); // apiKey, then apiVersion
            });
        });

        it('should fail the refresh, keeping the brokers it has, on a top-level Metadata v13 error', function () {
            var c = makeClient();
            var known = fakeConn('known:1');

            c.initialBrokers = [{ apiVersions: { 3: { min: 0, max: 13 } } }];
            c.brokerConnections = { 0: known };
            sinon.stub(c, '_sendToAnyBroker').callsFake(function (correlationId, buffer, parse) {
                return Promise.resolve().then(function () { return parse(Buffer.alloc(0)); });
            });
            sinon.stub(c.protocol, 'read').returns({
                MetadataResponseV13: function () {
                    return { result: { error: errors.byName('RebootstrapRequired'), broker: [], topicMetadata: [] } };
                }
            });

            return c.updateMetadata(['A']).then(function () {
                throw new Error('expected rejection');
            }, function (err) {
                err.code.should.equal('RebootstrapRequired');
                c.brokerConnections[0].should.equal(known);
            });
        });
    });

    describe('findLeader', function () {
        it('should report a known partition with no leader as LeaderNotAvailable (retriable)', function () {
            var c = makeClient();
            c.topicMetadata = Object.create(null);
            c.topicMetadata.t = { 0: { partitionId: 0, leader: -1 } };
            sinon.stub(c, 'updateMetadata').returns(Promise.resolve());
            return c.findLeader('t', 0).then(function () { throw new Error('expected rejection'); }, function (err) {
                err.code.should.equal('LeaderNotAvailable');
                return c.findLeader('t', 5);
            }).then(function () { throw new Error('expected rejection'); }, function (err) {
                err.code.should.equal('UnknownTopicOrPartition');
            });
        });
    });
});
