'use strict';

/* global describe, it, sinon, should */

var fs           = require('fs');
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

        it('should hand structured TLS values (arrays, { pem, passphrase }) to tls unchanged', function () {
            var cert = fs.readFileSync(path.join(__dirname, './ssl/client.crt'), 'utf8');
            var key = [{ pem: fs.readFileSync(path.join(__dirname, './ssl/client.key'), 'utf8'), passphrase: 'secret' }];
            var ca = [cert, Buffer.from(cert)];
            var c = new Kafka.Producer({
                connectionString: 'kafka://127.0.0.1:9093',
                ssl: { cert: cert, key: key, ca: ca }
            }).client;

            sinon.stub(c, 'apiVersionsRequest').returns(Promise.resolve({}));
            sinon.stub(c, 'updateMetadata').returns(Promise.resolve());

            return c.init().then(function () {
                c.options.ssl.key.should.equal(key);
                c.options.ssl.ca.should.equal(ca);
            });
        });

        it('should read a value given as a file URL', function () {
            var caPath = path.join(__dirname, './ssl/client.crt');
            var c = new Kafka.Producer({
                connectionString: 'kafka://127.0.0.1:9093',
                ssl: { ca: require('url').pathToFileURL(caPath) }
            }).client;

            sinon.stub(c, 'apiVersionsRequest').returns(Promise.resolve({}));
            sinon.stub(c, 'updateMetadata').returns(Promise.resolve());

            return c.init().then(function () {
                c.options.ssl.ca.toString().should.match(/^-----BEGIN CERTIFICATE-----/);
            });
        });

        it('should not put a key that is not PEM text in the error', function () {
            var key = 'TUlJRXZRSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2N3Z2dTakFnRUFBb0lCQVFD'; // base64, no PEM armour
            var c = new Kafka.Producer({
                connectionString: 'kafka://127.0.0.1:9093',
                ssl: { cert: path.join(__dirname, './ssl/client.crt'), key: key }
            }).client;

            return c.init().then(function () {
                throw new Error('expected init to reject');
            }, function (err) {
                err.message.should.match(/ssl\.key/);
                err.message.should.not.contain(key);
                should.not.exist(err.path);
            });
        });

        it('should read a file whose path holds "-----BEGIN " mid-line', function () {
            var dir = fs.mkdtempSync(path.join(require('os').tmpdir(), 'nk-pem-')), file = path.join(dir, 'kafka -----BEGIN ca.crt');
            var c = new Kafka.Producer({ connectionString: 'kafka://127.0.0.1:9093', ssl: { ca: file } }).client;
            fs.copyFileSync(path.join(__dirname, './ssl/client.crt'), file);
            sinon.stub(c, 'apiVersionsRequest').returns(Promise.resolve({}));
            sinon.stub(c, 'updateMetadata').returns(Promise.resolve());

            return c.init().then(function () {
                c.options.ssl.ca.toString().should.match(/^-----BEGIN CERTIFICATE-----/); // read, not passed on as PEM
            }).finally(function () {
                fs.rmSync(dir, { recursive: true, force: true });
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

    describe('legacy MessageSet compression', function () {
        it('should give the inner messages of a magic-1 wrapper their absolute offsets', function () {
            var c = makeClient();
            var inner = Buffer.from(c.protocol.write().MessageSet([
                { offset: 0, message: { value: Buffer.from('a') } },
                { offset: 1, message: { value: Buffer.from('b') } }
            ]).result);
            var wrapper = { magicByte: 1, attributes: { _raw: 0x01, codec: 1 }, value: require('zlib').gzipSync(inner) };

            return c._decompressMessageSet(wrapper, 41).then(function (messages) {
                messages.map(function (m) { return m.offset; }).should.eql([40, 41]); // inner offsets are relative
            });
        });

        it('should reject a compressed wrapper holding no messages', function () {
            var c = makeClient();
            var wrapper = { magicByte: 1, attributes: { _raw: 0x01, codec: 1 }, value: require('zlib').gzipSync(Buffer.alloc(0)) };

            return c._decompressMessageSet(wrapper, 41).then(function () {
                throw new Error('expected rejection');
            }, function (err) {
                err.message.should.match(/Empty/); // its offsets aren't gaps to acknowledge
            });
        });

        it('should reject a compressed wrapper whose inner message set is cut short', function () {
            var c = makeClient();
            var inner = Buffer.from(c.protocol.write().MessageSet([
                { offset: 0, message: { value: Buffer.from('aaaa') } },
                { offset: 1, message: { value: Buffer.from('bbbb') } }
            ]).result);
            var wrapper = { magicByte: 1, attributes: { _raw: 0x01, codec: 1 }, value: require('zlib').gzipSync(inner.slice(0, inner.length - 25)) };

            return c._decompressMessageSet(wrapper, 41).then(function () {
                throw new Error('expected rejection');
            }, function (err) {
                err.message.should.match(/Truncated/); // offset 40 would have been given to the first record
            });
        });

        it('should give the inner messages of a LogAppendTime wrapper its timestamp', function () {
            var c = makeClient();
            var inner = Buffer.from(c.protocol.write().MessageSet([
                { offset: 0, message: { value: Buffer.from('a') } },
                { offset: 1, message: { value: Buffer.from('b') } }
            ]).result);
            var wrapper = { magicByte: 1, timestamp: 5e12, attributes: { _raw: 0x09, codec: 1 }, value: require('zlib').gzipSync(inner) };

            return c._decompressMessageSet(wrapper).then(function (messages) {
                messages.map(function (m) { return m.message.timestamp; }).should.eql([5e12, 5e12]);
            });
        });

        [1, 2].forEach(function (codec) { // gzip, snappy: their async compressors read the input later
            it('should compress each MessageSet from its own bytes, not the shared writer buffer (codec ' + codec + ')', function () {
                var c = new Kafka.Producer({ clientId: 'client-unit', asyncCompression: true }).client;
                var compression = require('../lib/protocol/misc/compression');
                var a = [{ offset: 0, message: { value: Buffer.alloc(200000, 'a') } }];
                var b = [{ offset: 0, message: { value: Buffer.alloc(200000, 'b') } }];

                return Promise.all([c._compressMessageSet(a, codec), c._compressMessageSet(b, codec)]).then(function (r) {
                    return compression.decompressAsync(r[0][0].message.value, codec);
                }).then(function (inner) {
                    var decoded = c.protocol.read(inner).MessageSet(null, inner.length).result;
                    decoded[0].message.value.equals(a[0].message.value).should.equal(true);
                });
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

        // a cache entry as a resolved coordinator lookup leaves it
        function cachedLookup(conn) {
            var p = Promise.resolve(conn);
            p.connection = conn;
            return p;
        }

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
            c.groupCoordinators.g = cachedLookup(conn);

            return rejected(c.txnOffsetCommitRequest('tx', 'g', 1, 0, [{ topic: 't', partition: 0, offset: 1 }])).then(function () {
                should.not.exist(c.groupCoordinators.g);
                return promiseUtils.delay(0);
            }).then(function () {
                conn.close.callCount.should.equal(1);
            });
        });

        it('should drop the group coordinator when OffsetFetch loses it (GroupAdmin.fetchConsumerLag)', function () {
            var c = makeClient(), conn = lostConn();
            c.groupCoordinators.g = cachedLookup(conn);

            return rejected(c.offsetFetchRequestV1('g', [{ topicName: 't', partitions: [0] }])).then(function () {
                should.not.exist(c.groupCoordinators.g);
            });
        });

        it('should drop the group coordinator when DescribeGroups answers NotCoordinatorForGroup', function () {
            var c = makeClient();
            var conn = fakeConn('coord:1', function () { return Promise.resolve(Buffer.alloc(0)); });
            c.groupCoordinators.g = cachedLookup(conn);
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
            c.transactionCoordinators.tx = cachedLookup(conn);

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

        it('should not drop the coordinator that replaced one a late request failed on', function () {
            var c = makeClient(), fail, request, newConn = fakeConn('new:1'), newer = cachedLookup(newConn);
            var oldConn = fakeConn('old:1', function () {
                return new Promise(function (resolve, reject) { fail = reject; });
            });
            newConn.close = sinon.spy();
            c.groupCoordinators.g = cachedLookup(oldConn);

            request = rejected(c.txnOffsetCommitRequest('tx', 'g', 1, 0, [{ topic: 't', partition: 0, offset: 1 }]));
            return promiseUtils.delay(0).then(function () {
                c.updateGroupCoordinator('g'); // another request found the old one gone
                c.groupCoordinators.g = newer; // and looked the coordinator up again
                fail(new errors.NoKafkaConnectionError('old:1', 'Connection closed'));
                return request;
            }).then(function () {
                c.groupCoordinators.g.should.equal(newer);
                return promiseUtils.delay(0);
            }).then(function () {
                newConn.close.callCount.should.equal(0);
            });
        });

        [
            ['ShareFetch', 'ShareFetchRequestV1', function (c, acks) {
                return c.shareFetchRequest('g', 'm', { 0: 2 }, { 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] }, null, acks);
            }, function (acks) { return acks.t; }],
            ['ShareAcknowledge', 'ShareAcknowledgeRequestV1', function (c, acks) {
                return c.shareAcknowledgeRequest('g', 'm', -1, { 0: [{ topicName: 't', partitions: [{ partitionIndex: 0, acknowledgementBatches: acks.t[0] }] }] });
            }]
        ].forEach(function (r) {
            it('should not send ' + r[0] + ' acks for a topic since recreated under another ID', function () {
                var c = makeClient(), writer = c.protocol.write(), sent;
                var acks = { t: { 0: [
                    { firstOffset: 1, lastOffset: 1, acknowledgeTypes: [1], topicId: 'id-old' },
                    { firstOffset: 2, lastOffset: 2, acknowledgeTypes: [1], topicId: 'id-new' }
                ] } };
                c.topicIds.t = 'id-new';
                c.brokerConnections = { 0: fakeConn('b:1', function () { return Promise.reject(new errors.NoKafkaConnectionError('b:1', 'down')); }) };
                sinon.spy(writer, r[1]);

                return r[2](c, acks).catch(function () { return null; }).then(function () {
                    sent = writer[r[1]].args[0][0].topics[0].partitions[0].acknowledgementBatches;
                    writer[r[1]].restore();
                    sent.map(function (b) { return b.firstOffset; }).should.eql([2]);
                });
            });
        });

        it('should open no coordinator connection for a lookup answered just as it ends', function () {
            var c = makeClient(), lookup;
            c.initialBrokers = [];
            sinon.stub(c, '_sendToAnyBroker').returns(Promise.resolve({ coordinatorHost: 'h', coordinatorPort: 1 }));
            sinon.stub(c, '_createConnection');

            lookup = c._findGroupCoordinator('g').then(function () { return 'resolved'; }, function (err) { return err; });
            return c.end().then(function () {
                return lookup;
            }).then(function (r) {
                r.should.be.instanceOf(errors.NoKafkaConnectionError);
                c._createConnection.callCount.should.equal(0);
            });
        });

        it('should not rebootstrap, or reconnect from a late response, after end()', function () {
            var c = new Kafka.Producer({ clientId: 'client-unit', rebootstrap: true }).client, fail, refresh;
            sinon.stub(c, 'metadataRequest').returns(new Promise(function (resolve, reject) { fail = reject; }));
            sinon.stub(c, '_rebootstrap').returns(Promise.resolve());
            sinon.stub(c, '_createConnection');

            refresh = c.updateMetadata(['t']).catch(function () { return null; });
            return promiseUtils.delay(0).then(function () {
                c.metadataRequest.callCount.should.equal(1); // in flight
                return c.end();
            }).then(function () {
                fail(new errors.NoKafkaConnectionError('all', 'closed')); // the shutdown closed its connections
                return refresh;
            }).then(function () {
                c._rebootstrap.callCount.should.equal(0);
                c._createConnection.callCount.should.equal(0);
            });
        });

        it('should open no broker connections from a metadata response that lands after end()', function () {
            var c = makeClient(), answer, refresh;
            sinon.stub(c, 'metadataRequest').returns(new Promise(function (resolve) { answer = resolve; }));
            sinon.stub(c, '_createConnection');

            refresh = c.updateMetadata(['t']);
            return promiseUtils.delay(0).then(function () {
                return c.end();
            }).then(function () {
                answer({ broker: [{ nodeId: 0, host: 'h', port: 1 }], topicMetadata: [] });
                return refresh;
            }).then(function () {
                c._createConnection.callCount.should.equal(0);
            });
        });

        it('should not wait for a coordinator lookup still in progress when it ends', function () {
            var c = makeClient();
            c.initialBrokers = [];
            c.groupCoordinators.g = new Promise(function () {}); // FindCoordinator still in flight

            return c.end();
        });

        it('should close a coordinator socket still waiting for its ApiVersions reply when it ends', function () {
            var c = makeClient(), conn = fakeConn('coord:1');
            conn.close = sinon.spy();
            c.initialBrokers = [];
            sinon.stub(c, '_sendToAnyBroker').returns(Promise.resolve({ coordinatorHost: 'h', coordinatorPort: 1 }));
            sinon.stub(c, '_createConnection').returns(conn);
            sinon.stub(c, 'apiVersionsRequest').returns(new Promise(function () {})); // a silent broker

            c._findGroupCoordinator('g');
            return promiseUtils.delay(0).then(function () {
                return c.end();
            }).then(function () {
                conn.close.callCount.should.be.at.least(1);
            });
        });

        it('should report a broker\'s ShareAcknowledge failure on its partitions and wait for the others', function () {
            var c = makeClient(), finish;
            c.brokerConnections = {
                0: fakeConn('b0:1', function () { return Promise.reject(new errors.NoKafkaConnectionError('b0:1', 'down')); }),
                1: fakeConn('b1:1', function () { return new Promise(function (resolve) { finish = resolve; }); })
            };
            sinon.stub(c.protocol, 'read').returns({
                ShareAcknowledgeResponseV1: function () {
                    return { result: { error: null, responses: [{ topicId: 'id', partitions: [{ partitionIndex: 1, error: null }] }] } };
                }
            });
            c.topicNames.id = 't';

            return Promise.all([
                c.shareAcknowledgeRequest('g', 'm', -1, {
                    0: [{ topicName: 't', partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }],
                    1: [{ topicName: 't', partitions: [{ partitionIndex: 1, acknowledgementBatches: [] }] }]
                }),
                promiseUtils.delay(5).then(function () { finish(Buffer.alloc(0)); })
            ]).then(function (r) {
                r[0].map(function (x) { return [x.partition, x.error ? x.error.name : null]; })
                    .should.eql([[0, 'NoKafkaConnectionError'], [1, null]]);
            });
        });

        it('should keep a broker\'s ShareAcknowledge decode failure to its own partitions', function () {
            var c = makeClient(), finish;
            c.brokerConnections = {
                0: fakeConn('b0:1', function () { return Promise.resolve(Buffer.from('bad')); }),
                1: fakeConn('b1:1', function () { return new Promise(function (resolve) { finish = resolve; }); })
            };
            sinon.stub(c.protocol, 'read').callsFake(function (buffer) {
                if (buffer.toString() === 'bad') { throw new RangeError('Trying to access beyond buffer length'); }
                return { ShareAcknowledgeResponseV1: function () {
                    return { result: { error: null, responses: [{ topicId: 'id', partitions: [{ partitionIndex: 1, error: null }] }] } };
                } };
            });
            c.topicNames.id = 't';

            return Promise.all([
                c.shareAcknowledgeRequest('g', 'm', -1, {
                    0: [{ topicName: 't', partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }],
                    1: [{ topicName: 't', partitions: [{ partitionIndex: 1, acknowledgementBatches: [] }] }]
                }),
                promiseUtils.delay(5).then(function () { finish(Buffer.from('ok')); })
            ]).then(function (r) {
                r[0].map(function (x) { return [x.partition, x.error ? x.error.name : null]; }).should.eql([[0, 'RangeError'], [1, null]]);
            });
        });

        it('should dispatch ShareFetch in the same turn it is called', function () {
            var c = makeClient(), sent = false, p;
            c.brokerConnections = { 0: fakeConn('b0:1', function () { sent = true; return Promise.reject(new errors.NoKafkaConnectionError('b0:1', 'down')); }) };

            p = c.shareFetchRequest('g', 'm', { 0: 1 }, { 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] }, null, null);
            sent.should.equal(true); // no microtask in which end() could run first
            return p;
        });

        it('should send ShareFetch without waiting for a metadata refresh in flight', function () {
            var c = makeClient(), sent = false;
            c._updateMetadata_pending = true;
            c._updateMetadata_running = new Promise(function () {}); // its caller has already waited; a new one started since
            c.brokerConnections = { 0: fakeConn('b0:1', function () { sent = true; return Promise.reject(new errors.NoKafkaConnectionError('b0:1', 'down')); }) };

            return c.shareFetchRequest('g', 'm', { 0: 1 }, { 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] }, null, null).then(function () {
                sent.should.equal(true);
            });
        });

        it('should send ShareAcknowledge without waiting for a metadata refresh in flight', function () {
            var c = makeClient(), sent = false;
            c._updateMetadata_pending = true;
            c._updateMetadata_running = new Promise(function () {}); // stuck on a silent broker
            c.brokerConnections = { 0: fakeConn('b0:1', function () { sent = true; return Promise.reject(new errors.NoKafkaConnectionError('b0:1', 'down')); }) };

            return c.shareAcknowledgeRequest('g', 'm', -1, { 0: [] }).then(function () {
                sent.should.equal(true);
            });
        });

        it('should send ShareAcknowledge under the topic ID it is given', function () {
            var c = makeClient(), writer = c.protocol.write(), topics;
            c.topicIds.t = '00000000-0000-0000-0000-0000000000aa'; // a lagging cache
            c.brokerConnections = { 0: fakeConn('b:1', function () { return Promise.reject(new errors.NoKafkaConnectionError('b:1', 'down')); }) };
            sinon.spy(writer, 'ShareAcknowledgeRequestV1');

            return c.shareAcknowledgeRequest('g', 'm', -1, { 0: [{ topicName: 't', topicId: '00000000-0000-0000-0000-0000000000bb', partitions: [{
                partitionIndex: 0, acknowledgementBatches: [{ firstOffset: 1, lastOffset: 1, acknowledgeTypes: [1], topicId: '00000000-0000-0000-0000-0000000000bb' }]
            }] }] }).then(function () {
                topics = writer.ShareAcknowledgeRequestV1.args[0][0].topics;
                writer.ShareAcknowledgeRequestV1.restore();
                topics[0].topicId.should.equal('00000000-0000-0000-0000-0000000000bb');
                topics[0].partitions[0].acknowledgementBatches.should.have.length(1); // not filtered out as stale
            });
        });

        it('should report a close-only ShareAcknowledge failure, and a broker it has no connection to', function () {
            var c = makeClient();
            c.brokerConnections = { 0: fakeConn('b0:1', function () { return Promise.reject(new errors.NoKafkaConnectionError('b0:1', 'down')); }) };

            return c.shareAcknowledgeRequest('g', 'm', -1, { 0: [], 7: [] }).then(function (r) {
                r.map(function (x) { return [x.topic, x.error && x.error.name]; }).should.eql([[undefined, 'NoKafkaConnectionError'], [undefined, 'KafkaError']]);
            });
        });

        it('should drop the group coordinator when a share group heartbeat answers NotCoordinatorForGroup', function () {
            var c = makeClient(), conn = lostConn();
            conn.apiVersions = { 76: { min: 1, max: 1 } };
            conn.send = function () { return Promise.resolve(Buffer.alloc(0)); };
            c.groupCoordinators.g = cachedLookup(conn);
            sinon.stub(c.protocol, 'read').returns({
                ShareGroupHeartbeatResponseV1: function () { return { result: { error: errors.byName('NotCoordinatorForGroup') } }; }
            });

            return rejected(c.shareGroupHeartbeatRequest('g', 'm', 1, null, ['t'])).then(function (err) {
                err.code.should.equal('NotCoordinatorForGroup');
                should.not.exist(c.groupCoordinators.g); // the next heartbeat looks it up again
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

        it('should split a Produce mixing topics with and without IDs, keeping v13 for those with', function () {
            var c = makeClient(), versions = [];
            c.topicIds.a = '00000000-0000-0000-0000-0000000000aa';
            c.brokerConnections = { 0: fakeConn('b:1', function (id, buffer) {
                versions.push(Buffer.from(buffer).readInt16BE(2));
                return Promise.reject(new errors.NoKafkaConnectionError('b:1', 'down'));
            }) };
            c.brokerConnections[0].apiVersions = { 0: { min: 0, max: 13 } };

            return c.produceRequest([
                { topic: 'a', partition: 0, leader: 0, message: { value: 'x' } },
                { topic: 'b', partition: 0, leader: 0, message: { value: 'y' } } // no known ID
            ], 0).then(function (r) {
                versions.sort().should.eql([12, 13]); // 'a' still checked by ID on the wire
                r.map(function (x) { return x.topic; }).sort().should.eql(['a', 'b']);
            });
        });

        it('should send Produce by name when a topic has no known ID, though the broker has v13', function () {
            var c = makeClient(), sent;
            c.brokerConnections = { 0: fakeConn('b:1', function (id, buffer) { sent = Buffer.from(buffer); return Promise.reject(new errors.NoKafkaConnectionError('b:1', 'down')); }) };
            c.brokerConnections[0].apiVersions = { 0: { min: 0, max: 13 } };

            return c.produceRequest([{ topic: 't', partition: 0, leader: 0, message: { value: 'x' } }], 0).then(function () {
                sent.readInt16BE(2).should.equal(12); // v13 would send the zero UUID
            });
        });

        it('should send an unstamped Produce v13 to the topic ID known after compression', function () {
            var cl = new Kafka.Producer({ clientId: 'client-unit', asyncCompression: true }).client;
            var NEW = '00000000-0000-0000-0000-0000000000bb';
            var compression = require('../lib/protocol/misc/compression'), compress = compression.compressAsync;
            var writer = cl.protocol.write();
            cl.topicIds.t = '00000000-0000-0000-0000-0000000000aa';
            cl.brokerConnections = { 0: fakeConn('b:1', function () { return Promise.reject(new errors.NoKafkaConnectionError('b:1', 'down')); }) };
            cl.brokerConnections[0].apiVersions = { 0: { min: 0, max: 13 } };
            sinon.stub(compression, 'compressAsync').callsFake(function () {
                cl.topicIds.t = NEW; // a refresh lands while the batch compresses
                return compress.apply(this, arguments);
            });
            sinon.spy(writer, 'ProduceRequestV13');

            return cl.produceRequest([{ topic: 't', partition: 0, leader: 0, message: { value: 'x' } }], 1).finally(function () {
                compression.compressAsync.restore();
            }).then(function () {
                var topics = writer.ProduceRequestV13.args[0][0].topics;
                writer.ProduceRequestV13.restore();
                topics[0].topicId.should.equal(NEW);
            });
        });

        it('should tie a stamped request to the topic ID it is sent with, when it had none', function () {
            var cl = new Kafka.Producer({ clientId: 'client-unit', asyncCompression: true }).client, request;
            var compression = require('../lib/protocol/misc/compression'), compress = compression.compressAsync;
            cl.brokerConnections = { 0: fakeConn('b:1', function () { return Promise.reject(new errors.NoKafkaConnectionError('b:1', 'down')); }) };
            cl.brokerConnections[0].apiVersions = { 0: { min: 0, max: 13 } };
            sinon.stub(compression, 'compressAsync').callsFake(function () {
                cl.topicIds.t = '00000000-0000-0000-0000-0000000000aa'; // metadata first learns the ID now
                return compress.apply(this, arguments);
            });

            request = { topic: 't', partition: 0, leader: 0, _baseSequence: 0, message: { value: 'x' } }; // stamped with no ID
            return cl.produceRequest([request], 1).finally(function () {
                compression.compressAsync.restore();
            }).then(function () {
                request._topicId.should.equal('00000000-0000-0000-0000-0000000000aa'); // a retry to a recreated topic is caught
            });
        });

        [12, 13].forEach(function (v) {
            it('should not send a stamped Produce v' + v + ' batch whose topic was recreated while it compressed', function () {
                var cl = new Kafka.Producer({ clientId: 'client-unit', asyncCompression: true }).client, sent = 0, request;
                var compression = require('../lib/protocol/misc/compression'), compress = compression.compressAsync;
                cl.topicIds.t = '00000000-0000-0000-0000-0000000000aa';
                cl.brokerConnections = { 0: fakeConn('b:1', function () { sent++; return Promise.resolve(Buffer.alloc(0)); }) };
                cl.brokerConnections[0].apiVersions = { 0: { min: 0, max: v } };
                sinon.stub(compression, 'compressAsync').callsFake(function () {
                    cl.topicIds.t = '00000000-0000-0000-0000-0000000000bb';
                    return compress.apply(this, arguments);
                });

                request = { topic: 't', partition: 0, leader: 0, _baseSequence: 0, _topicId: '00000000-0000-0000-0000-0000000000aa', message: { value: 'x' } };
                return cl.produceRequest([request], 1).finally(function () {
                    compression.compressAsync.restore();
                }).then(function (r) {
                    sent.should.equal(0); // by name (v12) it would have landed in the new log
                    r.should.have.length(1);
                    r[0].error.code.should.equal('UnknownTopicId');
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
