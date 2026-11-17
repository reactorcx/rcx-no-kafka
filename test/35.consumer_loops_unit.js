'use strict';

/* global describe, it, sinon, should */

var Kafka        = require('../lib/index');
var errors       = require('../lib/errors');
var promiseUtils = require('../lib/promise-utils');
var Protocol     = require('../lib/protocol');
var protocol     = new Protocol({ bufferSize: 65536 });

function noop() { return Promise.resolve(); }

describe('Consumer loops (unit)', function () {
    describe('BaseConsumer handler failures', function () {
        function makeConsumer(handler) {
            var c = new Kafka.SimpleConsumer({ clientId: 'unit-consumer' });
            sinon.stub(c.client, 'fetchRequest').returns(Promise.resolve([
                { topic: 't', partition: 0, messageSet: [{ offset: 5 }], highwaterMarkOffset: 6 }
            ]));
            c.subscriptions['t:0'] = {
                topic: 't', partition: 0, offset: 5, leader: 0, maxBytes: 1024,
                handler: handler
            };
            return c;
        }

        it('should not advance the offset when the handler rejects (batch is redelivered)', function () {
            var c = makeConsumer(function () { return Promise.reject(new Error('db down')); });
            var p = c._fetch();
            c._closed = true;
            return p.then(function () {
                c.subscriptions['t:0'].offset.should.equal(5);
                c.subscriptions['t:0'].paused.should.equal(false);
            });
        });

        it('should advance the offset when the handler succeeds', function () {
            var c = makeConsumer(noop);
            var p = c._fetch();
            c._closed = true;
            return p.then(function () {
                c.subscriptions['t:0'].offset.should.equal(6);
                c.subscriptions['t:0'].paused.should.equal(false);
            });
        });
        it('should not apply an in-flight fetch to an entry re-subscribed meanwhile', function () {
            var delivered = [];
            var c = new Kafka.SimpleConsumer({ clientId: 'unit-consumer' });
            var p;
            c.subscriptions['t:0'] = { topic: 't', partition: 0, offset: 110, leader: 0, maxBytes: 1024, handler: noop };
            sinon.stub(c.client, 'fetchRequest').callsFake(function () {
                // a rebalance re-subscribes at the committed offset while this fetch is in flight
                c.subscriptions['t:0'] = {
                    topic: 't', partition: 0, offset: 100, leader: 0, maxBytes: 1024,
                    handler: function (ms) { delivered.push(ms); return Promise.resolve(); }
                };
                return Promise.resolve([{ topic: 't', partition: 0, messageSet: [{ offset: 110 }], nextOffset: 111, highwaterMarkOffset: 120 }]);
            });
            p = c._fetch();
            c._closed = true;
            return p.then(function () {
                delivered.should.have.length(0);
                c.subscriptions['t:0'].offset.should.equal(100);
            });
        });

        it('should not re-wrap the handler on every re-subscribe', function () {
            var c = new Kafka.SimpleConsumer({ clientId: 'unit-consumer' });
            var handler = noop, i;
            sinon.stub(c.client, 'findLeader').returns(Promise.resolve(0));
            function again() {
                return c.subscribe('t', 0, { offset: 5 }, handler).then(function () {
                    handler = c.subscriptions['t:0'].handler; // what _updateSubscription passes back
                });
            }
            return again().then(function () {
                var first = handler, p = Promise.resolve();
                for (i = 0; i < 3; i++) { p = p.then(again); }
                return p.then(function () { handler.should.equal(first); });
            });
        });

        it('should move past a fetch with nothing deliverable (e.g. a transaction marker)', function () {
            var c = makeConsumer(noop);
            var p;
            c.client.fetchRequest.returns(Promise.resolve([{ topic: 't', partition: 0, messageSet: [], nextOffset: 6, highwaterMarkOffset: 6 }]));
            p = c._fetch();
            c._closed = true;
            return p.then(function () {
                c.subscriptions['t:0'].offset.should.equal(6);
            });
        });
    });

    describe('heartbeat loop crash safety', function () {
        it('GroupConsumer: FencedInstanceId from the heartbeat loop must not produce an unhandled rejection', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-crash' });
            var unhandled = [];
            function onUnhandled(reason) { unhandled.push(reason); }

            process.on('unhandledRejection', onUnhandled);

            gc.strategies = { S: { handler: noop } };
            sinon.stub(gc.client, 'updateGroupCoordinator').returns(Promise.resolve());
            sinon.stub(gc.client, 'joinConsumerGroupRequest').returns(Promise.resolve({
                memberId: 'me', leaderId: 'other', generationId: 1, members: [], groupProtocol: 'S'
            }));
            sinon.stub(gc.client, 'syncConsumerGroupRequest').returns(Promise.resolve({ memberAssignment: null }));
            sinon.stub(gc.client, 'heartbeatRequest').callsFake(function () {
                return Promise.reject(errors.byName('FencedInstanceId'));
            });

            return gc._fullRejoin().then(function () {
                return promiseUtils.delay(100);
            }).then(function () {
                clearTimeout(gc._heartbeatTimeout);
                process.removeListener('unhandledRejection', onUnhandled);
                gc._closed.should.equal(true);
                unhandled.should.have.length(0);
            });
        });
    });

    describe('ShareConsumer rejoin/session handling', function () {
        function makeShareConsumer() {
            var sc = new Kafka.ShareConsumer({ clientId: 'unit-sc', groupId: 'sc-unit' });
            sc._topics = ['t'];
            sc._handler = noop;
            return sc;
        }

        it('should start only one fetch loop across rejoins', function () {
            var sc = makeShareConsumer();
            sinon.stub(sc.client, 'updateGroupCoordinator').returns(Promise.resolve());
            sinon.stub(sc.client, 'shareGroupHeartbeatRequest').returns(Promise.resolve({
                memberId: sc.memberId, memberEpoch: 1, heartbeatIntervalMs: 100000, assignment: null
            }));
            sinon.stub(sc, '_heartbeat').returns(Promise.resolve());
            sinon.stub(sc, '_fetch').returns(Promise.resolve());

            return sc._join().then(function () {
                return sc._join(); // a rejoin (e.g. after UnknownMemberId)
            }).then(function () {
                sc._fetch.callCount.should.equal(1);
                sc._closed = true;
            });
        });

        it('should requeue drained acknowledgements when the fetch fails', function () {
            var sc = makeShareConsumer();
            var p;
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.reject(new Error('boom')));

            sc.shareSessionEpochs = { 0: 1 }; // an open session, so the acks go out with this fetch
            sc.acknowledge({ topic: 't', partition: 0, offset: 5 });
            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                sc._pendingAcks.should.have.property('t:0');
                sc._pendingAcks['t:0'].batches.should.have.length(1);
            });
        });

        it('should not clobber a session reset that happens while a fetch is in flight', function () {
            var sc = makeShareConsumer();
            var p;
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function () {
                sc._resetShareSessions(); // assignment changed while the fetch was in flight
                return Promise.resolve([]);
            });

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                (sc.shareSessionEpochs['0'] || 0).should.equal(0);
            });
        });

        it('should reopen the session on InvalidShareSessionEpoch', function () {
            var sc = makeShareConsumer();
            var p;
            sc.shareSessionEpochs = { 0: 5 };
            sc.client.topicMetadata = { t: { 0: { leader: 0 } } };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([
                { topic: 't', partition: 0, error: errors.byName('InvalidShareSessionEpoch'), messageSet: [] }
            ]));

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                should.not.exist(sc.shareSessionEpochs['0']);
            });
        });

        it('should confine a top-level ShareFetch error to its broker and hold acks while it reopens', function () {
            var sc = new Kafka.ShareConsumer({ clientId: 'unit-sc', groupId: 'sc-unit', logger: { logLevel: 0 } });
            var topicId = '0a0b0c0d-0000-0000-0000-000000000001', delivered = [], replies;
            // ShareFetchResponseV1: a top-level error, or t:<partition> with one record at offset 10
            function response(code, partition) {
                // the batch first: the protocol instance has a single writer
                var b = Buffer.from(protocol.write().RecordBatch({ baseOffset: 10, records: [{ value: 'v' }] }).result);
                var w = protocol.write().Int32BE(1).UVarint(0).Int32BE(0).Int16BE(code).UVarint(0).Int32BE(0);
                if (partition === undefined) {
                    w.UVarint(1);
                } else {
                    w.UVarint(2).uuid(topicId).UVarint(2)
                        .Int32BE(partition).Int16BE(0).UVarint(0).Int16BE(0).UVarint(0) // errors, messages
                        .Int32BE(1).Int32BE(0).UVarint(0) // currentLeader
                        .UVarint(b.length + 1).raw(b)
                        .UVarint(2).Int64BE(10).Int64BE(10).Int16BE(1).UVarint(0) // acquiredRecords
                        .UVarint(0).UVarint(0);
                }
                return Buffer.from(w.UVarint(1).UVarint(0).result);
            }
            function broker(id) {
                return { send: function () { return Promise.resolve(replies[id].shift()); } };
            }
            function sent(call, partition) { // [epoch, acks for t:<partition>] as sent to that partition's broker
                var acks = call[5] && call[5].t && call[5].t[partition];
                return [call[2][partition], acks ? acks[0].firstOffset : null];
            }
            replies = { 0: [response(122), response(0), response(0)], 1: [response(0, 1), response(0), response(0)] };

            sc._handler = function (messages, topic, partition) {
                delivered.push(topic + ':' + partition + '@' + messages[0].offset);
                return Promise.resolve();
            };
            sc.shareSessionEpochs = { 0: 5, 1: 3 };
            sc.client.topicMetadata = { t: { 0: { leader: 0 }, 1: { leader: 1 } } };
            sc.client.topicNames = {};
            sc.client.topicNames[topicId] = 't';
            sc.client.brokerConnections = { 0: broker(0), 1: broker(1) };
            sinon.stub(sc, '_buildFetchRequests').returns({
                0: [{ topicName: 't', partitions: [{ partition: 0 }] }],
                1: [{ topicName: 't', partitions: [{ partition: 1 }] }]
            });
            sinon.spy(sc.client, 'shareFetchRequest');
            sc._closed = true; // run fetches by hand, no loop
            sc.acknowledge({ topic: 't', partition: 0, offset: 3 });
            sc.acknowledge({ topic: 't', partition: 1, offset: 4 });

            return sc._fetch().then(function () {
                delivered.should.eql(['t:1@10']); // broker 1's records survive broker 0's error
                should.not.exist(sc.shareSessionEpochs['0']);
                sc.shareSessionEpochs['1'].should.equal(4);
                return sc._fetch();
            }).then(function () {
                return sc._fetch();
            }).then(function () {
                var calls = sc.client.shareFetchRequest.args;
                // broker 0 rejected its acks with the request: they are resent once its new
                // session is open, never on the opening (epoch 0) request
                calls.map(function (c) { return sent(c, 0); }).should.eql([[5, 3], [0, null], [1, 3]]);
                // broker 1 is unaffected: its acks went out the first time
                calls.map(function (c) { return sent(c, 1); }).should.eql([[3, 4], [4, null], [5, null]]);
            });
        });

        it('should resend unapplied acks once per partition, and not those the broker answered', function () {
            var sc = makeShareConsumer();
            var notSent = new errors.NoKafkaConnectionError('b:9092', 'down'), p;
            sc.shareSessionEpochs = { 0: 2 };
            sc.client.topicMetadata = { t: { 0: { leader: 0 }, 1: { leader: 0 } } };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }, { partition: 1 }] }] });
            sinon.stub(sc.client, 'updateMetadata').returns(Promise.resolve());
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function () {
                return Promise.resolve([
                    { topic: 't', partition: 0, error: notSent, messageSet: [] },
                    { topic: 't', partition: 0, error: notSent, messageSet: [] }, // listed twice
                    { topic: 't', partition: 1, error: errors.byName('NotLeaderForPartition'), acknowledgeError: null, messageSet: [] }
                ]);
            });
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });
            sc.acknowledge({ topic: 't', partition: 1, offset: 2 });

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                Object.keys(sc._pendingAcks).should.eql(['t:0']);
                sc._pendingAcks['t:0'].batches.should.have.length(1);
            });
        });

        it('should rejoin on FencedMemberEpoch', function () {
            var sc = makeShareConsumer();
            var epochsSent = [];
            sinon.stub(sc.client, 'updateGroupCoordinator').returns(Promise.resolve());
            sinon.stub(sc.client, 'shareGroupHeartbeatRequest').callsFake(function (groupId, memberId, epoch) {
                epochsSent.push(epoch);
                if (epochsSent.length === 1) {
                    return Promise.reject(errors.byName('FencedMemberEpoch'));
                }
                return Promise.resolve({ memberId: memberId, memberEpoch: 7, heartbeatIntervalMs: 100000, assignment: null });
            });
            sinon.stub(sc, '_fetch').returns(Promise.resolve());

            sc.memberEpoch = 3;
            return sc._heartbeat().then(function () {
                clearTimeout(sc._heartbeatTimeout);
                sc._closed = true;
                epochsSent.slice(0, 2).should.eql([3, 0]);
                sc.memberEpoch.should.equal(7);
            });
        });
    });

    describe('end() racing an in-flight join', function () {
        it('GroupConsumer: a rejoin that lands after end() should leave instead of squatting', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-race' });
            var resolveJoin, rejoinP;

            sinon.stub(gc.client, 'updateGroupCoordinator').returns(Promise.resolve());
            sinon.stub(gc.client, 'leaveGroupRequest').returns(Promise.resolve());
            sinon.stub(gc.client, 'end').returns(Promise.resolve());
            sinon.stub(gc, '_runHeartbeat').returns(Promise.resolve());
            sinon.stub(gc, '_rejoin').callsFake(function () {
                return new Promise(function (resolve) {
                    resolveJoin = function () { gc.memberId = 'me'; resolve(); };
                });
            });

            rejoinP = gc._fullRejoin();
            return promiseUtils.delay(10).then(function () {
                var endP = gc.end(); // memberId is still null here
                resolveJoin();
                return Promise.all([endP, rejoinP]);
            }).then(function () {
                gc.client.leaveGroupRequest.lastCall.args.should.eql(['gc-unit-race', 'me']);
                gc._runHeartbeat.callCount.should.equal(0);
            });
        });

        it('ShareConsumer: a join that lands after end() should leave instead of squatting', function () {
            var sc = new Kafka.ShareConsumer({ clientId: 'unit-sc', groupId: 'sc-unit-race' });
            var epochsSent = [];
            var resolveJoin, joinP;

            sc._topics = ['t'];
            sc._handler = noop;
            sinon.stub(sc.client, 'updateGroupCoordinator').returns(Promise.resolve());
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            sinon.stub(sc.client, 'shareGroupHeartbeatRequest').callsFake(function (gid, mid, epoch) {
                epochsSent.push(epoch);
                if (epochsSent.length === 1) {
                    return new Promise(function (resolve) { resolveJoin = resolve; });
                }
                return Promise.resolve({});
            });

            joinP = sc._join();
            return promiseUtils.delay(10).then(function () {
                var endP = sc.end(); // memberEpoch is still 0, so end() sends no leave
                resolveJoin({ memberId: sc.memberId, memberEpoch: 5, heartbeatIntervalMs: 100000, assignment: null });
                return Promise.all([endP, joinP]);
            }).then(function () {
                clearTimeout(sc._heartbeatTimeout);
                epochsSent.should.eql([0, -1]);
            });
        });
    });

    describe('classic group leader with foreign topics', function () {
        it('should skip topics missing from metadata instead of crashing the leader', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-leader' });
            var updateArgs;

            gc.memberId = 'me';
            gc.leaderId = 'me';
            gc.generationId = 1;
            gc.strategyName = 'S';
            gc.strategies = { S: { strategy: new Kafka.DefaultAssignmentStrategy(), handler: noop } };
            gc.members = [{ id: 'me', subscriptions: ['known', 'missing'], metadata: null }];
            gc.topics = ['known'];
            gc.client.topicMetadata = { known: { 0: { partitionId: 0 } } };

            sinon.stub(gc.client, 'updateMetadata').callsFake(function (topics) {
                updateArgs = topics.slice().sort();
                return Promise.resolve();
            });
            sinon.stub(gc.client, 'syncConsumerGroupRequest').returns(Promise.resolve({ memberAssignment: { partitionAssignment: [] } }));

            return gc._syncGroup().then(function () {
                updateArgs.should.eql(['known', 'missing']);
            });
        });
    });
});
