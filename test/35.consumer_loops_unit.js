'use strict';

/* global describe, it, sinon, should */

var Kafka        = require('../lib/index');
var errors       = require('../lib/errors');
var promiseUtils = require('../lib/promise-utils');

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
    });

    describe('heartbeat loop crash safety', function () {
        it('ConsumerGroup: terminal join failure must not produce an unhandled rejection', function () {
            var cg = new Kafka.ConsumerGroup({ clientId: 'unit-cg', groupId: 'cg-unit-crash' });
            var unhandled = [];
            function onUnhandled(reason) { unhandled.push(reason); }
            this.timeout(8000);

            process.on('unhandledRejection', onUnhandled);

            cg.options.maxJoinAttempts = 1;
            cg.heartbeatIntervalMs = 1;
            sinon.stub(cg.client, 'consumerGroupHeartbeatRequest').callsFake(function () {
                return Promise.reject(errors.byName('FencedMemberEpoch'));
            });
            sinon.stub(cg.client, 'updateGroupCoordinator').callsFake(function () {
                return Promise.reject(new Error('coordinator down'));
            });

            cg._scheduleHeartbeat();

            return promiseUtils.delay(1500).then(function () {
                cg._closed = true;
                clearTimeout(cg._heartbeatTimeout);
                process.removeListener('unhandledRejection', onUnhandled);
                unhandled.should.have.length(0);
            });
        });

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
        it('ConsumerGroup: a join that lands after end() should leave instead of squatting', function () {
            var cg = new Kafka.ConsumerGroup({ clientId: 'unit-cg', groupId: 'cg-unit-race' });
            var resolveJoin;
            var epochsSent = [];
            var joinP;

            cg._topics = ['t'];
            cg._handler = noop;
            sinon.stub(cg.client, 'updateGroupCoordinator').returns(Promise.resolve());
            sinon.stub(cg.client, 'end').returns(Promise.resolve());
            sinon.stub(cg.client, 'consumerGroupHeartbeatRequest').callsFake(function (gid, mid, epoch) {
                epochsSent.push(epoch);
                if (epochsSent.length === 1) {
                    return new Promise(function (resolve) { resolveJoin = resolve; });
                }
                return Promise.resolve({});
            });

            joinP = cg._join();
            return promiseUtils.delay(10).then(function () {
                var endP = cg.end(); // memberEpoch is still 0, so no leave is sent here
                resolveJoin({ memberId: cg.memberId, memberEpoch: 5, heartbeatIntervalMs: 100000, assignment: null });
                return Promise.all([endP, joinP]);
            }).then(function () {
                return promiseUtils.delay(20);
            }).then(function () {
                clearTimeout(cg._heartbeatTimeout);
                epochsSent.should.eql([0, -1]);
            });
        });
    });

    describe('KIP-848 reconciliation retry', function () {
        it('should retry reconciliation on later heartbeats after a failed acquire', function () {
            var cg = new Kafka.ConsumerGroup({ clientId: 'unit-cg', groupId: 'cg-unit-reconcile' });
            var subscribeStub;
            var hbResponses = [
                { memberEpoch: 1, heartbeatIntervalMs: 100000, assignment: { _present: 1, topicPartitions: [{ topicId: 'TID', partitions: [0] }] } },
                { memberEpoch: 1, heartbeatIntervalMs: 100000, assignment: null }
            ];

            cg._topics = ['t'];
            cg._handler = noop;
            cg.client.topicNames.TID = 't';
            sinon.stub(cg.client, 'consumerGroupHeartbeatRequest').callsFake(function () {
                return Promise.resolve(hbResponses.shift() || { memberEpoch: 1, heartbeatIntervalMs: 100000, assignment: null });
            });
            sinon.stub(cg.client, 'updateMetadata').returns(Promise.resolve());
            sinon.stub(cg, 'fetchOffset').returns(Promise.resolve([{ topic: 't', partition: 0, offset: 3 }]));
            subscribeStub = sinon.stub(cg, 'subscribe');
            subscribeStub.onFirstCall().returns(Promise.reject(new Error('leader down')));
            subscribeStub.returns(Promise.resolve());

            return cg._heartbeat().then(function () {
                clearTimeout(cg._heartbeatTimeout);
                Object.keys(cg.owned).should.have.length(0); // first acquire failed
                return cg._heartbeat();
            }).then(function () {
                clearTimeout(cg._heartbeatTimeout);
                cg._closed = true;
                cg.owned.should.have.property('t:0');
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
