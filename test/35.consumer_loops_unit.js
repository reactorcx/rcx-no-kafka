'use strict';

/* global describe, it, sinon, should */

var Kafka        = require('../lib/index');
var errors       = require('../lib/errors');
var promiseUtils = require('../lib/promise-utils');
var Protocol     = require('../lib/protocol');
var protocol     = new Protocol({ bufferSize: 65536 });

function noop() { return Promise.resolve(); }

// acks are taken only for a partition's current delivery: record one holding these offsets
function markDelivered(sc, topic, partition, offsets) {
    sc._delivered({ topic: topic, partition: partition, acquiredRecords: offsets.map(function (o) { return { firstOffset: o, lastOffset: o }; }) });
}

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

        it('should resume a recreated topic at recoveryOffset, not at the deleted log\'s offset', function () {
            var c = makeConsumer(noop);
            c.subscriptions['t:0'].offset = 50;
            c.subscriptions['t:0'].topicId = 'id-old';
            c.client.topicIds.t = 'id-new'; // a by-name refresh found the topic recreated
            sinon.stub(c.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(c.client, 'getPartitionOffset').returns(Promise.resolve(0));
            c._closed = true;

            return c._fetch().then(function () {
                c.client.fetchRequest.callCount.should.equal(0); // not at 50 in the new log
                return promiseUtils.delay(5);
            }).then(function () {
                c.client.getPartitionOffset.args[0][3].should.equal(c.options.recoveryOffset);
                c.subscriptions['t:0'].offset.should.equal(0);
                c.subscriptions['t:0'].topicId.should.equal('id-new');
                c.subscriptions['t:0'].paused.should.equal(false);
            });
        });

        it('should fetch by the topic ID of the subscription\'s offset', function () {
            var c = makeConsumer(noop);
            c.subscriptions['t:0'].topicId = 'id-old';
            c.client.topicIds.t = 'id-old';
            c._closed = true;
            return c._fetch().then(function () {
                c.client.fetchRequest.args[0][0]['0'][0].topicId.should.equal('id-old'); // not what metadata says when it is sent
            });
        });

        it('should bind a subscription made before topic IDs to the first one, without moving it', function () {
            var c = makeConsumer(noop);
            c.client.topicIds.t = 'id1'; // the brokers now give topic IDs
            c._closed = true;
            return c._fetch().then(function () {
                c.client.fetchRequest.callCount.should.equal(1);
                c.subscriptions['t:0'].topicId.should.equal('id1');
                c.subscriptions['t:0'].offset.should.equal(6); // fetched on from 5, not reset
            });
        });

        it('should look an offset up again if its topic was recreated meanwhile', function () {
            var c = new Kafka.SimpleConsumer({ clientId: 'unit-consumer' });
            c.client.topicIds.t = 'id-old';
            sinon.stub(c.client, 'findLeader').returns(Promise.resolve(0));
            sinon.stub(c.client, 'getPartitionOffset').callsFake(function () {
                if (c.client.getPartitionOffset.callCount === 1) {
                    c.client.topicIds.t = 'id-new'; // recreated while the first lookup was out
                    return Promise.resolve(50);
                }
                return Promise.resolve(0);
            });

            return c.subscribe('t', 0, noop).then(function () {
                c.client.getPartitionOffset.callCount.should.equal(2);
                c.subscriptions['t:0'].offset.should.equal(0);
                c.subscriptions['t:0'].topicId.should.equal('id-new');
            });
        });

        it('should keep a re-subscribed offset\'s topic ID across a metadata refresh', function () {
            var c = makeConsumer(noop);
            c.subscriptions['t:0'].topicId = 'id-old';
            sinon.stub(c.client, 'updateMetadata').callsFake(function () { c.client.topicIds.t = 'id-new'; return Promise.resolve(); });
            sinon.stub(c.client, 'findLeader').returns(Promise.resolve(0));

            return c._updateSubscription('t', 0).then(function () {
                c.subscriptions['t:0'].offset.should.equal(5);
                c.subscriptions['t:0'].topicId.should.equal('id-old'); // so the next fetch sees the recreation
            });
        });

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
            sinon.stub(gc.client, '_findGroupCoordinator').returns(Promise.resolve({}));
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
            markDelivered(sc, 't', 0, [5]);
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
            markDelivered(sc, 't', 0, [3]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 3 });
            markDelivered(sc, 't', 1, [4]);
            sc.acknowledge({ topic: 't', partition: 1, offset: 4 });

            return sc._fetch().then(function () {
                delivered.should.eql(['t:1@10']); // broker 1's records survive broker 0's error
                should.not.exist(sc.shareSessionEpochs['0']);
                sc.shareSessionEpochs['1'].should.equal(4);
                return sc._fetch();
            }).then(function () {
                return sc._fetch();
            }).then(function () {
                var calls = sc.client.shareFetchRequest.args; // one request per broker per round
                function to(id) { return calls.filter(function (c) { return c[3][id]; }); }
                // broker 0 rejected its acks with the request: they are resent once its new
                // session is open, never on the opening (epoch 0) request
                to(0).map(function (c) { return sent(c, 0); }).should.eql([[5, 3], [0, null], [1, 3]]);
                // broker 1 is unaffected: its acks went out the first time
                to(1).map(function (c) { return sent(c, 1); }).should.eql([[3, 4], [4, null], [5, null]]);
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
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });
            markDelivered(sc, 't', 1, [2]);
            sc.acknowledge({ topic: 't', partition: 1, offset: 2 });

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                Object.keys(sc._pendingAcks).should.eql(['t:0']);
                sc._pendingAcks['t:0'].batches.should.have.length(1);
            });
        });

        ['UnknownTopicId', 'InconsistentTopicId'].forEach(function (code) {
            it('should refresh metadata on ' + code + ' (topic recreated under a new ID)', function () {
                var sc = makeShareConsumer();
                sinon.stub(sc.client, 'updateMetadata').returns(Promise.resolve());

                return sc._partitionError(errors.byName(code), 't', 0).then(function () {
                    sc.client.updateMetadata.args[0][0].should.eql(['t']);
                });
            });
        });

        function twoLeaders(sc) {
            sc.client.topicMetadata = Object.create(null);
            sc.client.topicMetadata.t = { 0: { partitionId: 0, leader: 0 }, 1: { partitionId: 1, leader: 1 } };
        }

        it('should fetch the partitions it has metadata for while refreshing the rest', function () {
            var sc = makeShareConsumer(), refreshed;
            sc.assignedPartitions = { id1: [0, 1] };
            sc.client.topicNames = { id1: 't' };
            sc.client.topicIds.t = 'id1';
            sc.client.topicMetadata = Object.create(null);
            sc.client.topicMetadata.t = { 0: { partitionId: 0, leader: 0 } }; // partition 1 added since the last refresh
            sinon.stub(sc.client, 'updateMetadata').returns(new Promise(function (resolve) { refreshed = resolve; })); // slow
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([]));

            return sc._fetch().then(function () {
                sc.client.updateMetadata.args[0][0].should.eql(['t']);
                // not held up by the refresh: partition 0 goes out now
                sc.client.shareFetchRequest.args[0][3].should.eql({ 0: [{ topicName: 't', topicId: 'id1', partitions: [{ partition: 0 }] }] });
                sc.client.topicMetadata.t[1] = { partitionId: 1, leader: 0 };
                refreshed();
                return promiseUtils.delay(0);
            }).then(function () {
                sc._closed = true;
                return sc._fetch(); // the next round has partition 1's metadata
            }).then(function () {
                sc.client.shareFetchRequest.args[1][3][0][0].partitions.should.eql([{ partition: 0 }, { partition: 1 }]);
            });
        });

        it('should keep one background metadata refresh in flight however often it polls', function () {
            var sc = makeShareConsumer(), finish, i;
            sinon.stub(sc.client, 'updateMetadata').returns(new Promise(function (resolve) { finish = resolve; })); // stalled
            sc._refreshMetadata(['t']);
            return promiseUtils.delay(0).then(function () {
                for (i = 0; i < 50; i++) { // polls while it is stalled
                    sc._refreshMetadata(['t']);
                    sc._refreshMetadata(['u']);
                }
                sc.client.updateMetadata.callCount.should.equal(1);
                sc.client.updateMetadata.returns(Promise.resolve());
                finish();
                return sc._refreshing;
            }).then(function () {
                sc.client.updateMetadata.callCount.should.equal(2); // once more, for everything asked meanwhile
                sc.client.updateMetadata.args[1][0].sort().should.eql(['t', 'u']);
            });
        });

        it('should refresh metadata while it is another topic\'s than the assignment names', function () {
            var sc = makeShareConsumer();
            sc.assignedPartitions = { 'id-new': [0] }; // the assignment already names the recreated topic
            sc.client.topicNames = { 'id-new': 't' };
            sc.client.topicIds.t = 'id-old'; // a lagging broker still reports the old one
            sc.client.topicMetadata = Object.create(null);
            sc.client.topicMetadata.t = { 0: { partitionId: 0, leader: 0 } };
            sinon.stub(sc.client, 'updateMetadata').returns(Promise.resolve());

            return sc._refreshMissingMetadata().then(function () {
                sc.client.updateMetadata.callCount.should.equal(1);
            });
        });

        it('should not fetch an assigned topic whose metadata is now a recreated topic\'s', function () {
            var sc = makeShareConsumer();
            sc.assignedPartitions = { 'id-old': [0] }; // the heartbeat hasn't brought the new assignment yet
            sc.client.topicNames = { 'id-old': 't', 'id-new': 't' };
            sc.client.topicIds.t = 'id-new'; // the by-name refresh already learned the replacement
            sc.client.topicMetadata = Object.create(null);
            sc.client.topicMetadata.t = { 0: { partitionId: 0, leader: 0 } };

            sc._buildFetchRequests().should.eql({});
        });

        it('should still close, with its acks, a session after an assignment change', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });
            sc._applyAssignment({ _present: 1, topicPartitions: [{ topicId: 'id1', partitions: [0] }] }); // a heartbeat, before the next fetch

            return sc.end().then(function () {
                var requests = sc.client.shareAcknowledgeRequest.args[0][3];
                requests[0][0].partitions[0].acknowledgementBatches[0].firstOffset.should.equal(1);
            });
        });

        it('should forget only the session of the broker that reported it lost', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4, 1: 3 };
            delete sc.client.topicMetadata.t[1]; // a refresh since dropped the partition's metadata

            sc._partitionError(errors.byName('ShareSessionNotFound'), 't', 1, '1');
            sc.shareSessionEpochs.should.eql({ 0: 4 }); // broker 0's session is untouched
        });

        it('should not keep the last fetch\'s records alive for end()', function () {
            var sc = makeShareConsumer(), p;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([{ topic: 't', partition: 0, messageSet: [] }]));

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                return sc._fetchRequest;
            }).then(function (held) {
                should.not.exist(held);
            });
        });

        it('should send a healthy broker\'s final acks while another broker\'s fetch hangs', function () {
            var sc = makeShareConsumer(), clock, ended;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2, 1: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({
                0: [{ topicName: 't', partitions: [{ partition: 0 }] }],
                1: [{ topicName: 't', partitions: [{ partition: 1 }] }]
            });
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function (g, m, e, requests) {
                return requests[1] ? new Promise(function () {}) : Promise.resolve([]); // broker 1 never answers
            });
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });
            sc._pendingAcks['t:0'].batches.length.should.equal(1);

            sc._fetch(); // carries t:0's first ack; broker 0 answers, broker 1 never does
            return promiseUtils.delay(5).then(function () {
                markDelivered(sc, 't', 0, [2]);
                sc.acknowledge({ topic: 't', partition: 0, offset: 2 }); // made while broker 1's fetch hangs
                clock = sinon.useFakeTimers();
                ended = sc.end();
                return Promise.resolve();
            }).then(function () {
                var i, p = Promise.resolve();
                for (i = 0; i < 20; i++) { p = p.then(function () { return Promise.resolve(); }); } // let broker 0's path run
                return p;
            }).then(function () {
                var sentTo = sc.client.shareAcknowledgeRequest.args.map(function (a) { return Object.keys(a[3]); });
                sentTo.should.eql([['0']]); // broker 0 closed, with its ack, before the deadline
                clock.tick(30000);
                clock.restore();
                return ended;
            }).then(function () {
                sc.client.end.callCount.should.equal(1);
            });
        });

        it('should close a session a broker opened while another broker\'s fetch hangs', function () {
            var sc = makeShareConsumer(), clock, ended;
            twoLeaders(sc);
            sc.shareSessionEpochs = {}; // both sessions open with this fetch (epoch 0)
            sinon.stub(sc, '_buildFetchRequests').returns({
                0: [{ topicName: 't', partitions: [{ partition: 0 }] }],
                1: [{ topicName: 't', partitions: [{ partition: 1 }] }]
            });
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function (g, m, e, requests) {
                return requests[1] ? new Promise(function () {}) : Promise.resolve([]); // broker 1 never answers
            });
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());

            sc._fetch();
            return promiseUtils.delay(5).then(function () {
                var i, p = Promise.resolve();
                clock = sinon.useFakeTimers();
                ended = sc.end();
                for (i = 0; i < 20; i++) { p = p.then(function () { return null; }); } // let broker 0's path run
                return p;
            }).then(function () {
                // broker 0's session opened with that fetch (epoch 0 -> 1): it is closed, not skipped
                sc.client.shareAcknowledgeRequest.args.map(function (a) { return Object.keys(a[3]); }).should.eql([['0']]);
                clock.tick(30000);
                clock.restore();
                return ended;
            });
        });

        it('should still close, with its acks, a session whose fetch failed in transit', function () {
            var sc = makeShareConsumer(), p;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 3 };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function () {
                return Promise.reject(new RangeError('Trying to access beyond buffer length')); // a response it can't decode
            });
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });

            p = sc._fetch(); // carries the ack, fails, requeues it
            return p.then(function () {
                return sc.end();
            }).then(function () {
                var requests = sc.client.shareAcknowledgeRequest.args[0][3];
                requests[0][0].partitions[0].acknowledgementBatches[0].firstOffset.should.equal(1);
            });
        });

        it('should try to close a session the first fetch may have opened when its reply was unreadable', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = {}; // epoch 0: this fetch opens the session
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function () {
                return Promise.reject(new RangeError('Trying to access beyond buffer length'));
            });
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());

            return sc._fetch().then(function () {
                return sc.end();
            }).then(function () {
                sc.client.shareAcknowledgeRequest.args.map(function (a) { return Object.keys(a[3]); }).should.eql([['0']]);
            });
        });

        function fetchOnce(sc, result, handler) {
            var p;
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([Object.assign({ topic: 't', topicId: 'id-1', partition: 0, leader: '0' }, result)]));
            sc._handler = handler || noop;
            p = sc._fetch();
            sc._closed = true;
            return p;
        }

        function record(offset) { return { offset: offset, message: { value: Buffer.from('v') } }; }

        it('should forget a partition that moved to another broker, and ignore the old broker\'s reply for it', function () {
            var sc = makeShareConsumer(), calls = [], handled = [], round = 0;
            sc.shareSessionEpochs = { 0: 1 };
            sc._closed = true; // one fetch per call
            sc._handler = function (messageSet, topic, partition) { handled.push([topic, partition, messageSet[0].offset]); };
            sinon.stub(sc, '_buildFetchRequests').callsFake(function () {
                return round === 0 ? { 0: [{ topicName: 't', topicId: 'id1', partitions: [{ partition: 0 }, { partition: 1 }] }] }
                    : { 0: [{ topicName: 't', topicId: 'id1', partitions: [{ partition: 1 }] }], 1: [{ topicName: 't', topicId: 'id1', partitions: [{ partition: 0 }] }] };
            });
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function (g, m, epochs, only, forgotten) {
                var l = Object.keys(only)[0];
                calls.push([l, epochs[l], forgotten]);
                if (round === 0) { return Promise.resolve([]); }
                return Promise.resolve([{ topic: 't', topicId: 'id1', partition: 0, leader: l, // both answer for t:0
                    messageSet: [record(l === '0' ? 5 : 9)], acquiredRecords: [{ firstOffset: l === '0' ? 5 : 9, lastOffset: l === '0' ? 5 : 9, deliveryCount: 1 }] }]);
            });
            return sc._fetch().then(function () {
                round = 1; // t:0 moved to broker 1
                return sc._fetch();
            }).then(function () {
                calls.should.eql([
                    ['0', 1, []],
                    ['0', 2, [{ topicId: 'id1', partitions: [0] }]], // the old session lets go of it
                    ['1', 0, []]
                ]);
                handled.should.eql([['t', 0, 9]]); // the new leader's delivery only
            });
        });

        [['Unknown', undefined], ['InvalidRequest', 3]].forEach(function (c) {
            it('should ' + (c[1] ? 'keep' : 'reopen') + ' the share session after a top-level ' + c[0] + ' error', function () {
                var sc = makeShareConsumer();
                sc.shareSessionEpochs = { 0: 3 };
                sinon.stub(sc.client, 'updateMetadata').returns(Promise.resolve());
                fetchOnce(sc, { notFetched: true, error: errors.byName(c[0]) });
                return promiseUtils.delay(10).then(function () {
                    should.equal(sc.shareSessionEpochs[0], c[1]); // Unknown may come after the session advanced
                    should.equal(sc._reopening[0], c[1] ? undefined : true);
                });
            });
        });

        it('should dispatch a fetch of many empty partitions quickly', function () {
            var sc = makeShareConsumer(), partitions = [], results = [], i, t0;
            for (i = 0; i < 30000; i++) {
                partitions.push({ partition: i });
                results.push({ topic: 't', topicId: 'id1', partition: i, leader: '0', messageSet: [], acquiredRecords: [] });
            }
            sc._closed = true;
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', topicId: 'id1', partitions: partitions }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve(results));
            t0 = Date.now();
            return sc._fetch().then(function () {
                (Date.now() - t0).should.be.below(200); // shift() per result was ~500 ms
            });
        });

        it('should acknowledge acquired offsets that hold no record as gaps', function () {
            var sc = makeShareConsumer();
            var result = { messageSet: [record(0), record(3)], acquiredRecords: [{ firstOffset: 0, lastOffset: 4, deliveryCount: 1 }] };
            function handler(messageSet, topic, partition) {
                return sc.acknowledge(messageSet.map(function (m) { return { topic: topic, partition: partition, offset: m.offset }; }));
            }
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            return fetchOnce(sc, result, handler).then(function () {
                var acks = sc._drainPendingAcks().t[0];
                acks.map(function (b) { return [b.firstOffset, b.lastOffset, b.acknowledgeTypes[0]]; })
                    .should.eql([[0, 0, 1], [1, 2, 0], [3, 3, 1], [4, 4, 0]]); // ACCEPT records, GAP the rest, in order
            });
        });

        it('should acknowledge a delivery of gaps only, without calling the handler', function () {
            var sc = makeShareConsumer(), called = false;
            var result = { messageSet: [], acquiredRecords: [{ firstOffset: 1, lastOffset: 1, deliveryCount: 1 }] };
            function handler() { called = true; return Promise.resolve(); }
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            return fetchOnce(sc, result, handler).then(function () {
                called.should.equal(false);
                sc._drainPendingAcks().t[0].map(function (b) { return [b.firstOffset, b.acknowledgeTypes[0]]; }).should.eql([[1, 0]]);
            });
        });

        it('should release only the records a failed handler hadn\'t acknowledged, keeping the gaps', function () {
            var sc = makeShareConsumer();
            var result = { messageSet: [record(0), record(1), record(3)], acquiredRecords: [{ firstOffset: 0, lastOffset: 4, deliveryCount: 1 }] };
            function handler(messageSet, topic, partition) {
                sc.acknowledge({ topic: topic, partition: partition, offset: 0 }); // processed 0, then failed
                return Promise.reject(new Error('handler bug'));
            }
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            return fetchOnce(sc, result, handler).then(function () {
                sc._drainPendingAcks().t[0].map(function (b) { return [b.firstOffset, b.lastOffset, b.acknowledgeTypes[0]]; })
                    .should.eql([[0, 0, 1], [1, 1, 2], [2, 2, 0], [3, 3, 2], [4, 4, 0]]); // ACCEPT, RELEASE records, GAP the rest
            });
        });

        it('should release a record a failed handler took off its array', function () {
            var sc = makeShareConsumer();
            var result = { messageSet: [record(0), record(1)], acquiredRecords: [{ firstOffset: 0, lastOffset: 1, deliveryCount: 1 }] };
            function handler(messageSet) {
                messageSet.shift(); // takes 0 off, then fails before acknowledging it
                return Promise.reject(new Error('handler bug'));
            }
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            return fetchOnce(sc, result, handler).then(function () {
                sc._drainPendingAcks().t[0].map(function (b) { return [b.firstOffset, b.lastOffset, b.acknowledgeTypes[0]]; })
                    .should.eql([[0, 1, 2]]); // both released, not 0 left locked
            });
        });

        it('should work out a failed handler\'s releases quickly, however scattered', function () {
            var sc = makeShareConsumer(), records = [], i, t0;
            for (i = 0; i < 5000; i++) { records.push(record(i * 2)); } // a compaction gap after each
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            function handler(messageSet, topic, partition) {
                messageSet.filter(function (m, j) { return j % 2 === 0; }).forEach(function (m) {
                    sc.acknowledge({ topic: topic, partition: partition, offset: m.offset });
                });
                t0 = Date.now();
                return Promise.reject(new Error('handler bug'));
            }
            return fetchOnce(sc, { messageSet: records, acquiredRecords: [{ firstOffset: 0, lastOffset: 9998, deliveryCount: 1 }] }, handler).then(function () {
                (Date.now() - t0).should.be.below(100); // was ~400 ms
            });
        });

        it('should keep the gap acks inside a range a handler releases', function () {
            var sc = makeShareConsumer();
            var result = { messageSet: [record(0), record(3)], acquiredRecords: [{ firstOffset: 0, lastOffset: 4, deliveryCount: 1 }] };
            function handler(messageSet, topic, partition) {
                return sc.acknowledge({ topic: topic, partition: partition, firstOffset: 0, lastOffset: 4 }, Kafka.ShareConsumer.ACKNOWLEDGE_TYPE.RELEASE);
            }
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            return fetchOnce(sc, result, handler).then(function () {
                sc._drainPendingAcks().t[0].map(function (b) { return [b.firstOffset, b.lastOffset, b.acknowledgeTypes[0]]; })
                    .should.eql([[0, 0, 2], [1, 2, 0], [3, 3, 2], [4, 4, 0]]); // RELEASE records only, missing ones archived
            });
        });

        it('should prune queued acks against many acquired ranges quickly', function () {
            var sc = makeShareConsumer(), ranges = [], acks = [], i, t0;
            for (i = 0; i < 5000; i++) {
                ranges.push({ firstOffset: i * 2, lastOffset: i * 2 });
                acks.push({ topic: 't', partition: 0, offset: i * 2 });
            }
            sc._delivered({ topic: 't', partition: 0, acquiredRecords: ranges });
            sc.acknowledge(acks);
            t0 = Date.now();
            sc._delivered({ topic: 't', partition: 0, acquiredRecords: ranges.map(function (r) { return { firstOffset: r.firstOffset + 20001, lastOffset: r.lastOffset + 20001 }; }) });
            (Date.now() - t0).should.be.below(60); // was ~125 ms
            sc._pendingAcks['t:0'].batches.length.should.equal(5000); // none overlap: all kept
        });

        it('should let a range ack take the place of the gap acks inside it', function () {
            var sc = makeShareConsumer();
            var result = { messageSet: [record(0), record(3)], acquiredRecords: [{ firstOffset: 0, lastOffset: 4, deliveryCount: 1 }] };
            function handler(messageSet, topic, partition) {
                return sc.acknowledge({ topic: topic, partition: partition, firstOffset: 0, lastOffset: 3 });
            }
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            return fetchOnce(sc, result, handler).then(function () {
                sc._drainPendingAcks().t[0].map(function (b) { return [b.firstOffset, b.lastOffset, b.acknowledgeTypes[0]]; })
                    .should.eql([[0, 3, 1], [4, 4, 0]]); // no overlapping batches
            });
        });

        it('should send final acks to the broker that delivered the records, not the current leader', function () {
            var sc = makeShareConsumer();
            var result = { messageSet: [record(5)], acquiredRecords: [{ firstOffset: 5, lastOffset: 5, deliveryCount: 1 }] };
            function handler(messageSet, topic, partition) {
                sc.client.topicMetadata.t[0] = { partitionId: 0, leader: 1 }; // the partition moves meanwhile
                return sc.acknowledge({ topic: topic, partition: partition, offset: 5 });
            }
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2, 1: 2 };
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            return fetchOnce(sc, result, handler).then(function () {
                sc._closed = false;
                return sc.end();
            }).then(function () {
                var sent = {};
                sc.client.shareAcknowledgeRequest.args.forEach(function (a) { Object.assign(sent, a[3]); });
                sent[0][0].partitions[0].acknowledgementBatches[0].firstOffset.should.equal(5);
                sent[1].should.eql([]); // broker 1 only gets its session closed
            });
        });

        it('should drop queued acks for a partition delivered by another broker since', function () {
            var sc = makeShareConsumer();
            sinon.spy(sc.client, 'warn');
            sc._delivered({ topic: 't', partition: 0, leader: '0', acquiredRecords: [{ firstOffset: 1, lastOffset: 1 }] });
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });
            sc._delivered({ topic: 't', partition: 0, leader: '1', acquiredRecords: [{ firstOffset: 9, lastOffset: 9 }] });

            should.not.exist(sc._pendingAcks['t:0']);
            sc.client.warn.args.some(function (a) { return /moved from broker/.test(a[0] + ' ' + a.slice(1).join(' ')); }).should.equal(true);
        });

        it('should keep end()\'s deadline when the wall clock goes back', function () {
            var sc = makeShareConsumer(), clock, ended, done = false;
            sc.memberEpoch = 5;
            sinon.stub(sc.client, 'shareGroupHeartbeatRequest').returns(new Promise(function () {})); // the leave stalls
            sinon.stub(sc.client, 'end').returns(Promise.resolve());

            clock = sinon.useFakeTimers(100000);
            ended = sc.end().then(function () { done = true; });
            clock.setSystemTime(40000); // the wall clock jumps back a minute
            return Promise.resolve().then(function () {
                var i, p = Promise.resolve();
                for (i = 0; i < 10; i++) { p = p.then(function () { return null; }); }
                return p;
            }).then(function () {
                clock.tick(30000);
                clock.restore();
                return Promise.race([ended, promiseUtils.delay(20)]);
            }).then(function () {
                done.should.equal(true); // within 30 s, not 90
            });
        });

        it('should leave the group even when a broker never answers the final acks', function () {
            var sc = makeShareConsumer(), clock, ended;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 3 };
            sc.memberEpoch = 5;
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(new Promise(function () {}));
            sinon.stub(sc.client, 'shareGroupHeartbeatRequest').returns(Promise.resolve({}));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());

            clock = sinon.useFakeTimers();
            ended = sc.end();
            clock.tick(25000); // the acks' share of the deadline
            clock.restore();
            return ended.then(function () {
                sc.client.shareGroupHeartbeatRequest.args[0][2].should.equal(-1); // the leave went out
                sc.client.end.callCount.should.equal(1);
            });
        });

        it('should say when closing a share session failed', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 3 };
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([{ topic: undefined, partition: undefined, error: errors.byName('LeaderNotAvailable') }]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            sinon.spy(sc.client, 'warn');

            return sc.end().then(function () {
                sc.client.warn.args.some(function (a) { return /Closing the share session on broker 0 failed/.test(a[0]); }).should.equal(true);
            });
        });

        it('should not hold up a fetch round for a partition\'s slow recovery', function () {
            var sc = makeShareConsumer(), handled = false;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }, { partition: 1 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([
                { topic: 't', partition: 1, error: errors.byName('UnknownTopicId'), messageSet: [] },
                { topic: 't', topicId: 'id-1', partition: 0, messageSet: [{ offset: 1, message: { value: Buffer.from('x') } }],
                    acquiredRecords: [{ firstOffset: 1, lastOffset: 1, deliveryCount: 1 }] }
            ]));
            sinon.stub(sc.client, 'updateMetadata').returns(new Promise(function () {})); // never finishes
            sc.options.handlerConcurrency = 1;
            sc._handler = function () { handled = true; return Promise.resolve(); };

            sc._closed = true;
            return sc._fetch().then(function () {
                handled.should.equal(true); // partition 0 wasn't kept waiting behind partition 1's refresh
            });
        });

        it('should keep running handlers\' deliveries when another partition\'s recovery fails', function () {
            var sc = makeShareConsumer(), finish, p, acked;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }, { partition: 1 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([
                { topic: 't', topicId: 'id-1', partition: 0, messageSet: [{ offset: 1, message: { value: Buffer.from('x') } }],
                    acquiredRecords: [{ firstOffset: 1, lastOffset: 1, deliveryCount: 1 }] },
                { topic: 't', partition: 1, error: errors.byName('NotLeaderForPartition'), messageSet: [] }
            ]));
            sinon.stub(sc.client, 'updateMetadata').callsFake(function () {
                return Promise.reject(new errors.NoKafkaConnectionError('b:9092', 'down')); // recovery fails
            });
            sc._handler = function (messageSet, topic, partition) {
                return new Promise(function (resolve) { finish = resolve; }).then(function () {
                    return sc.acknowledge({ topic: topic, partition: partition, offset: 1 });
                });
            };

            p = sc._fetch();
            sc._closed = true;
            return promiseUtils.delay(5).then(function () {
                acked = false;
                p.then(function () { acked = true; });
                return promiseUtils.delay(5);
            }).then(function () {
                acked.should.equal(false); // the round waits for the running handler
                finish();
                return p;
            }).then(function () {
                sc._pendingAcks['t:0'].batches.map(function (b) { return b.firstOffset; }).should.eql([1]);
            });
        });

        it('should still close a session the broker says is out of step (InvalidShareSessionEpoch)', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });
            sc._partitionError(errors.byName('InvalidShareSessionEpoch'), 't', 0); // the session is still there

            return sc.end().then(function () {
                sc.client.shareAcknowledgeRequest.args[0][3][0][0].partitions[0].acknowledgementBatches[0].firstOffset.should.equal(1);
            });
        });

        it('should not wait for a metadata refresh in flight before fetching', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sc.client._updateMetadata_pending = true; // someone else's refresh, stuck on a slow broker
            sc.client._updateMetadata_running = new Promise(function () {});
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([]));

            sc._closed = true;
            return sc._fetch().then(function () {
                sc.client.shareFetchRequest.callCount.should.equal(1);
            });
        });

        it('should drain a fetch\'s acks over many partitions quickly', function () {
            var sc = makeShareConsumer(), partitions = [], i, t0;
            sc.client.topicMetadata = Object.create(null);
            sc.client.topicMetadata.t = {};
            for (i = 0; i < 20000; i++) {
                sc.client.topicMetadata.t[i] = { partitionId: i, leader: 0 };
                partitions.push({ partition: i });
                sc._delivered({ topic: 't', partition: i, leader: '0', acquiredRecords: [{ firstOffset: 1, lastOffset: 1 }] });
                sc.acknowledge({ topic: 't', partition: i, offset: 1 });
            }
            sc.shareSessionEpochs = { 0: 2 };
            sc._buildFetchRequests = function () { return { 0: [{ topicName: 't', partitions: partitions }] }; };
            sc.client.shareFetchRequest = function () { return Promise.resolve([]); };
            sc._closed = true;
            t0 = Date.now();
            return sc._fetch().then(function () {
                (Date.now() - t0).should.be.below(100); // a scan per ack was ~145 ms
                Object.keys(sc._pendingAcks).should.eql([]);
            });
        });

        it('should send acks for a revoked partition acknowledge-only on its broker\'s session, forgetting it', function () {
            var sc = makeShareConsumer(), calls = {};
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2, 1: 2 };
            sc._sessionParts = { 1: [{ topicId: 'id1', partitions: [1] }] };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', topicId: 'id1', partitions: [{ partition: 0 }] }] }); // t:1 revoked
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function (g, m, e, only, forgotten, acks) {
                calls[Object.keys(only)[0]] = { topics: only[Object.keys(only)[0]], forgotten: forgotten, acks: acks };
                return Promise.resolve([]);
            });
            sc._closed = true;
            markDelivered(sc, 't', 0, [1]);
            markDelivered(sc, 't', 1, [1]);
            sc._deliveries['t:1'].topicId = 'id1';
            sc.acknowledge([{ topic: 't', partition: 0, offset: 1 }, { topic: 't', partition: 1, offset: 1 }]);

            return sc._fetch().then(function () {
                Object.keys(calls['0'].acks.t).should.eql(['0']);
                Object.keys(calls['1'].acks.t).should.eql(['1']); // not left queued: its records' locks would expire
                calls['1'].topics[0].partitions.should.eql([{ partition: 1, ackOnly: true }]);
                calls['1'].forgotten.should.eql([{ topicId: 'id1', partitions: [1] }]); // not fetched again
                Object.keys(sc._pendingAcks).should.eql([]);
                sc._sessionParts['1'].should.eql([]);
            });
        });

        it('should send a moved partition\'s acks to the broker that delivered it only', function () {
            var sc = makeShareConsumer(), calls = {};
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 }; // broker 1's session opens on this fetch: no acks on it
            sinon.stub(sc, '_buildFetchRequests').returns({ 1: [{ topicName: 't', topicId: 'id1', partitions: [{ partition: 0 }] }] }); // t:0 moved to 1
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function (g, m, e, only, forgotten, acks) {
                calls[Object.keys(only)[0]] = acks;
                return Promise.resolve([]);
            });
            sc._closed = true;
            markDelivered(sc, 't', 0, [1]); // by broker 0
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });

            return sc._fetch().then(function () {
                Object.keys(calls['0'].t).should.eql(['0']); // acknowledge-only, to the broker that delivered it
                should.equal(calls['1'], null); // not on the opening request of its new leader
            });
        });

        it('should not requeue a failed request\'s acks onto a partition\'s new leader', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            // the old leader's request (an ACCEPT) is out when the new leader delivers the record again
            sc._delivered({ topic: 't', topicId: 'id1', partition: 0, leader: '1', acquiredRecords: [{ firstOffset: 1, lastOffset: 1 }] });
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 }, Kafka.ShareConsumer.ACKNOWLEDGE_TYPE.RELEASE);
            sc._requeueAcks({ t: { 0: [{ firstOffset: 1, lastOffset: 1, acknowledgeTypes: [1], topicId: 'id1', _gen: 0 }] } }, '0'); // then fails

            sc._pendingAcks['t:0'].batches.map(function (b) { return b.acknowledgeTypes[0]; }).should.eql([2]); // this delivery's RELEASE only
            String(sc._pendingAcks['t:0'].leader).should.equal('1');
        });

        it('should keep a new delivery\'s acks for its own broker when an old request fails before them', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc._delivered({ topic: 't', topicId: 'id1', partition: 0, leader: '1', acquiredRecords: [{ firstOffset: 1, lastOffset: 1 }] });
            sc._requeueAcks({ t: { 0: [{ firstOffset: 1, lastOffset: 1, acknowledgeTypes: [1], topicId: 'id1', _gen: 0 }] } }, '0'); // nothing queued yet
            should.not.exist(sc._pendingAcks['t:0']); // the new delivery's broker can't apply broker 0's acks

            sc._pendingAcks['t:0'] = { topicName: 't', partition: 0, leader: '0', batches: [{ firstOffset: 1, lastOffset: 1, acknowledgeTypes: [1], _gen: 0 }] };
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 }, Kafka.ShareConsumer.ACKNOWLEDGE_TYPE.RELEASE);
            String(sc._pendingAcks['t:0'].leader).should.equal('1'); // not appended to broker 0's entry
            sc._pendingAcks['t:0'].batches.map(function (b) { return b.acknowledgeTypes[0]; }).should.eql([2]);
        });

        it('should close an idle broker\'s reopening session with its queued acks', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = {};
            sc._reopening = { 0: true }; // its ack-only fetch failed; no partitions left on it
            sc.client.brokerConnections = { 0: {} };
            sinon.stub(sc, '_buildFetchRequests').returns({});
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sc._closed = true;
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });

            return sc._fetch().then(function () {
                var args = sc.client.shareAcknowledgeRequest.args[0];
                args[2].should.equal(-1);
                args[3]['0'][0].partitions[0].acknowledgementBatches[0].firstOffset.should.equal(1);
                Object.keys(sc._pendingAcks).should.eql([]);
                should.not.exist(sc._reopening[0]);
            });
        });

        it('should keep an idle broker\'s acks while there is no connection to close its session on', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc._reopening = { 0: true };
            sc.client.brokerConnections = {}; // not in metadata just now
            sinon.stub(sc, '_buildFetchRequests').returns({});
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sc._closed = true;
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });

            return sc._fetch().then(function () {
                sc.client.shareAcknowledgeRequest.callCount.should.equal(0);
                Object.keys(sc._pendingAcks).should.eql(['t:0']);
                sc._reopening[0].should.equal(true); // tried again once the broker is back
            });
        });

        it('should handle a lost session on a fetch carrying only acknowledgements', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({}); // t:0 revoked
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function (g, m, e, only) {
                return Promise.resolve(only['0'][0].partitions.map(function (q) {
                    return { topic: 't', partition: q.partition, leader: '0', notFetched: true, error: errors.byName('ShareSessionNotFound') };
                }));
            });
            sc._closed = true;
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });

            return sc._fetch().then(function () {
                should.not.exist(sc.shareSessionEpochs[0]); // forgotten, not retried every poll
            });
        });

        it('should drop acks no share session can carry, and keep those a fetch will', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = {}; // broker 0's opens on this fetch; broker 1 has none, and no fetch
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', topicId: 'id1', partitions: [{ partition: 2 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([]));
            sinon.spy(sc.client, 'warn');
            sc._closed = true;
            markDelivered(sc, 't', 0, [1]);
            markDelivered(sc, 't', 1, [1]);
            sc.acknowledge([{ topic: 't', partition: 0, offset: 1 }, { topic: 't', partition: 1, offset: 1 }]);

            return sc._fetch().then(function () {
                Object.keys(sc._pendingAcks).should.eql(['t:0']); // broker 0's session is open next round
                sc.client.warn.args.some(function (a) { return /t:1/.test(a.join(' ')); }).should.equal(true);
            });
        });

        it('should keep the share sessions across an assignment change', function () {
            var sc = makeShareConsumer();
            sc.shareSessionEpochs = { 0: 4 };
            sc._applyAssignment({ _present: 1, topicPartitions: [{ topicId: 'id1', partitions: [0] }] });
            sc.shareSessionEpochs.should.eql({ 0: 4 }); // revoked partitions are forgotten by the next fetch instead
        });

        it('should settle a fetch in flight before draining the final acks', function () {
            var sc = makeShareConsumer(), fail, fetching, ended;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(new Promise(function (resolve, reject) { fail = reject; }));
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            sinon.spy(sc.client, 'warn');
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });

            fetching = sc._fetch(); // drains the ack into the request
            return promiseUtils.delay(0).then(function () {
                ended = sc.end();
                fail(new errors.NoKafkaConnectionError('b:9092', 'down')); // the fetch requeues it
                return Promise.all([fetching, ended]);
            }).then(function () {
                // a failed request doesn't close the session on the broker: the requeued ack goes out with its close
                sc.client.shareAcknowledgeRequest.args[0][3][0][0].partitions[0].acknowledgementBatches[0].firstOffset.should.equal(1);
            });
        });

        it('should not wait for the handler that calls end()', function () {
            var sc = makeShareConsumer(), ended;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([{
                topic: 't', topicId: 'id-1', partition: 0, messageSet: [{ offset: 1, message: { value: Buffer.from('x') } }],
                acquiredRecords: [{ firstOffset: 1, lastOffset: 1, deliveryCount: 1 }]
            }]));
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            sc._handler = function (messageSet, topic, partition) {
                sc.acknowledge({ topic: topic, partition: partition, offset: 1 });
                ended = sc.end(); // the fetch can't finish before this handler does
                return ended;
            };

            sc._fetchPromise = sc._fetch();
            return sc._fetchPromise.then(function () {
                return ended;
            }).then(function () {
                sc.client.shareAcknowledgeRequest.callCount.should.equal(1); // its ack was sent, not cut off by the deadline
                sc.client.end.callCount.should.equal(1);
            });
        });

        it('should take acks while the delivery\'s handler runs, and refuse them after it settles', function () {
            var sc = makeShareConsumer(), late, p;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([{
                topic: 't', topicId: 'id-1', partition: 0,
                messageSet: [{ offset: 1, message: { value: Buffer.from('x') } }, { offset: 2, message: { value: Buffer.from('y') } }],
                acquiredRecords: [{ firstOffset: 1, lastOffset: 2, deliveryCount: 1 }]
            }]));
            sinon.spy(sc.client, 'warn');
            sc._handler = function (messageSet, topic, partition) {
                return promiseUtils.delay(5).then(function () { // async work, then an ack before it resolves
                    return sc.acknowledge({ topic: topic, partition: partition, offset: 1 });
                }).then(function () {
                    late = function () { return sc.acknowledge({ topic: topic, partition: partition, offset: 2 }); }; // kept for later
                });
            };

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                return late(); // after the handler settled: the delivery is over, as Java's next poll()
            }).then(function () {
                sc._pendingAcks['t:0'].batches.map(function (b) { return b.firstOffset; }).should.eql([1]);
                sc.client.warn.args.some(function (a) { return /handler/.test(a[0]) && a[1] === 't:0@2'; }).should.equal(true);
            });
        });

        it('should refuse an ack for records outside the partition\'s current delivery', function () {
            var sc = makeShareConsumer();
            sinon.spy(sc.client, 'warn');
            markDelivered(sc, 't', 0, [7]); // offset 3 came in an earlier delivery (or from a since-recreated topic)

            return sc.acknowledge([{ topic: 't', partition: 0, offset: 3 }, { topic: 't', partition: 0, offset: 7 }, { topic: 't', partition: 1, offset: 7 }]).then(function () {
                sc._pendingAcks['t:0'].batches.map(function (b) { return b.firstOffset; }).should.eql([7]);
                should.not.exist(sc._pendingAcks['t:1']);
                sc.client.warn.args.some(function (a) { return /handler/.test(a[0]) && a[1] === 't:0@3, t:1@7'; }).should.equal(true);
            });
        });

        it('should accept a range ack covered by the delivery\'s acquired ranges, gaps included', function () {
            var sc = makeShareConsumer();
            sc._delivered({ topic: 't', partition: 0, acquiredRecords: [{ firstOffset: 14, lastOffset: 20 }, { firstOffset: 10, lastOffset: 13 }] });

            return sc.acknowledge([{ topic: 't', partition: 0, firstOffset: 10, lastOffset: 20 }, { topic: 't', partition: 0, firstOffset: 9, lastOffset: 12 }]).then(function () {
                sc._pendingAcks['t:0'].batches.map(function (b) { return [b.firstOffset, b.lastOffset]; }).should.eql([[10, 20]]);
            });
        });

        it('should drop queued acks for offsets delivered again, keeping the rest', function () {
            var sc = makeShareConsumer(), p;
            twoLeaders(sc);
            sc.shareSessionEpochs = {}; // the leader's session is reopening: the next fetch can't carry acks
            markDelivered(sc, 't', 0, [1, 2]);
            sc.acknowledge([{ topic: 't', partition: 0, offset: 1 }, { topic: 't', partition: 0, offset: 2 }]);
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([{
                // offset 2's acquisition ended (e.g. revoked and reacquired): delivered again
                topic: 't', topicId: 'id-1', partition: 0, messageSet: [{ offset: 2, message: { value: Buffer.from('x') } }],
                acquiredRecords: [{ firstOffset: 2, lastOffset: 2, deliveryCount: 2 }]
            }]));
            sc._handler = noop;

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                should.not.exist(sc.client.shareFetchRequest.args[0][5]); // held, not carried
                // sending the old ack for 2 next to a fresh one would overlap; 1 is still acquired
                sc._pendingAcks['t:0'].batches.map(function (b) { return b.firstOffset; }).should.eql([1]);
            });
        });

        it('should send a partition\'s acks from different deliveries in separate fetches', function () {
            var sc = makeShareConsumer(), sentAcks = [];
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 }); // kept from before a revoke and reacquire
            markDelivered(sc, 't', 0, [7]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 7 });
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function (g, m, e, r, f, acks) {
                sentAcks.push(acks && acks.t[0].map(function (b) { return b.firstOffset; }));
                return Promise.resolve([]);
            });

            sc._closed = true;
            return sc._fetch().then(function () { return sc._fetch(); }).then(function () {
                // a stale ack makes the broker reject the partition's acks as a whole: not the fresh one too
                sentAcks.should.eql([[1], [7]]);
            });
        });

        it('should keep the parts of a queued range ack that were not delivered again', function () {
            var sc = makeShareConsumer();
            markDelivered(sc, 't', 0, [10, 11, 12]);
            sc.acknowledge({ topic: 't', partition: 0, firstOffset: 10, lastOffset: 12 });

            markDelivered(sc, 't', 0, [11]); // only 11 was reacquired
            sc._pendingAcks['t:0'].batches.map(function (b) { return [b.firstOffset, b.lastOffset]; }).should.eql([[10, 10], [12, 12]]);
        });

        it('should keep its own copy of the acquired ranges a handler is given', function () {
            var sc = makeShareConsumer(), ranges = [{ firstOffset: 1, lastOffset: 1 }, { firstOffset: 2, lastOffset: 2 }];
            sc._delivered({ topic: 't', partition: 0, acquiredRecords: ranges });
            ranges.shift(); // the handler takes ranges off its array as it goes

            return sc.acknowledge({ topic: 't', partition: 0, offset: 1 }).then(function () {
                sc._pendingAcks['t:0'].batches.length.should.equal(1);
            });
        });

        it('should check an ack against thousands of acquired ranges quickly', function () {
            var sc = makeShareConsumer(), ranges = [], acks = [], i, t0;
            for (i = 0; i < 5000; i++) {
                ranges.push({ firstOffset: i * 2, lastOffset: i * 2 });
                acks.push({ topic: 't', partition: 0, offset: i * 2 });
            }
            sc._delivered({ topic: 't', partition: 0, acquiredRecords: ranges.reverse() });
            t0 = Date.now();
            return sc.acknowledge(acks).then(function () {
                (Date.now() - t0).should.be.below(100); // a sort and scan per ack took ~280 ms
                sc._pendingAcks['t:0'].batches.length.should.equal(5000);
            });
        });

        it('should start no fetch, and deliver nothing, once end() has run', function () {
            var sc = makeShareConsumer(), refreshed, p;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sinon.stub(sc, '_refreshMissingMetadata').returns(new Promise(function (resolve) { refreshed = resolve; }));
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());

            p = sc._fetch(); // waiting on the metadata refresh
            return sc.end().then(function () {
                refreshed();
                return p;
            }).then(function () {
                sc.client.shareFetchRequest.callCount.should.equal(0);
            });
        });

        it('should not hand out records that arrive after end()', function () {
            var sc = makeShareConsumer(), handled = 0, finish, p;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(new Promise(function (resolve) { finish = resolve; }));
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            sc._handler = function () { handled++; return Promise.resolve(); };

            p = sc._fetch();
            return promiseUtils.delay(0).then(function () {
                var ended = sc.end();
                finish([{ topic: 't', topicId: 'id-1', partition: 0, messageSet: [{ offset: 1, message: { value: Buffer.from('x') } }],
                    acquiredRecords: [{ firstOffset: 1, lastOffset: 1, deliveryCount: 1 }] }]);
                return Promise.all([p, ended]);
            }).then(function () {
                handled.should.equal(0);
            });
        });

        it('should share one shutdown between concurrent end() calls', function () {
            var sc = makeShareConsumer(), first, second;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });

            first = sc.end();
            second = sc.end(); // e.g. a handler and a shutdown hook
            second.should.equal(first);
            return second.then(function () {
                sc.client.shareAcknowledgeRequest.callCount.should.equal(1);
                sc.client.end.callCount.should.equal(1);
            });
        });

        it('should close every open share session on end(), acks or not', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4, 1: 2 }; // a handler is still working on broker 1's records
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });

            return sc.end().then(function () {
                var requests = {};
                sc.client.shareAcknowledgeRequest.args.forEach(function (a) { Object.assign(requests, a[3]); }); // one per broker
                Object.keys(requests).sort().should.eql(['0', '1']);
                requests[1].should.eql([]);
            });
        });

        it('should tag an ack with the ID of the topic its records came from', function () {
            var sc = makeShareConsumer(), p;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sc.client.topicIds.t = 'id-new'; // metadata already knows the topic was recreated
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([{
                topic: 't', topicId: 'id-old', partition: 0, messageSet: [{ offset: 1, message: { value: Buffer.from('x') } }],
                acquiredRecords: [{ firstOffset: 1, lastOffset: 1, deliveryCount: 1 }]
            }]));
            sc._handler = function (messageSet, topic, partition) {
                return sc.acknowledge({ topic: topic, partition: partition, offset: 1 });
            };

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                sc._pendingAcks['t:0'].batches[0].topicId.should.equal('id-old');
            });
        });

        it('should close within the deadline when the final acks never get an answer', function () {
            var sc = makeShareConsumer(), clock = sinon.useFakeTimers(), ended;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(new Promise(function () {})); // waits on a stuck refresh
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });

            ended = sc.end();
            return Promise.resolve().then(function () {
                clock.tick(30000);
                clock.restore();
                return ended;
            }).then(function () {
                sc.client.shareAcknowledgeRequest.callCount.should.equal(1);
                sc.client.end.callCount.should.equal(1);
            });
        });

        it('should not send, and should say so, an ack made after end()', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            sinon.spy(sc.client, 'warn');

            return sc.end().then(function () {
                return sc.acknowledge({ topic: 't', partition: 0, offset: 7 }); // a handler still running when end() was called
            }).then(function () {
                sc.client.shareAcknowledgeRequest.callCount.should.equal(1); // the session close, before the ack
                sc.client.shareAcknowledgeRequest.args[0][3].should.eql({ 0: [] });
                Object.keys(sc._pendingAcks).should.eql([]);
                sc.client.warn.args.some(function (a) { return /after end\(\)/.test(a[0]); }).should.equal(true);
            });
        });

        it('should drain and fetch with more queued acks than fit in a call stack', function () {
            var sc = makeShareConsumer(), acks = [], i, sent;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            for (i = 0; i < 150000; i++) { acks.push({ topic: 't', partition: 0, offset: i }); }
            sc._delivered({ topic: 't', partition: 0, acquiredRecords: [{ firstOffset: 0, lastOffset: 149999 }] });
            sc.acknowledge(acks);
            sc._delivered({ topic: 't', partition: 0, acquiredRecords: [{ firstOffset: 200000, lastOffset: 200000 }] }); // prunes the queue
            sc._buildFetchRequests = function () { return { 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] }; };
            sc.client.shareFetchRequest = function (g, m, e, r, f, a) { sent = a.t[0].length; return Promise.resolve([]); };

            sc._closed = true;
            return sc._fetch().then(function () {
                sent.should.equal(150000);
            });
        });

        it('should not advance the session epoch for a fetch the broker never took', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([
                { topic: 't', partition: 0, leader: '0', notFetched: true, error: errors.byName('LeaderNotAvailable'), messageSet: [] }
            ]));
            sinon.stub(sc.client, 'updateMetadata').returns(Promise.resolve());

            sc._closed = true;
            return sc._fetch().then(function () {
                sc.shareSessionEpochs.should.eql({ 0: 4 }); // not 5: the broker is still at 4
            });
        });

        it('should send final acks under the topic ID of the delivery, not a lagging cache\'s', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            sc._delivered({ topic: 't', topicId: 'id-new', partition: 0, leader: '0', acquiredRecords: [{ firstOffset: 1, lastOffset: 1 }] });
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });
            sc.client.topicIds.t = 'id-old'; // a lagging metadata broker

            return sc.end().then(function () {
                sc.client.shareAcknowledgeRequest.args[0][3][0][0].topicId.should.equal('id-new');
            });
        });

        it('should send no more final ack rounds once the leave has started', function () {
            var sc = makeShareConsumer(), clock, ended, first;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sc.memberEpoch = 5;
            sinon.stub(sc.client, 'shareAcknowledgeRequest').onFirstCall().returns(new Promise(function (resolve) { first = resolve; }))
                .returns(Promise.resolve([]));
            sinon.stub(sc.client, 'shareGroupHeartbeatRequest').returns(Promise.resolve({}));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });
            markDelivered(sc, 't', 0, [7]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 7 });

            clock = sinon.useFakeTimers();
            ended = sc.end();
            return Array.apply(null, new Array(10)).reduce(function (p) { return p.then(function () { return null; }); }, Promise.resolve()).then(function () {
                sc.client.shareAcknowledgeRequest.callCount.should.equal(1); // the first (earlier delivery's) round is out
                clock.tick(25000); // and outlasts the acks' deadline
                clock.restore();
                return ended;
            }).then(function () {
                first([]);
                return promiseUtils.delay(5);
            }).then(function () {
                sc.client.shareAcknowledgeRequest.callCount.should.equal(1); // no close racing the leave
            });
        });

        it('should build final acks over many topics quickly', function () {
            var sc = makeShareConsumer(), i, t0, sent;
            sc.shareSessionEpochs = { 0: 2 };
            for (i = 0; i < 30000; i++) {
                sc._delivered({ topic: 't' + i, partition: 0, leader: '0', acquiredRecords: [{ firstOffset: 1, lastOffset: 1 }] });
                sc.acknowledge({ topic: 't' + i, partition: 0, offset: 1 });
            }
            sinon.stub(sc.client, 'shareAcknowledgeRequest').callsFake(function (g, m, epoch, requests) {
                sent = requests['0'];
                return Promise.resolve([]);
            });
            t0 = Date.now();
            return sc._sendFinalAcks('0').then(function () {
                (Date.now() - t0).should.be.below(300); // a scan per partition was ~1.5 s
                sent.length.should.equal(30000);
            });
        });

        it('should stop the earlier final ack rounds after one the broker didn\'t take, still closing', function () {
            var sc = makeShareConsumer(), sent = [];
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sinon.stub(sc.client, 'shareAcknowledgeRequest').callsFake(function (g, m, epoch) {
                sent.push(epoch);
                return Promise.resolve(sent.length === 1 ? [{ topic: 't', partition: 0, error: new errors.NoKafkaConnectionError('b', 'down'), notAcked: true }] : []);
            });
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            [1, 5, 9].forEach(function (o) { // three deliveries' acks for one partition
                markDelivered(sc, 't', 0, [o]);
                sc.acknowledge({ topic: 't', partition: 0, offset: o });
            });

            return sc.end().then(function () {
                sent.should.eql([4, -1]); // not 5: the epoch is unknown after the failed round
            });
        });

        it('should close with acks from different deliveries in separate requests, the latest with the close', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 }); // kept from an earlier delivery
            markDelivered(sc, 't', 0, [7]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 7 });

            return sc.end().then(function () {
                sc.client.shareAcknowledgeRequest.args.map(function (a) {
                    return [a[2], a[3][0][0].partitions[0].acknowledgementBatches.map(function (b) { return b.firstOffset; })];
                }).should.eql([[4, [1]], [-1, [7]]]);
            });
        });

        it('should send queued acks when it ends, closing each open share session', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 }; // broker 1 has no open session: it can't take acks
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            markDelivered(sc, 't', 0, [1]);
            markDelivered(sc, 't', 1, [1]);
            sc.acknowledge([{ topic: 't', partition: 0, offset: 1 }, { topic: 't', partition: 1, offset: 1 }]);

            return sc.end().then(function () {
                var args = sc.client.shareAcknowledgeRequest.args[0], partition = args[3][0][0].partitions[0];
                args[2].should.equal(-1);
                Object.keys(args[3]).should.eql(['0']);
                partition.partitionIndex.should.equal(0);
                partition.acknowledgementBatches.map(function (b) { return [b.firstOffset, b.lastOffset, b.acknowledgeTypes]; })
                    .should.eql([[1, 1, [1]]]);
                sc.client.end.callCount.should.equal(1);
            });
        });

        it('should still close the client when the final acks fail', function () {
            var sc = makeShareConsumer();
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 4 };
            sinon.stub(sc.client, 'shareAcknowledgeRequest').callsFake(function () {
                return Promise.reject(new errors.NoKafkaConnectionError('b:9092', 'down'));
            });
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });

            return sc.end().then(function () {
                sc.client.end.callCount.should.equal(1);
            });
        });

        it('should not fetch the old member\'s assignment as the new one', function () {
            var sc = makeShareConsumer();
            sc.assignedPartitions = { 'id-1': [0] };
            sc._newMember();
            sc.assignedPartitions.should.eql({}); // until the coordinator gives the new member its own
        });

        it('should leave the old member\'s sessions, acks and fetch behind when it rejoins as a new one', function () {
            var sc = makeShareConsumer(), oldMember = sc.memberId, finish, p;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 3 };
            markDelivered(sc, 't', 0, [1]);
            sc.acknowledge({ topic: 't', partition: 0, offset: 1 });
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(new Promise(function (resolve) { finish = resolve; }));
            sc._handler = function () { throw new Error('no records of the old member are delivered'); };

            p = sc._fetch(); // in flight as the old member
            return promiseUtils.delay(0).then(function () {
                sc._newMember(); // the heartbeat said UnknownMemberId
                finish([{ topic: 't', topicId: 'id-1', partition: 0, leader: '0', messageSet: [{ offset: 2, message: { value: Buffer.from('x') } }],
                    acquiredRecords: [{ firstOffset: 2, lastOffset: 2, deliveryCount: 1 }] }]);
                sc._closed = true;
                return p;
            }).then(function () {
                sc.memberId.should.not.equal(oldMember);
                Object.keys(sc._pendingAcks).should.eql([]);
                sc.shareSessionEpochs.should.eql({});
                sc._reopening.should.eql({}); // nothing for end() to close under the new member
            });
        });

        it('should deliver nothing still waiting for a handler slot once the member is replaced', function () {
            var sc = makeShareConsumer(), release, p, handled = [];
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sc.options.handlerConcurrency = 1;
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }, { partition: 1 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([0, 1].map(function (partition) {
                return { topic: 't', topicId: 'id-1', partition: partition, leader: '0', messageSet: [{ offset: 1, message: { value: Buffer.from('x') } }],
                    acquiredRecords: [{ firstOffset: 0, lastOffset: 1 }] };
            })));
            sc._handler = function (messageSet, topic, partition) {
                handled.push(partition);
                return new Promise(function (resolve) { release = resolve; });
            };

            p = sc._fetch();
            return promiseUtils.delay(5).then(function () {
                handled.should.eql([0]); // partition 1 waits for the only slot
                sc._newMember();
                release();
                sc._closed = true;
                return p;
            }).then(function () {
                handled.should.eql([0]); // partition 1 is the old member's: not delivered
                Object.keys(sc._pendingAcks).should.eql([]); // nor GAP-acked under the new member
            });
        });

        it('should hand out one broker\'s records without waiting for a slower broker', function () {
            var sc = makeShareConsumer(), handled = [];
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2, 1: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({
                0: [{ topicName: 't', partitions: [{ partition: 0 }] }],
                1: [{ topicName: 't', partitions: [{ partition: 1 }] }]
            });
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function (g, m, e, requests) {
                if (requests[1]) { return new Promise(function () {}); } // broker 1 is slow
                return Promise.resolve([{ topic: 't', topicId: 'id-1', partition: 0, leader: '0',
                    messageSet: [{ offset: 1, message: { value: Buffer.from('x') } }], acquiredRecords: [{ firstOffset: 1, lastOffset: 1 }] }]);
            });
            sc._handler = function (messageSet, topic, partition) { handled.push(partition); return Promise.resolve(); };

            sc._fetch();
            return promiseUtils.delay(5).then(function () {
                handled.should.eql([0]); // its decoded records aren't held until broker 1 answers
                sc._closed = true;
            });
        });

        it('should not send a broker\'s final acks once the leave has begun', function () {
            var sc = makeShareConsumer(), clock, ended, answer;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 1: 2 };
            sc.memberEpoch = 5;
            sc._fetchRequests = { 1: new Promise(function (resolve) { answer = resolve; }) }; // broker 1's fetch in flight
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'shareGroupHeartbeatRequest').returns(Promise.resolve({}));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            sinon.spy(sc.client, 'warn');

            clock = sinon.useFakeTimers();
            ended = sc.end();
            clock.tick(25000);
            clock.restore();
            return ended.then(function () {
                answer(); // answers after the deadline, the leave already sent
                return promiseUtils.delay(5);
            }).then(function () {
                sc.client.shareAcknowledgeRequest.callCount.should.equal(0);
                sc.client.warn.args.some(function (a) { return /after the acknowledgement deadline/.test(a.join(' ')); }).should.equal(true);
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
        it('GroupConsumer: end() during a join without a member ID should leave with the ID it gets', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-race' });
            var order = [], answerJoin, rejoinP, endP;

            sinon.stub(gc.client, 'updateGroupCoordinator').returns(Promise.resolve());
            sinon.stub(gc.client, '_findGroupCoordinator').returns(Promise.resolve({}));
            sinon.stub(gc.client, 'joinConsumerGroupRequest').callsFake(function () {
                order.push('join');
                return new Promise(function (resolve, reject) {
                    answerJoin = function () { // KIP-394: the broker now holds 'me' as a member
                        var err = errors.byCode(79);
                        err.memberId = 'me';
                        reject(err);
                    };
                });
            });
            sinon.stub(gc.client, 'leaveGroupRequest').callsFake(function () { order.push('leave'); return Promise.resolve(); });
            sinon.stub(gc.client, 'end').callsFake(function () { order.push('end'); return Promise.resolve(); });
            sinon.stub(gc, '_runHeartbeat').returns(Promise.resolve());

            rejoinP = gc._fullRejoin();
            return promiseUtils.delay(10).then(function () {
                endP = gc.end(); // memberId is still null here
                return promiseUtils.delay(10);
            }).then(function () {
                order.should.eql(['join']); // waiting for the join, the client still open
                answerJoin();
                return Promise.all([endP, rejoinP]);
            }).then(function () {
                order.should.eql(['join', 'leave', 'end']); // no second join with the new ID
                gc.client.leaveGroupRequest.lastCall.args.should.eql(['gc-unit-race', 'me']);
                gc._runHeartbeat.callCount.should.equal(0);
            });
        });

        it('ShareConsumer: end() during the coordinator lookup should not join', function () {
            var sc = new Kafka.ShareConsumer({ clientId: 'unit-sc', groupId: 'sc-unit-race' });
            var lookedUp, joined;
            sinon.stub(sc.client, 'updateGroupCoordinator').returns(new Promise(function (resolve) { lookedUp = resolve; }));
            sinon.stub(sc.client, 'shareGroupHeartbeatRequest').returns(Promise.resolve({ memberId: sc.memberId, memberEpoch: 1 }));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());

            joined = sc._join();
            return sc.end().then(function () {
                lookedUp();
                return joined;
            }).then(function () {
                sc.client.shareGroupHeartbeatRequest.args.filter(function (a) { return a[2] === 0; }).length.should.equal(0);
            });
        });

        it('GroupConsumer: end() during the join\'s coordinator lookup should not send it, nor wait', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-race' });
            var found, t0;
            sinon.stub(gc.client, '_findGroupCoordinator').returns(new Promise(function (resolve) { found = resolve; }));
            sinon.stub(gc.client, 'joinConsumerGroupRequest').returns(Promise.resolve({}));
            sinon.stub(gc.client, 'end').returns(Promise.resolve());

            gc._joinGroup().catch(noop);
            t0 = Date.now();
            return gc.end().then(function () {
                (Date.now() - t0).should.be.below(500); // no join sent: none to wait for
                found({});
                return promiseUtils.delay(10);
            }).then(function () {
                gc.client.joinConsumerGroupRequest.callCount.should.equal(0);
            });
        });

        it('GroupConsumer: end() should stop a join\'s retry instead of waiting for it', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-race' });
            var fail, endP, t0;

            sinon.stub(gc.client, 'updateGroupCoordinator').returns(Promise.resolve());
            sinon.stub(gc.client, '_findGroupCoordinator').returns(Promise.resolve({}));
            sinon.stub(gc.client, 'joinConsumerGroupRequest').callsFake(function () {
                if (gc.client.joinConsumerGroupRequest.callCount > 1) { // a retry would get an ID
                    return Promise.resolve({ memberId: 'late', leaderId: 'x', generationId: 1, members: [] });
                }
                return new Promise(function (resolve, reject) { fail = reject; });
            });
            sinon.stub(gc.client, 'leaveGroupRequest').returns(Promise.resolve());
            sinon.stub(gc.client, 'end').returns(Promise.resolve());
            sinon.stub(gc, '_syncGroup').returns(new Promise(function () {}));

            gc._fullRejoin();
            return promiseUtils.delay(5).then(function () {
                t0 = Date.now();
                endP = gc.end();
                fail(errors.byName('GroupCoordinatorNotAvailable'));
                return endP;
            }).then(function () {
                (Date.now() - t0).should.be.below(500); // not after the 1 s backoff
                gc.client.joinConsumerGroupRequest.callCount.should.equal(1);
                gc.client.leaveGroupRequest.callCount.should.equal(0);
            });
        });

        it('GroupConsumer: end() should wait for an unanswered join only up to the session timeout', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-race', sessionTimeout: 30 });

            sinon.stub(gc.client, 'updateGroupCoordinator').returns(Promise.resolve());
            sinon.stub(gc.client, '_findGroupCoordinator').returns(Promise.resolve({}));
            sinon.stub(gc.client, 'joinConsumerGroupRequest').returns(new Promise(function () {}));
            sinon.stub(gc.client, 'leaveGroupRequest').returns(Promise.resolve());
            sinon.stub(gc.client, 'end').returns(Promise.resolve());

            gc._fullRejoin();
            return promiseUtils.delay(5).then(function () {
                return gc.end();
            }).then(function () {
                gc.client.leaveGroupRequest.callCount.should.equal(0); // never got an ID
                gc.client.end.callCount.should.equal(1);
            });
        });

        it('GroupConsumer: no join should start after end()', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-race' });
            var lookedUp;

            sinon.stub(gc.client, 'updateGroupCoordinator').returns(new Promise(function (resolve) { lookedUp = resolve; }));
            sinon.stub(gc.client, '_findGroupCoordinator').returns(Promise.resolve({}));
            sinon.stub(gc.client, 'joinConsumerGroupRequest').returns(Promise.resolve({}));
            sinon.stub(gc.client, 'end').returns(Promise.resolve());

            gc._fullRejoin();
            return gc.end().then(function () {
                lookedUp();
                return promiseUtils.delay(10);
            }).then(function () {
                gc.client.joinConsumerGroupRequest.callCount.should.equal(0);
            });
        });

        it('ShareConsumer: a join landing while end() sends its final acks should leave after them', function () {
            var sc = new Kafka.ShareConsumer({ clientId: 'unit-sc', groupId: 'sc-unit-race' });
            var order = [], resolveJoin, ackDone, joinP, endP;

            sc._topics = ['t'];
            sc._handler = noop;
            sc.client.topicMetadata = Object.create(null);
            sc.client.topicMetadata.t = { 0: { partitionId: 0, leader: 0 } };
            sc.shareSessionEpochs = { 0: 3 };
            sinon.stub(sc.client, 'updateGroupCoordinator').returns(Promise.resolve());
            sinon.stub(sc.client, 'end').returns(Promise.resolve());
            sinon.stub(sc.client, 'shareAcknowledgeRequest').callsFake(function () {
                order.push('acks');
                return new Promise(function (resolve) { ackDone = resolve; });
            });
            sinon.stub(sc.client, 'shareGroupHeartbeatRequest').callsFake(function (gid, mid, epoch) {
                if (epoch === 0) { return new Promise(function (resolve) { resolveJoin = resolve; }); }
                order.push('leave');
                return Promise.resolve({});
            });

            joinP = sc._join(); // a rejoin in flight
            return promiseUtils.delay(5).then(function () {
                endP = sc.end();
                return promiseUtils.delay(5);
            }).then(function () {
                resolveJoin({ memberId: sc.memberId, memberEpoch: 7, heartbeatIntervalMs: 100000, assignment: null });
                return promiseUtils.delay(5);
            }).then(function () {
                order.should.eql(['acks']); // the leave waits for the acks
                ackDone([]);
                return Promise.all([endP, joinP]);
            }).then(function () {
                clearTimeout(sc._heartbeatTimeout);
                order.should.eql(['acks', 'leave']);
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

    // '__proto__', 'constructor' and 'toString' are legal topic names
    describe('topics named like Object.prototype members', function () {
        function metadata(topic) {
            var m = Object.create(null);
            m[topic] = { 0: { partitionId: 0, leader: 0 } };
            return m;
        }

        it('BaseConsumer: should fetch a topic named constructor', function () {
            var c = new Kafka.SimpleConsumer({ clientId: 'unit-consumer' });
            var p;
            sinon.stub(c.client, 'fetchRequest').returns(Promise.resolve([]));
            c.subscriptions['constructor:0'] = { topic: 'constructor', partition: 0, offset: 5, leader: 0, maxBytes: 1024, handler: noop };

            p = c._fetch();
            c._closed = true;
            return p.then(function () {
                c.client.fetchRequest.callCount.should.equal(1);
                c.client.fetchRequest.args[0][0][0][0].topicName.should.equal('constructor');
            });
        });

        it('GroupConsumer: the leader should assign a topic named __proto__', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-proto' });

            gc.memberId = 'me';
            gc.leaderId = 'me';
            gc.generationId = 1;
            gc.strategyName = 'S';
            gc.strategies = { S: { strategy: new Kafka.DefaultAssignmentStrategy(), handler: noop } };
            gc.members = [{ id: 'me', subscriptions: ['__proto__'], metadata: null }];
            gc.topics = ['__proto__'];
            gc.client.topicMetadata = metadata('__proto__');
            sinon.stub(gc.client, 'updateMetadata').returns(Promise.resolve());
            sinon.stub(gc.client, 'syncConsumerGroupRequest').returns(Promise.resolve({ memberAssignment: { partitionAssignment: [] } }));

            return gc._syncGroup().then(function () {
                gc.client.updateMetadata.args[0][0].should.eql(['__proto__']);
                gc.client.syncConsumerGroupRequest.args[0][3].should.have.length(1); // the leader's assignment
            });
        });

        it('ShareConsumer: should build a fetch request for a topic named constructor', function () {
            var sc = new Kafka.ShareConsumer({ clientId: 'unit-sc', groupId: 'sc-unit' });
            sc.assignedPartitions = { id1: [0] };
            sc.client.topicNames = { id1: 'constructor' };
            sc.client.topicIds.constructor = 'id1';
            sc.client.topicMetadata = metadata('constructor');

            sc._buildFetchRequests().should.eql({ 0: [{ topicName: 'constructor', topicId: 'id1', partitions: [{ partition: 0 }] }] });
        });

        it('ShareConsumer: should drain and requeue acks for a topic named __proto__', function () {
            var sc = new Kafka.ShareConsumer({ clientId: 'unit-sc', groupId: 'sc-unit' });
            var p;
            sc._handler = noop;
            sc.shareSessionEpochs = { 0: 2 };
            sc.client.topicMetadata = metadata('__proto__');
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: '__proto__', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'updateMetadata').returns(Promise.resolve());
            sinon.stub(sc.client, 'shareFetchRequest').callsFake(function (g, m, e, r, f, acks) {
                Object.keys(acks).should.eql(['__proto__']);
                return Promise.resolve([{ topic: '__proto__', partition: 0, error: new errors.NoKafkaConnectionError('b:9092', 'down'), messageSet: [] }]);
            });
            markDelivered(sc, '__proto__', 0, [1]);
            sc.acknowledge({ topic: '__proto__', partition: 0, offset: 1 });

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                should.not.exist(Object.prototype[0]);
                sc.client.shareFetchRequest.callCount.should.equal(1);
                Object.keys(sc._pendingAcks).should.eql(['__proto__:0']); // requeued for the next fetch
            }).finally(function () {
                delete Object.prototype[0];
            });
        });
    });

    describe('GroupConsumer end() and rebalance callbacks', function () {
        it('should close the client even when LeaveGroup fails', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-end' });
            gc.memberId = 'me';
            sinon.stub(gc.client, 'leaveGroupRequest').callsFake(function () {
                return Promise.reject(new errors.NoKafkaConnectionError('coord:1', 'down'));
            });
            sinon.stub(gc.client, 'end').returns(Promise.resolve());

            return gc.end().then(function () {
                gc.client.end.callCount.should.equal(1);
            });
        });

        it('should not send LeaveGroup before it has joined', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-end' });
            sinon.stub(gc.client, 'leaveGroupRequest').returns(Promise.resolve());
            sinon.stub(gc.client, 'end').returns(Promise.resolve());

            return gc.end().then(function () {
                gc.client.leaveGroupRequest.callCount.should.equal(0);
                gc.client.end.callCount.should.equal(1);
            });
        });

        [
            ['throws', function () { throw new Error('callback bug'); }],
            ['rejects', function () { return Promise.reject(new Error('callback bug')); }]
        ].forEach(function (c) {
            it('should finish the rebalance when a callback ' + c[0], function () {
                var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-cb' });
                var unhandled = [];
                function onUnhandled(reason) { unhandled.push(reason); }

                process.on('unhandledRejection', onUnhandled);
                gc.strategyName = 'S';
                gc.strategies = { S: { handler: noop } };
                gc.topics = ['t'];
                gc._onPartitionsRevoked = c[1];
                gc._onPartitionsAssigned = c[1];
                gc.subscriptions['t:0'] = { topic: 't', partition: 0 };
                sinon.stub(gc.client, 'updateMetadata').returns(Promise.resolve());
                sinon.stub(gc, 'fetchOffset').returns(Promise.resolve([{ topic: 't', partition: 1, offset: 5 }]));
                sinon.stub(gc, 'subscribe').returns(Promise.resolve());

                return gc._updateSubscriptions([{ topic: 't', partitions: [1] }]).then(function () {
                    return promiseUtils.delay(10);
                }).then(function () {
                    process.removeListener('unhandledRejection', onUnhandled);
                    gc.subscribe.callCount.should.equal(1);
                    unhandled.should.have.length(0);
                });
            });
        });

        it('should call rebalance callbacks with the consumer as this', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-cb' });
            var seen = [];
            gc._cooperative = true;
            gc.strategyName = 'S';
            gc.strategies = { S: { handler: noop } };
            gc.ownedPartitions = [{ topic: 't', partitions: [0, 1] }];
            gc._onPartitionsRevoked = function () { seen.push(this); };
            gc._onPartitionsAssigned = function () { seen.push(this); };
            sinon.stub(gc, 'unsubscribe').returns(Promise.resolve());

            return gc._updateSubscriptions([{ topic: 't', partitions: [1] }]).then(function () {
                seen.should.have.length(2);
                seen[0].should.equal(gc);
                seen[1].should.equal(gc);
            });
        });

        it('should finish a cooperative rebalance when onPartitionsRevoked throws', function () {
            var gc = new Kafka.GroupConsumer({ clientId: 'unit-gc', groupId: 'gc-unit-cb' });
            gc._cooperative = true;
            gc.strategyName = 'S';
            gc.strategies = { S: { handler: noop } };
            gc.ownedPartitions = [{ topic: 't', partitions: [0, 1] }];
            gc._onPartitionsRevoked = function () { throw new Error('callback bug'); };
            sinon.stub(gc, 'unsubscribe').returns(Promise.resolve());

            return gc._updateSubscriptions([{ topic: 't', partitions: [1] }]).then(function () {
                gc._needsRejoin.should.equal(true); // KIP-429: rejoin to release the revoked partition
                gc.ownedPartitions.should.eql([{ topic: 't', partitions: [1] }]);
            });
        });
    });
});
