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

        it('should refresh metadata for an assigned partition it has none for, then fetch it', function () {
            var sc = makeShareConsumer(), p;
            sc.assignedPartitions = { id1: [0, 1] };
            sc.client.topicNames = { id1: 't' };
            sc.client.topicIds.t = 'id1';
            sc.client.topicMetadata = Object.create(null);
            sc.client.topicMetadata.t = { 0: { partitionId: 0, leader: 0 } }; // partition 1 added since the last refresh
            sinon.stub(sc.client, 'updateMetadata').callsFake(function () {
                sc.client.topicMetadata.t[1] = { partitionId: 1, leader: 0 };
                return Promise.resolve();
            });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([]));

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                sc.client.updateMetadata.args[0][0].should.eql(['t']);
                sc.client.shareFetchRequest.args[0][3].should.eql({ 0: [{ topicName: 't', topicId: 'id1', partitions: [{ partition: 0 }, { partition: 1 }] }] });
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

        it('should still close, with its acks, a session an assignment change reset', function () {
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

        it('should not send a fetch that was waiting on a metadata refresh when end() ran', function () {
            var sc = makeShareConsumer(), refreshed, p;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2 };
            sc.client._updateMetadata_pending = true; // someone else's refresh in flight
            sc.client._updateMetadata_running = new Promise(function (resolve) { refreshed = resolve; });
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'shareAcknowledgeRequest').returns(Promise.resolve([]));
            sinon.stub(sc.client, 'end').returns(Promise.resolve());

            p = sc._fetch();
            return promiseUtils.delay(0).then(function () {
                var ended = sc.end();
                sc.client._updateMetadata_pending = false;
                refreshed();
                return Promise.all([p, ended]);
            }).then(function () {
                sc.client.shareFetchRequest.callCount.should.equal(0);
            });
        });

        it('should keep acks for a partition the next fetch does not carry', function () {
            var sc = makeShareConsumer(), p;
            twoLeaders(sc);
            sc.shareSessionEpochs = { 0: 2, 1: 2 };
            sinon.stub(sc, '_buildFetchRequests').returns({ 0: [{ topicName: 't', partitions: [{ partition: 0 }] }] });
            sinon.stub(sc.client, 'shareFetchRequest').returns(Promise.resolve([]));
            markDelivered(sc, 't', 0, [1]);
            markDelivered(sc, 't', 1, [1]);
            sc.acknowledge([{ topic: 't', partition: 0, offset: 1 }, { topic: 't', partition: 1, offset: 1 }]);

            p = sc._fetch();
            sc._closed = true;
            return p.then(function () {
                Object.keys(sc.client.shareFetchRequest.args[0][5].t).should.eql(['0']);
                Object.keys(sc._pendingAcks).should.eql(['t:1']); // not lost: sent when 1 is fetched again, or on end()
            });
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
                // the session went with the failed request: the ack can't be sent, and that is said
                sc.client.warn.args.some(function (a) { return /No open share session/.test(a[0]) && /t:0/.test(a[1]); }).should.equal(true);
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
                var requests = sc.client.shareAcknowledgeRequest.args[0][3];
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
                var args = sc.client.shareAcknowledgeRequest.args[0];
                args[2].should.equal(-1);
                args[3].should.eql({ 0: [{ topicName: 't', partitions: [{
                    partitionIndex: 0, acknowledgementBatches: [{ firstOffset: 1, lastOffset: 1, acknowledgeTypes: [1], topicId: undefined }]
                }] }] });
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
