'use strict';

/* global describe, it, beforeEach */

// Unit tests for the mechanism-C FIX: `_updateSubscriptions` / `_updateSubscriptionsCooperative`
// now AWAIT the `onPartitionsRevoked` callback before fetching offsets and re-subscribing.
// rle-api's kafka-source.js makes that callback drain the in-flight, already-emitted batch (it
// resolves only once those offsets commit), so the re-subscribe reads a committed offset PAST
// the batch and the rejoin no longer re-delivers it (no "immutable field '_id'" duplicates).
//
// Counterpart to test/30 (which characterizes the un-drained replay). No broker — everything is
// stubbed, like test/29 and test/30.

var Kafka = require('../lib/index');

describe('Rejoin drains in-flight commits before re-subscribe (mechanism C fix)', function () {
    var consumer, subscribeArgs, handler;

    beforeEach(function () {
        var realSubscribe;
        consumer = new Kafka.GroupConsumer({ connectionString: 'localhost:9092' });
        handler = function () { return Promise.resolve(); };
        consumer._cooperative = false;
        consumer.strategyName = 'TestStrategy';
        consumer.strategies = { TestStrategy: { handler: handler } };
        consumer.topics = ['reward-topic'];
        ['debug', 'log', 'warn', 'error'].forEach(function (m) { consumer.client[m] = function () {}; });
        consumer.client.updateMetadata = function () { return Promise.resolve(); };
        consumer.client.findLeader = function () { return Promise.resolve(0); };
        realSubscribe = consumer.subscribe.bind(consumer);
        subscribeArgs = [];
        consumer.subscribe = function (topic, partition, options, h) {
            subscribeArgs.push({ topic: topic, partition: partition, options: options });
            return realSubscribe(topic, partition, options, h);
        };
    });

    it('re-subscribes PAST the in-flight batch once the drain commits it (eager, no replay)', function () {
        consumer.subscriptions = {
            'reward-topic:0': { topic: 'reward-topic', partition: 0, offset: 110, leader: 0, handler: handler }
        };
        // Batch [100..109] in flight; committed pointer at 100. The draining revoke callback
        // simulates the in-flight commit landing (committed -> 110) and resolves ASYNC, so the
        // re-subscribe reads 110 only if it actually AWAITED the drain.
        var committed = 100;
        consumer._onPartitionsRevoked = function () {
            return new Promise(function (resolve) {
                setTimeout(function () { committed = 110; resolve(); }, 5);
            });
        };
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: committed }]);
        };

        return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]).then(function () {
            subscribeArgs.should.have.length(1);
            subscribeArgs[0].options.should.deep.equal({ offset: 110 });
            consumer.subscriptions['reward-topic:0'].offset.should.equal(110);
        });
    });

    it('does not fetch offsets / re-subscribe until the revoke drain resolves', function () {
        consumer.subscriptions = {
            'reward-topic:0': { topic: 'reward-topic', partition: 0, offset: 110, leader: 0, handler: handler }
        };
        var drainResolved = false, release;
        consumer._onPartitionsRevoked = function () {
            return new Promise(function (resolve) { release = function () { drainResolved = true; resolve(); }; });
        };
        consumer.fetchOffset = function () {
            drainResolved.should.equal(true, 'fetchOffset ran before the drain resolved');
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 110 }]);
        };

        var p = consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]);
        // _onPartitionsRevoked is now invoked from inside a .then (so a synchronous throw is
        // caught), which defers the call by one microtask tick relative to this synchronous
        // return — give that tick a chance to run before asserting/releasing.
        return Promise.resolve().then(function () {
            subscribeArgs.should.have.length(0); // nothing subscribed while the drain is pending
            release();
            return p;
        }).then(function () { subscribeArgs.should.have.length(1); });
    });

    it('falls back to re-subscribe from committed if the drain rejects (degraded, no hang)', function () {
        consumer.subscriptions = {
            'reward-topic:0': { topic: 'reward-topic', partition: 0, offset: 110, leader: 0, handler: handler }
        };
        consumer._onPartitionsRevoked = function () { return Promise.reject(new Error('drain boom')); };
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 100 }]);
        };

        return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]).then(function () {
            subscribeArgs[0].options.should.deep.equal({ offset: 100 });
        });
    });

    it('cooperative: drains the revoked partition before subscribing the added partition', function () {
        consumer._cooperative = true;
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0] }];
        consumer.subscriptions = {
            'reward-topic:0': { topic: 'reward-topic', partition: 0, offset: 110, leader: 0, handler: handler }
        };
        var committed = 100, revokedSeen = null;
        consumer._onPartitionsRevoked = function (parts) {
            revokedSeen = parts;
            return new Promise(function (resolve) {
                setTimeout(function () { committed = 110; resolve(); }, 5);
            });
        };
        consumer.fetchOffset = function (reqs) {
            return Promise.resolve(reqs.map(function (r) {
                return { topic: r.topic, partition: r.partition, offset: committed };
            }));
        };

        // New assignment gives up partition 0 and gains partition 1.
        return consumer._updateSubscriptionsCooperative(
            [{ topic: 'reward-topic', partitions: [1] }], handler
        ).then(function () {
            revokedSeen.should.deep.equal([{ topic: 'reward-topic', partition: 0 }]);
            var addSub = subscribeArgs.filter(function (s) { return s.partition === 1; })[0];
            addSub.options.should.deep.equal({ offset: 110 }); // subscribed only after the drain
        });
    });

    it('eager: a revoked partition cannot be re-fetched while the drain is pending', function () {
        // Regression test for the race the fix closes: _fetch's own handler-completion logic
        // (base_consumer.js) used to reset `paused` back to false once an in-flight handler
        // resolved, which would let the fetch loop poll a revoked-but-not-yet-deleted partition
        // again mid-drain. The fix hard-deletes the subscription entry up front, so _fetch (which
        // only looks at Object.keys(self.subscriptions)) can't see it at all.
        var fetchRequestCalls = 0, releaseDrain, updatePromise;
        consumer.subscriptions = {
            'reward-topic:0': { topic: 'reward-topic', partition: 0, offset: 110, leader: 0, handler: handler }
        };
        consumer.client.fetchRequest = function () {
            fetchRequestCalls++;
            return Promise.resolve([]);
        };
        consumer._onPartitionsRevoked = function () {
            return new Promise(function (resolve) { releaseDrain = resolve; });
        };
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 110 }]);
        };

        updatePromise = consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [] }]);

        return Promise.resolve().then(function () {
            // The drain hasn't resolved yet: the partition must already be gone from the fetch loop.
            (consumer.subscriptions['reward-topic:0'] === undefined).should.equal(true);
            return consumer._fetch(); // simulates the idleTimeout tick firing mid-drain
        }).then(function () {
            fetchRequestCalls.should.equal(0, '_fetch polled a partition that is mid-revoke-drain');
            clearTimeout(consumer._fetchTimeout); // don't let the real loop keep rescheduling
            releaseDrain();
            return updatePromise;
        });
    });

    it('degrades to re-subscribe from committed if the drain never settles (revokeTimeout)', function () {
        var warnedWith = null;
        consumer.options.revokeTimeout = 20;
        consumer.subscriptions = {
            'reward-topic:0': { topic: 'reward-topic', partition: 0, offset: 110, leader: 0, handler: handler }
        };
        consumer.client.warn = function () { warnedWith = Array.prototype.slice.call(arguments); };
        consumer._onPartitionsRevoked = function () { return new Promise(function () {}); }; // never settles
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 100 }]);
        };

        return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]).then(function () {
            subscribeArgs[0].options.should.deep.equal({ offset: 100 }); // degraded, from committed
            warnedWith[0].should.match(/onPartitionsRevoked drain failed/);
        });
    });
});
