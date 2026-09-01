'use strict';

/* global describe, it, beforeEach */

// CORE-4239: `_updateSubscriptions` / `_updateSubscriptionsCooperative` await the
// `onPartitionsRevoked` callback before fetching offsets and re-subscribing. The consumer uses
// that callback to commit its in-flight, already-emitted batch, so the re-subscribe reads a
// committed offset past the batch and the rejoin no longer re-delivers it.

var helpers = require('./helpers');

describe('Rejoin drains in-flight commits before re-subscribe', function () {
    var consumer, subscribeArgs, handler;

    beforeEach(function () {
        var stub = helpers.stubbedGroupConsumer();
        consumer = stub.consumer;
        subscribeArgs = stub.subscribeArgs;
        handler = stub.handler;
        consumer._cooperative = false;
    });

    it('re-subscribes PAST the in-flight batch once the drain commits it (eager, no replay)', function () {
        // Batch [100..109] in flight; committed pointer at 100. The draining revoke callback
        // simulates the in-flight commit landing (committed -> 110) and resolves ASYNC, so the
        // re-subscribe sees 110 only if it actually awaited the drain.
        var committed = 100;

        consumer.subscriptions = helpers.inFlightSubscription(handler);
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
        var drain = helpers.controllableDrain(), released = false, update;

        consumer.subscriptions = helpers.inFlightSubscription(handler);
        consumer._onPartitionsRevoked = drain.callback;
        consumer.fetchOffset = function () {
            released.should.equal(true, 'fetchOffset ran before the drain resolved');
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 110 }]);
        };

        update = consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]);

        return drain.invoked.then(function () {
            subscribeArgs.should.have.length(0); // nothing subscribed while the drain is pending
            released = true;
            drain.release();
            return update;
        }).then(function () {
            subscribeArgs.should.have.length(1);
        });
    });

    it('falls back to re-subscribe from committed if the drain rejects (degraded, no hang)', function () {
        consumer.subscriptions = helpers.inFlightSubscription(handler);
        consumer._onPartitionsRevoked = function () { return Promise.reject(new Error('drain boom')); };
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 100 }]);
        };

        return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]).then(function () {
            subscribeArgs[0].options.should.deep.equal({ offset: 100 });
        });
    });

    it('cooperative: drains the revoked partition before subscribing the added partition', function () {
        var committed = 100, revokedSeen = null, addSub;

        consumer._cooperative = true;
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0] }];
        consumer.subscriptions = helpers.inFlightSubscription(handler);
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
            addSub = subscribeArgs.filter(function (s) { return s.partition === 1; })[0];
            addSub.options.should.deep.equal({ offset: 110 }); // subscribed only after the drain
        });
    });

    it('eager: a revoked partition cannot be re-fetched while the drain is pending', function () {
        // The subscriptions map is wiped up front, so _fetch (which only looks at
        // Object.keys(self.subscriptions)) cannot poll a revoked partition mid-drain — not even
        // via base_consumer's clearing of `paused` when an in-flight handler resolves.
        var drain = helpers.controllableDrain(), fetchRequestCalls = 0, update;

        consumer.subscriptions = helpers.inFlightSubscription(handler);
        consumer.client.fetchRequest = function () {
            fetchRequestCalls++;
            return Promise.resolve([]);
        };
        consumer._onPartitionsRevoked = drain.callback;
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 110 }]);
        };
        consumer._closed = true; // so the probe _fetch() below doesn't reschedule the real loop

        update = consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [] }]);

        return drain.invoked.then(function () {
            // The drain hasn't resolved yet: the partition must already be gone from the fetch loop.
            (consumer.subscriptions['reward-topic:0'] === undefined).should.equal(true);
            return consumer._fetch(); // simulates the idleTimeout tick firing mid-drain
        }).then(function () {
            fetchRequestCalls.should.equal(0, '_fetch polled a partition that is mid-revoke-drain');
            drain.release();
            return update;
        });
    });

    it('honours an explicit revokeTimeout of 0 as "do not wait"', function () {
        // `revokeTimeout || default` used to read an explicit 0 as unset and wait the full
        // default instead — the opposite of what the caller asked for.
        var warnedWith = null, drainSettled = false;

        consumer.options.revokeTimeout = 0;
        consumer.subscriptions = helpers.inFlightSubscription(handler);
        consumer.client.warn = function () { warnedWith = Array.prototype.slice.call(arguments); };
        consumer._onPartitionsRevoked = function () {
            return new Promise(function (resolve) {
                setTimeout(function () { drainSettled = true; resolve(); }, 50);
            });
        };
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 100 }]);
        };

        return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]).then(function () {
            drainSettled.should.equal(false, 'waited for the drain despite revokeTimeout: 0');
            warnedWith[0].should.contain('re-subscribe'); // degraded, and from the eager path
            subscribeArgs[0].options.should.deep.equal({ offset: 100 });
        });
    });

    it('degrades to re-subscribe from committed if the drain never settles (revokeTimeout)', function () {
        var warnedWith = null;

        consumer.options.revokeTimeout = 20;
        consumer.subscriptions = helpers.inFlightSubscription(handler);
        consumer.client.warn = function () { warnedWith = Array.prototype.slice.call(arguments); };
        consumer._onPartitionsRevoked = function () { return new Promise(function () {}); }; // never settles
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 100 }]);
        };

        return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]).then(function () {
            subscribeArgs[0].options.should.deep.equal({ offset: 100 }); // degraded, from committed
            warnedWith[0].should.contain('re-subscribe'); // degraded, and from the eager path
        });
    });
});
