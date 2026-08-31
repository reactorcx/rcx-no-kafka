'use strict';

/* global describe, it, beforeEach */

// Characterizes the duplicate-delivery mechanism CORE-4239 fixes: a rejoin re-subscribes every
// partition from its COMMITTED offset, discarding the in-memory position the consumer had
// already reached (base_consumer advances `sub.offset` only after the handler resolves). A batch
// that was already emitted but not yet committed is therefore rewound to and re-delivered —
// with no restart, and on a single instance.
//
// These tests register NO `onPartitionsRevoked` callback, so the revoke drain added in
// CORE-4239 is a no-op and the rewind still stands. That is the intended degraded contract:
// with no draining callback the consumer behaves exactly as it did before the fix. test/31
// covers the drained case, where the re-subscribe lands past the in-flight batch instead.

var helpers = require('./helpers');

describe('Rejoin re-subscribes from committed offset (undrained in-flight replay)', function () {
    var consumer, subscribeArgs, handler;

    beforeEach(function () {
        var stub = helpers.stubbedGroupConsumer();
        consumer = stub.consumer;
        subscribeArgs = stub.subscribeArgs;
        handler = stub.handler;
        consumer._cooperative = false; // eager path — the default group protocol
    });

    // The core defect: the in-memory position is AHEAD of the committed offset because a batch
    // was emitted but not yet committed, and a rejoin throws that progress away.
    it('rewinds an in-flight partition to its committed offset (re-delivering the batch)', function () {
        // Batch [100..109] in flight: in-memory offset advanced to 110, commit pointer at 100.
        consumer.subscriptions = helpers.inFlightSubscription(handler, 110);
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 100 }]);
        };

        return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]).then(function () {
            // Re-subscribed from the committed offset (100), not the in-memory 110 — so the next
            // fetch starts at 100 and re-delivers the already-emitted batch [100..109].
            subscribeArgs.should.have.length(1);
            subscribeArgs[0].options.should.deep.equal({ offset: 100 });
            consumer.subscriptions['reward-topic:0'].offset.should.equal(100);
        });
    });

    // Control: with the commit pointer caught up there is no in-flight gap, and the same rejoin
    // path is a harmless no-op — the replay window is exactly the uncommitted emitted batch.
    it('does not replay when committed offset == consumed position', function () {
        consumer.subscriptions = helpers.inFlightSubscription(handler, 110);
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 0, offset: 110 }]);
        };

        return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]).then(function () {
            subscribeArgs[0].options.should.deep.equal({ offset: 110 });
            consumer.subscriptions['reward-topic:0'].offset.should.equal(110);
        });
    });

    // The shape of the production incident: one instance, no restart, a whole batch replayed.
    it('rewinds the whole in-flight batch on a single instance, with no restart', function () {
        // 21-record batch [500..520] in flight; commit pointer still at 500.
        consumer.subscriptions = helpers.inFlightSubscription(handler, 521, 3);
        consumer.fetchOffset = function () {
            return Promise.resolve([{ topic: 'reward-topic', partition: 3, offset: 500 }]);
        };

        return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [3] }]).then(function () {
            subscribeArgs[0].options.should.deep.equal({ offset: 500 });
            // 21 events re-delivered to the same instance -> 21 duplicate eventIds downstream.
            consumer.subscriptions['reward-topic:3'].offset.should.equal(500);
        });
    });
});
