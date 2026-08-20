'use strict';

/* global describe, it, beforeEach */

// Unit tests for the mechanism-C residual-gap FIX: `_fullRejoin` now drains whatever this
// instance owned BEFORE wiping `ownedPartitions`, mirroring `_updateSubscriptionsCooperative`.
//
// Bug this closes: `_fullRejoin` unconditionally set `self.ownedPartitions = []` before the
// subsequent `_syncGroup`/`_updateSubscriptionsCooperative` call. Since that call computes what
// to revoke by diffing the NEW assignment against `self.ownedPartitions`, an already-empty
// `ownedPartitions` means `toRevoke` is always empty on a full rejoin — so `onPartitionsRevoked`
// (the callback that drains in-flight, already-emitted-but-uncommitted work in kafka-source.js)
// never fires, and whatever was in-flight at that moment is silently abandoned and later
// re-delivered. `_fullRejoin` runs whenever `_rejoin`'s own bounded retries are exhausted or a
// heartbeat request itself fails — exactly the failure mode a chronic rebalance storm produces.
//
// No broker — everything is stubbed, like test/29, test/30, test/31.

var Kafka = require('../lib/index');

describe('Full rejoin drains previously-owned partitions before proceeding (mechanism C residual fix)', function () {
    var consumer;

    beforeEach(function () {
        consumer = new Kafka.GroupConsumer({ connectionString: 'localhost:9092' });
        ['debug', 'log', 'warn', 'error'].forEach(function (m) { consumer.client[m] = function () {}; });
        consumer.client.updateGroupCoordinator = function () { return Promise.resolve(); };
        consumer._rejoin = function () { return Promise.resolve(); };
        consumer._heartbeat = function () { return Promise.resolve(); }; // avoid real heartbeat loop
    });

    it('drains previously-owned partitions before wiping ownedPartitions / rejoining', function () {
        var revokedSeen = null, ownedPartitionsAtRejoinTime = 'not-yet-called';
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0, 1] }];
        consumer._onPartitionsRevoked = function (parts) {
            revokedSeen = parts;
            return new Promise(function (resolve) { setTimeout(resolve, 5); });
        };
        consumer._rejoin = function () {
            ownedPartitionsAtRejoinTime = consumer.ownedPartitions.slice();
            return Promise.resolve();
        };

        return consumer._fullRejoin().then(function () {
            revokedSeen.should.deep.equal([
                { topic: 'reward-topic', partition: 0 },
                { topic: 'reward-topic', partition: 1 }
            ]);
            // ownedPartitions must already be wiped by the time _rejoin runs (rejoin still
            // reports a clean slate), but only AFTER the drain resolved.
            ownedPartitionsAtRejoinTime.should.deep.equal([]);
            consumer.ownedPartitions.should.deep.equal([]);
        });
    });

    it('does not call onPartitionsRevoked when nothing was owned', function () {
        var called = false;
        consumer.ownedPartitions = [];
        consumer._onPartitionsRevoked = function () { called = true; return Promise.resolve(); };

        return consumer._fullRejoin().then(function () {
            called.should.equal(false);
            consumer.ownedPartitions.should.deep.equal([]);
        });
    });

    it('does not hang waiting on the drain before rejoining (drain resolves asynchronously)', function () {
        var rejoinCalledBeforeDrainResolved = false, releaseDrain, p;
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0] }];
        consumer._onPartitionsRevoked = function () {
            return new Promise(function (resolve) { releaseDrain = resolve; });
        };
        consumer._rejoin = function () {
            rejoinCalledBeforeDrainResolved = true;
            return Promise.resolve();
        };

        p = consumer._fullRejoin();
        return Promise.resolve().then(function () {
            rejoinCalledBeforeDrainResolved.should.equal(false, '_rejoin ran before the drain resolved');
            releaseDrain();
            return p;
        }).then(function () {
            rejoinCalledBeforeDrainResolved.should.equal(true);
        });
    });

    it('falls back to wiping/rejoining if the drain rejects (degraded, no hang)', function () {
        var rejoined = false;
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0] }];
        consumer._onPartitionsRevoked = function () { return Promise.reject(new Error('drain boom')); };
        consumer._rejoin = function () { rejoined = true; return Promise.resolve(); };

        return consumer._fullRejoin().then(function () {
            rejoined.should.equal(true);
            consumer.ownedPartitions.should.deep.equal([]);
        });
    });

    it('degrades if the drain never settles (bounded by revokeTimeout)', function () {
        var rejoined = false, warnedWith = null;
        consumer.options.revokeTimeout = 20;
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0] }];
        consumer.client.warn = function () { warnedWith = Array.prototype.slice.call(arguments); };
        consumer._onPartitionsRevoked = function () { return new Promise(function () {}); }; // never settles
        consumer._rejoin = function () { rejoined = true; return Promise.resolve(); };

        return consumer._fullRejoin().then(function () {
            rejoined.should.equal(true);
            warnedWith[0].should.match(/onPartitionsRevoked drain failed during full rejoin/);
        });
    });
});
