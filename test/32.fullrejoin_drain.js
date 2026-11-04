'use strict';

/* global describe, it, beforeEach */

// CORE-4239 residual gap: `_fullRejoin` wiped `ownedPartitions` before rejoining, and the revoke
// diff in `_updateSubscriptionsCooperative` is computed against `ownedPartitions` — so a full
// rejoin (bounded rejoin retries exhausted, or a failed heartbeat request) left `toRevoke` empty
// and never drained the in-flight work. It now drains what it owns before the wipe.

var helpers = require('./helpers');

describe('Full rejoin drains previously-owned partitions before proceeding', function () {
    var consumer;

    beforeEach(function () {
        consumer = helpers.stubbedGroupConsumer().consumer;
        consumer._cooperative = true; // ownedPartitions is only ever populated in cooperative mode
        consumer.client.updateGroupCoordinator = function () { return Promise.resolve(); };
        consumer._rejoin = function () { return Promise.resolve(); };
        consumer._heartbeat = function () { return Promise.resolve(); }; // avoid the real heartbeat loop
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

    it('drains with a usable memberId so the callback can actually commit', function () {
        // Regression: memberId was nulled before the drain, and commitOffset rejects outright
        // while memberId is null — so every full-rejoin drain failed before it could commit.
        var committed = null;

        consumer.memberId = 'member-1';
        consumer.generationId = 7;
        consumer.client.offsetCommitRequestV2 = function (groupId, memberId, generationId, reqs) {
            committed = { memberId: memberId, reqs: reqs };
            return Promise.resolve();
        };
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0] }];
        consumer._onPartitionsRevoked = function (parts) {
            return consumer.commitOffset(parts.map(function (p) {
                return { topic: p.topic, partition: p.partition, offset: 109 };
            }));
        };

        return consumer._fullRejoin().then(function () {
            (committed === null).should.equal(false, 'revoke callback could not commit during the drain');
            committed.memberId.should.equal('member-1');
            (consumer.memberId === null).should.equal(true); // still cleared before the rejoin
        });
    });

    it('re-resolves the group coordinator before draining, not after', function () {
        // The common trigger for a full rejoin is a heartbeat failure against a coordinator that
        // is already unhealthy. Draining first meant issuing commits through that dead connection
        // and burning the whole revokeTimeout to watch them fail.
        var order = [];

        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0] }];
        consumer.client.updateGroupCoordinator = function () {
            order.push('updateGroupCoordinator');
            return Promise.resolve();
        };
        consumer._onPartitionsRevoked = function () {
            order.push('drain');
            return Promise.resolve();
        };

        return consumer._fullRejoin().then(function () {
            order.should.deep.equal(['updateGroupCoordinator', 'drain']);
        });
    });

    it('retries with ownedPartitions intact when the coordinator lookup fails', function () {
        // The drain must not be skipped for good just because the first lookup failed — the next
        // attempt still has something to drain.
        var attempts = 0, drained = null;

        consumer.options.maxRejoinAttempts = 2;
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0] }];
        consumer.client.updateGroupCoordinator = function () {
            attempts++;
            return attempts === 1 ? Promise.reject(new Error('no coordinator')) : Promise.resolve();
        };
        consumer._onPartitionsRevoked = function (parts) {
            drained = parts;
            return Promise.resolve();
        };

        return consumer._fullRejoin().then(function () {
            attempts.should.equal(2);
            drained.should.deep.equal([{ topic: 'reward-topic', partition: 0 }]);
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

    it('does not rejoin until the drain resolves', function () {
        var drain = helpers.controllableDrain(), rejoined = false, rejoin;

        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0] }];
        consumer._onPartitionsRevoked = drain.callback;
        consumer._rejoin = function () {
            rejoined = true;
            return Promise.resolve();
        };

        rejoin = consumer._fullRejoin();

        return drain.invoked.then(function () {
            rejoined.should.equal(false, '_rejoin ran before the drain resolved');
            drain.release();
            return rejoin;
        }).then(function () {
            rejoined.should.equal(true);
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
            warnedWith[0].should.contain('full rejoin'); // degraded, and from the full-rejoin path
        });
    });

    it('removes owned partitions from the fetch loop before the drain runs (cooperative)', function () {
        // _fetch polls anything still in self.subscriptions that is not `paused`. An owned
        // partition sitting idle when the rejoin starts is not paused, so without this it keeps
        // being fetched for the whole drain and parks work the drain has already stopped looking
        // at — which then commits for a partition this member no longer owns.
        var subscribedDuringDrain = null;

        consumer.subscriptions = {
            'reward-topic:0': { topic: 'reward-topic', partition: 0, offset: 110, leader: 0 },
            'reward-topic:1': { topic: 'reward-topic', partition: 1, offset: 90, leader: 0 }
        };
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0, 1] }];
        consumer._onPartitionsRevoked = function () {
            subscribedDuringDrain = Object.keys(consumer.subscriptions);
            return Promise.resolve();
        };

        return consumer._fullRejoin().then(function () {
            subscribedDuringDrain.should.deep.equal([]);
        });
    });

    it('leaves subscriptions alone in eager mode, so the eager drain still sees them', function () {
        // Guard against "fix" it by wiping self.subscriptions. ownedPartitions is only ever
        // assigned in _updateSubscriptionsCooperative, so in eager mode it stays empty and the
        // full-rejoin drain no-ops by design — the real eager drain is in _updateSubscriptions,
        // which derives previousPartitions from Object.keys(self.subscriptions). Emptying that map
        // here would silently disable drain-on-rejoin for every eager consumer.
        var drainCalls = 0, eagerDrainSaw = null;

        consumer._cooperative = false; // beforeEach sets it true for the other cases
        consumer.subscriptions = helpers.inFlightSubscription(function () { return Promise.resolve(); }, 110, 0);
        consumer.ownedPartitions = [];
        consumer.fetchOffset = function (reqs) {
            return Promise.resolve(reqs.map(function (r) {
                return { topic: r.topic, partition: r.partition, offset: 110 };
            }));
        };
        consumer._onPartitionsRevoked = function (parts) {
            drainCalls += 1;
            eagerDrainSaw = parts;
            return Promise.resolve();
        };
        consumer._rejoin = function () {
            return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]);
        };

        return consumer._fullRejoin().then(function () {
            drainCalls.should.equal(1);
            eagerDrainSaw.should.deep.equal([{ topic: 'reward-topic', partition: 0 }]);
        });
    });

    it('does not leave a partition subscribed after it is reassigned away', function () {
        // Everything owned is dropped before the drain, and the rejoin re-subscribes only what the
        // new assignment contains — so a partition handed to another member does not linger in the
        // fetch loop with nothing left to remove it.
        consumer.subscriptions = {
            'reward-topic:0': { topic: 'reward-topic', partition: 0, offset: 110, leader: 0 },
            'reward-topic:1': { topic: 'reward-topic', partition: 1, offset: 90, leader: 0 }
        };
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0, 1] }];
        consumer.fetchOffset = function (reqs) {
            return Promise.resolve(reqs.map(function (r) {
                return { topic: r.topic, partition: r.partition, offset: 90 };
            }));
        };
        consumer._onPartitionsRevoked = function () { return Promise.resolve(); };
        consumer._rejoin = function () {
            // partition 0 went to another member; only partition 1 comes back.
            return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [1] }]);
        };

        return consumer._fullRejoin().then(function () {
            Object.keys(consumer.subscriptions).should.deep.equal(['reward-topic:1']);
        });
    });
});
