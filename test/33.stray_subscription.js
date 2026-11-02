'use strict';

/* global describe, it, beforeEach */

// CORE-4239, PR #2 review 5708446109 (GAP Prod, 2026-10-29). A partition this member no longer
// owns must not survive a rebalance in `subscriptions`. If it does, nothing removes it, because the
// revoke diff is computed from ownedPartitions rather than from subscriptions. The member then
// keeps fetching and committing a partition that its new owner is also consuming.
//
// The route seen in production: a fetch fails with NotLeaderForPartition, and _updateSubscription
// starts re-subscribing that partition. While findLeader is still in flight, a full rejoin drops
// the partition. When findLeader resolves, the re-subscribe used to write the partition back at
// its old position.

var helpers = require('./helpers');

function deferred() {
    var d = {};
    d.promise = new Promise(function (resolve) { d.resolve = resolve; });
    return d;
}

describe('A revoked partition cannot be resurrected into the fetch loop', function () {
    var consumer, slowLeader;

    beforeEach(function () {
        consumer = helpers.stubbedGroupConsumer().consumer;
        consumer._cooperative = true;
        consumer.client.updateGroupCoordinator = function () { return Promise.resolve(); };
        consumer._heartbeat = function () { return Promise.resolve(); };
        consumer._onPartitionsRevoked = function () { return Promise.resolve(); };
        consumer.fetchOffset = function (reqs) {
            return Promise.resolve(reqs.map(function (r) {
                return { topic: r.topic, partition: r.partition, offset: 90 };
            }));
        };
        consumer.subscriptions = helpers.inFlightSubscription(function () {}, 110, 0);
        Object.assign(consumer.subscriptions, helpers.inFlightSubscription(function () {}, 90, 1));
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [0, 1] }];
        // Partition 0 goes to another member; only partition 1 comes back.
        consumer._rejoin = function () {
            return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [1] }]);
        };

        // findLeader for partition 0 stays pending until the test releases it. This simulates
        // the leader lookup during a rolling broker restart.
        slowLeader = deferred();
        consumer.client.findLeader = function (topic, partition) {
            return partition === 0 ? slowLeader.promise : Promise.resolve(0);
        };
    });

    it('drops a re-subscribe that resolves after a full rejoin gave the partition away', function () {
        var pending = consumer._updateSubscription('reward-topic', 0);

        // setImmediate lets _updateSubscription read the entry and reach findLeader first.
        return new Promise(function (resolve) { setImmediate(resolve); })
        .then(function () { return consumer._fullRejoin(); })
        .then(function () {
            slowLeader.resolve(0);
            return pending;
        })
        .then(function () {
            Object.keys(consumer.subscriptions).should.deep.equal(['reward-topic:1']);
        });
    });

    it('drops a re-subscribe that resolves while the full-rejoin drain is running', function () {
        var pending = consumer._updateSubscription('reward-topic', 0);

        consumer._onPartitionsRevoked = function () {
            slowLeader.resolve(0);
            return pending.then(function () {});
        };

        return new Promise(function (resolve) { setImmediate(resolve); })
        .then(function () { return consumer._fullRejoin(); })
        .then(function () {
            Object.keys(consumer.subscriptions).should.deep.equal(['reward-topic:1']);
        });
    });

    it('does not store a LeaderNotAvailable placeholder for a partition dropped meanwhile', function () {
        var pending, notAvailable = new Error('LeaderNotAvailable');
        notAvailable.code = 'LeaderNotAvailable';

        consumer.client.findLeader = function () {
            return slowLeader.promise.then(function () { throw notAvailable; });
        };
        pending = consumer._updateSubscription('reward-topic', 0);

        return new Promise(function (resolve) { setImmediate(resolve); })
        .then(function () {
            consumer.unsubscribe('reward-topic', 0);
            slowLeader.resolve();
            return pending;
        })
        .then(function () {
            consumer.subscriptions.should.not.have.property('reward-topic:0');
        });
    });

    it('refreshes in place, keeping offset, paused and maxBytes advanced meanwhile', function () {
        // The fetch loop does not always wait for the re-subscribe (mapConcurrent fails fast), so
        // a later fetch round can advance and pause the same entry while findLeader is pending.
        var entry = consumer.subscriptions['reward-topic:0'], pending;

        entry.maxBytes = 10 * 1024 * 1024;
        pending = consumer._updateSubscription('reward-topic', 0);

        return new Promise(function (resolve) { setImmediate(resolve); })
        .then(function () {
            entry.offset = 150;
            entry.paused = true;
            slowLeader.resolve(3);
            return pending;
        })
        .then(function () {
            consumer.subscriptions['reward-topic:0'].should.equal(entry);
            entry.offset.should.equal(150);
            entry.paused.should.equal(true);
            entry.maxBytes.should.equal(10 * 1024 * 1024);
            entry.leader.should.equal(3);
        });
    });

    it('keeps the live entry when the refresh finds no leader', function () {
        var entry = consumer.subscriptions['reward-topic:0'], pending, notAvailable;

        notAvailable = new Error('LeaderNotAvailable');
        notAvailable.code = 'LeaderNotAvailable';
        consumer.client.findLeader = function () {
            return slowLeader.promise.then(function () { throw notAvailable; });
        };
        pending = consumer._updateSubscription('reward-topic', 0);

        return new Promise(function (resolve) { setImmediate(resolve); })
        .then(function () {
            entry.offset = 150;
            entry.paused = true;
            slowLeader.resolve();
            return pending;
        })
        .then(function () {
            consumer.subscriptions['reward-topic:0'].should.equal(entry);
            entry.leader.should.equal(-1);
            entry.offset.should.equal(150);
            entry.paused.should.equal(true);
        });
    });

    it('still refreshes the leader of a partition that stayed subscribed', function () {
        var pending = consumer._updateSubscription('reward-topic', 0);

        slowLeader.resolve(3);
        return pending.then(function () {
            consumer.subscriptions['reward-topic:0'].leader.should.equal(3);
            consumer.subscriptions['reward-topic:0'].offset.should.equal(110);
        });
    });
});

describe('A finishing handler advances only the entry it was started for', function () {
    it('does not unpause or move a newer entry created under the same key', function () {
        var consumer = helpers.stubbedGroupConsumer().consumer, release, started, fetched, replacement;

        started = new Promise(function (resolve) {
            consumer.subscriptions = helpers.inFlightSubscription(function () {
                resolve();
                return new Promise(function (r) { release = r; });
            }, 100, 0);
        });
        consumer.client.fetchRequest = function () {
            return Promise.resolve([{
                topic: 'reward-topic', partition: 0, highwaterMarkOffset: 200,
                messageSet: [{ offset: 100 }, { offset: 109 }]
            }]);
        };
        consumer._closed = true; // run one _fetch round without rescheduling the loop

        fetched = consumer._fetch();
        return started.then(function () {
            // A rebalance dropped the partition and re-subscribed it from the committed offset,
            // and the new entry has its own handler in flight.
            replacement = { topic: 'reward-topic', partition: 0, offset: 90, leader: 0, paused: true,
                handler: function () { return Promise.resolve(); } };
            consumer.subscriptions['reward-topic:0'] = replacement;
            release();
            return fetched;
        })
        .then(function () {
            replacement.offset.should.equal(90);
            replacement.paused.should.equal(true);
        });
    });
});

describe('A cooperative rebalance leaves only the assignment subscribed', function () {
    var consumer;

    beforeEach(function () {
        consumer = helpers.stubbedGroupConsumer().consumer;
        consumer._cooperative = true;
        consumer.client.updateGroupCoordinator = function () { return Promise.resolve(); };
        consumer._heartbeat = function () { return Promise.resolve(); };
        consumer._onPartitionsRevoked = function () { return Promise.resolve(); };
        consumer.fetchOffset = function (reqs) {
            return Promise.resolve(reqs.map(function (r) {
                return { topic: r.topic, partition: r.partition, offset: 90 };
            }));
        };
    });

    it('unsubscribes a partition that ownedPartitions never recorded, across a full rejoin', function () {
        // The stray is subscribed but not in ownedPartitions, so the revoke diff cannot see it.
        consumer.subscriptions = helpers.inFlightSubscription(function () {}, 110, 0);
        Object.assign(consumer.subscriptions, helpers.inFlightSubscription(function () {}, 90, 1));
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [1] }];
        consumer._rejoin = function () {
            return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [1] }]);
        };

        return consumer._fullRejoin().then(function () {
            Object.keys(consumer.subscriptions).should.deep.equal(['reward-topic:1']);
        });
    });

    it('drops a stray before the drain and passes it to onPartitionsRevoked', function () {
        var drain = helpers.controllableDrain(), assigned = null, done;

        consumer.subscriptions = helpers.inFlightSubscription(function () {}, 110, 0);
        Object.assign(consumer.subscriptions, helpers.inFlightSubscription(function () {}, 90, 1));
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [1] }];
        consumer._onPartitionsRevoked = drain.callback;
        consumer._onPartitionsAssigned = function (parts) { assigned = parts; };

        done = consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [1] }]);
        return drain.invoked.then(function () {
            // The stray is out of the fetch loop for the whole drain, not just after it.
            Object.keys(consumer.subscriptions).should.deep.equal(['reward-topic:1']);
            drain.seen.should.deep.equal([{ topic: 'reward-topic', partition: 0 }]);
            consumer._needsRejoin.should.not.equal(true); // the group never counted it as ours
            drain.release();
            return done;
        })
        .then(function () {
            // Ownership did not change, so this is not an assignment event; the stray was
            // already reported to onPartitionsRevoked above.
            (assigned === null).should.equal(true);
        });
    });

    it('drains a stray that is also in the new assignment before re-subscribing it', function () {
        var drain = helpers.controllableDrain(), done, live;

        consumer.subscriptions = helpers.inFlightSubscription(function () {}, 110, 0);
        live = consumer.subscriptions['reward-topic:0'];
        live.paused = true; // its handler is still running
        consumer.ownedPartitions = [];
        consumer._onPartitionsRevoked = drain.callback;

        done = consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [0] }]);
        return drain.invoked.then(function () {
            drain.seen.should.deep.equal([{ topic: 'reward-topic', partition: 0 }]);
            consumer.subscriptions.should.not.have.property('reward-topic:0');
            drain.release();
            return done;
        })
        .then(function () {
            // Subscribed afresh from the committed offset, not left as the live entry.
            consumer.subscriptions['reward-topic:0'].should.not.equal(live);
            consumer.subscriptions['reward-topic:0'].offset.should.equal(90);
        });
    });

    it('a full rejoin still drains a stray when its first attempt fails', function () {
        var seen = [], coordinatorCalls = 0;

        this.timeout(5000); // the retry backs off for 1s
        consumer.subscriptions = helpers.inFlightSubscription(function () {}, 110, 0);
        consumer.ownedPartitions = [];
        consumer.client.updateGroupCoordinator = function () {
            coordinatorCalls++;
            return coordinatorCalls === 1 ? Promise.reject(new Error('coordinator down')) : Promise.resolve();
        };
        consumer._onPartitionsRevoked = function (parts) { seen.push(parts); return Promise.resolve(); };
        consumer._rejoin = function () { return Promise.resolve(); };

        return consumer._fullRejoin().then(function () {
            coordinatorCalls.should.equal(2);
            seen.should.deep.equal([[{ topic: 'reward-topic', partition: 0 }]]);
        });
    });

    it('a full rejoin drops and drains a stray before its own drain', function () {
        var drain = helpers.controllableDrain(), done;

        consumer.subscriptions = helpers.inFlightSubscription(function () {}, 110, 0);
        Object.assign(consumer.subscriptions, helpers.inFlightSubscription(function () {}, 90, 1));
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [1] }];
        consumer._onPartitionsRevoked = drain.callback;
        consumer._rejoin = function () { return Promise.resolve(); };

        done = consumer._fullRejoin();
        return drain.invoked.then(function () {
            Object.keys(consumer.subscriptions).should.deep.equal([]);
            drain.seen.should.deep.equal([
                { topic: 'reward-topic', partition: 1 },
                { topic: 'reward-topic', partition: 0 }
            ]);
            drain.release();
            return done;
        });
    });

    it('keeps a retained partition subscribed at its current position', function () {
        consumer.subscriptions = helpers.inFlightSubscription(function () {}, 110, 0);
        Object.assign(consumer.subscriptions, helpers.inFlightSubscription(function () {}, 120, 1));
        consumer.ownedPartitions = [{ topic: 'reward-topic', partitions: [1] }];

        return consumer._updateSubscriptions([{ topic: 'reward-topic', partitions: [1] }]).then(function () {
            Object.keys(consumer.subscriptions).should.deep.equal(['reward-topic:1']);
            consumer.subscriptions['reward-topic:1'].offset.should.equal(120);
        });
    });
});
