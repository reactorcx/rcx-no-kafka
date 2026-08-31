'use strict';

// Shared stubs for the broker-less GroupConsumer unit tests (test/31, test/32).

var Kafka = require('../lib/index');

/**
 * A GroupConsumer with no broker: loggers silenced, metadata/leader lookups stubbed, and
 * `subscribe` wrapped so a test can assert what got re-subscribed and from which offset.
 *
 * @return {Object} { consumer, subscribeArgs, handler }
 */
function stubbedGroupConsumer() {
    var consumer = new Kafka.GroupConsumer({ connectionString: 'localhost:9092' }),
        handler = function () { return Promise.resolve(); },
        subscribeArgs = [],
        realSubscribe;

    consumer.strategyName = 'TestStrategy';
    consumer.strategies = { TestStrategy: { handler: handler } };
    consumer.topics = ['reward-topic'];
    ['debug', 'log', 'warn', 'error'].forEach(function (m) { consumer.client[m] = function () {}; });
    consumer.client.updateMetadata = function () { return Promise.resolve(); };
    consumer.client.findLeader = function () { return Promise.resolve(0); };

    realSubscribe = consumer.subscribe.bind(consumer);
    consumer.subscribe = function (topic, partition, options, h) {
        subscribeArgs.push({ topic: topic, partition: partition, options: options });
        return realSubscribe(topic, partition, options, h);
    };

    return { consumer: consumer, subscribeArgs: subscribeArgs, handler: handler };
}

/**
 * A subscriptions map holding one partition (default 0) with an in-flight batch: emitted up to
 * `offset` (default 110), not yet committed.
 */
function inFlightSubscription(handler, offset, partition) {
    var result = {}, p = partition === undefined ? 0 : partition;

    result['reward-topic:' + p] = {
        topic: 'reward-topic',
        partition: p,
        offset: offset === undefined ? 110 : offset,
        leader: 0,
        handler: handler
    };

    return result;
}

/**
 * A revoke callback the test drives: `invoked` resolves once the consumer calls it, `release()`
 * lets it finish, `seen` holds the partitions it was called with. Waiting on `invoked` keeps the
 * tests independent of how many microtask hops _drainRevoked takes to reach the callback.
 */
function controllableDrain() {
    var drain = { seen: null, release: null };

    drain.invoked = new Promise(function (resolveInvoked) {
        drain.callback = function (partitions) {
            drain.seen = partitions;
            resolveInvoked();
            return new Promise(function (resolve) { drain.release = resolve; });
        };
    });

    return drain;
}

module.exports = {
    stubbedGroupConsumer: stubbedGroupConsumer,
    inFlightSubscription: inFlightSubscription,
    controllableDrain: controllableDrain
};
