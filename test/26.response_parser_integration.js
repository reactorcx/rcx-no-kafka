'use strict';

/* global describe, it, before, after */

var Kafka = require('../lib/index');
var promiseUtils = require('../lib/promise-utils');

var API_KEY_FETCH = 1;
var API_KEY_OFFSET = 2;
var API_KEY_OFFSET_FETCH = 9;
var API_KEY_DESCRIBE_GROUPS = 15;

var producer = new Kafka.Producer({ requiredAcks: 1, clientId: 'integ-producer' });
var consumer = new Kafka.SimpleConsumer({ idleTimeout: 100, clientId: 'integ-consumer' });
var groupConsumer = new Kafka.GroupConsumer({
    groupId: 'integ-parser-test-group',
    timeout: 1000,
    idleTimeout: 100,
    heartbeatTimeout: 100,
    clientId: 'integ-group-consumer'
});
var admin = new Kafka.GroupAdmin({ clientId: 'integ-admin' });

// Cap apiVersions[apiKey].max on a list of connections, return restore function
function capApiVersions(connections, apiKey, maxVersion) {
    var saved = [], i, conn;
    for (i = 0; i < connections.length; i++) {
        conn = connections[i];
        if (conn && conn.apiVersions && conn.apiVersions[apiKey]) {
            saved.push({ conn: conn, original: conn.apiVersions[apiKey].max });
            conn.apiVersions[apiKey].max = maxVersion;
        }
    }
    return function restore() {
        var j;
        for (j = 0; j < saved.length; j++) {
            saved[j].conn.apiVersions[apiKey].max = saved[j].original;
        }
    };
}

// Collect all broker connections from a client (brokerConnections values + initialBrokers)
function getBrokerConnections(client) {
    var conns = client.initialBrokers.slice();
    var keys = Object.keys(client.brokerConnections);
    var i;
    for (i = 0; i < keys.length; i++) {
        conns.push(client.brokerConnections[keys[i]]);
    }
    return conns;
}

describe('Response Parser Integration Tests', function () {
    this.timeout(30000);

    before(function () {
        return Promise.all([
            producer.init(),
            consumer.init(),
            admin.init(),
            groupConsumer.init({
                subscriptions: ['kafka-test-topic'],
                metadata: 'test-metadata',
                handler: function () {}
            })
        ]);
    });

    after(function () {
        return Promise.all([
            producer.end(),
            consumer.end(),
            admin.end(),
            groupConsumer.end()
        ]);
    });

    it('MetadataResponseV13: should parse topicIds from broker', function () {
        // V13 is the production version with Kafka 4.1.1 — no cap needed
        // Must request specific topic to get topic metadata populated
        return producer.client.updateMetadata(['kafka-test-topic']).then(function () {
            var topicId, partitions;

            producer.client.topicIds.should.have.property('kafka-test-topic');
            topicId = producer.client.topicIds['kafka-test-topic'];
            topicId.should.be.a('string');
            // UUID format: 8-4-4-4-12 hex chars
            topicId.should.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);

            producer.client.topicMetadata.should.have.property('kafka-test-topic');
            partitions = producer.client.topicMetadata['kafka-test-topic'];
            partitions.should.be.an('object');
            Object.keys(partitions).length.should.be.gt(0);
            partitions[0].should.have.property('partitionId');
            partitions[0].should.have.property('leader');
        });
    });

    it('FetchResponseV4: should fetch messages using legacy v4 parser', function () {
        // Kafka 4.1.1 broker min Fetch version is 4, so cap to 4 (not 3)
        var restore = capApiVersions(getBrokerConnections(consumer.client), API_KEY_FETCH, 4);
        var received = false;

        return producer.send({
            topic: 'kafka-test-topic',
            partition: 0,
            message: { value: 'fetch-v4-test' }
        })
        .then(function () {
            return consumer.offset('kafka-test-topic', 0);
        })
        .then(function (offset) {
            var handler = function (messageSet) {
                var i;
                for (i = 0; i < messageSet.length; i++) {
                    if (messageSet[i].message.value.toString('utf8') === 'fetch-v4-test') {
                        received = true;
                    }
                }
            };
            return consumer.subscribe('kafka-test-topic', 0, { offset: offset - 1 }, handler)
            .then(promiseUtils.delayChain(1000))
            .then(function () {
                return consumer.unsubscribe('kafka-test-topic', 0);
            })
            .then(function () {
                restore();
                received.should.equal(true, 'Should have received the message via FetchResponseV4');
            });
        })
        .catch(function (err) {
            restore();
            throw err;
        });
    });

    it('DescribeGroupResponseV4: should parse group description using v4 parser', function () {
        var groupId = groupConsumer.options.groupId;

        // Resolve the coordinator connection for the admin's client
        return admin.client._findGroupCoordinator(groupId).then(function (coordConn) {
            var restore = capApiVersions([coordConn], API_KEY_DESCRIBE_GROUPS, 4);

            return admin.describeGroup(groupId).then(function (group) {
                restore();
                group.should.be.an('object');
                group.should.have.property('error', null);
                group.should.have.property('groupId', groupId);
                group.should.have.property('state');
                group.should.have.property('protocolType', 'consumer');
                group.should.have.property('members').that.is.an('array');
                group.should.have.property('authorizedOperations');
                group.members.length.should.be.gte(1);
                group.members[0].should.have.property('memberId').that.is.a('string');
                group.members[0].should.have.property('clientId');
            })
            .catch(function (err) {
                restore();
                throw err;
            });
        });
    });

    it('DescribeGroupResponseV5: should parse group description using v5 flexible parser', function () {
        var groupId = groupConsumer.options.groupId;

        return admin.client._findGroupCoordinator(groupId).then(function (coordConn) {
            var restore = capApiVersions([coordConn], API_KEY_DESCRIBE_GROUPS, 5);

            return admin.describeGroup(groupId).then(function (group) {
                restore();
                group.should.be.an('object');
                group.should.have.property('error', null);
                group.should.have.property('groupId', groupId);
                group.should.have.property('state');
                group.should.have.property('protocolType', 'consumer');
                group.should.have.property('members').that.is.an('array');
                group.should.have.property('authorizedOperations');
                group.members.length.should.be.gte(1);
                group.members[0].should.have.property('memberId').that.is.a('string');
                group.members[0].should.have.property('clientId');
            })
            .catch(function (err) {
                restore();
                throw err;
            });
        });
    });

    it('OffsetFetchResponseV3: should fetch consumer offsets using v3 parser', function () {
        var groupId = groupConsumer.options.groupId;

        return admin.client._findGroupCoordinator(groupId).then(function (coordConn) {
            var restore = capApiVersions([coordConn], API_KEY_OFFSET_FETCH, 4);

            return admin.fetchConsumerLag(groupId, [{
                topicName: 'kafka-test-topic',
                partitions: [0, 1, 2]
            }])
            .then(function (results) {
                restore();
                results.should.be.an('array').with.length(3);
                results[0].should.have.property('topic', 'kafka-test-topic');
                results[0].should.have.property('partition');
                results[0].should.have.property('offset');
                results[0].should.have.property('highwaterMark');
                results[0].should.have.property('consumerLag');
            })
            .catch(function (err) {
                restore();
                throw err;
            });
        });
    });

    it('OffsetResponseV2: should fetch partition offsets using v2 parser', function () {
        var restore = capApiVersions(getBrokerConnections(consumer.client), API_KEY_OFFSET, 3);

        return consumer.offset('kafka-test-topic', 0).then(function (offset) {
            restore();
            offset.should.be.a('number');
            offset.should.be.gte(0);
        })
        .catch(function (err) {
            restore();
            throw err;
        });
    });
});
