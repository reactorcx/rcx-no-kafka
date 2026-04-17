'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');
var Client   = require('../lib/client');

describe('KIP-951: Leader Discovery Optimizations (Produce v10/v11)', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    //////////////////////////
    // Produce v10 Request  //
    //////////////////////////

    describe('Produce v10', function () {
        it('should encode ProduceRequestV10', function () {
            var encoded = protocol.write().ProduceRequestV10({
                correlationId: 30,
                clientId: 'test',
                transactionalId: null,
                requiredAcks: -1,
                timeout: 30000,
                topics: [{
                    topicName: 'my-topic',
                    partitions: [{
                        partition: 0,
                        batch: {
                            baseOffset: 0,
                            records: [{
                                key: null,
                                value: Buffer.from('hello', 'utf8')
                            }],
                            codec: 0
                        }
                    }]
                }]
            }).result;
            encoded.length.should.be.above(0);
        });

        it('should decode ProduceResponseV10 with CurrentLeader', function () {
            var writer, result;

            writer = protocol.write();
            // correlationId
            writer.Int32BE(30);
            // response header TaggedFields
            writer.TaggedFields();
            // topics: 1 topic
            writer.compactArray([1], function () {
                this.compactString('my-topic');
                this.compactArray([1], function () {
                    this.Int32BE(0);  // partition
                    this.Int16BE(6);  // error code 6 = NOT_LEADER_OR_FOLLOWER
                    this.Int64BE(0);  // offset
                    this.Int64BE(-1); // timestamp
                    this.Int64BE(0);  // logStartOffset
                    // recordErrors: empty
                    this.compactArray([], function () {});
                    // errorMessage: null
                    this.compactNullableString(null);
                    // Tagged fields: 1 tag — tag 0 = CurrentLeader
                    this.UVarint(1);    // 1 tagged field
                    this.UVarint(0);    // tag id = 0
                    this.UVarint(9);    // size = 4 (leaderId) + 4 (leaderEpoch) + 1 (empty TaggedFields)
                    this.Int32BE(2);    // leaderId = broker 2
                    this.Int32BE(15);   // leaderEpoch = 15
                    this.UVarint(0);    // CurrentLeader's own TaggedFields (empty)
                });
                this.TaggedFields();
            });
            // throttleTime
            writer.Int32BE(0);
            // body TaggedFields (empty)
            writer.TaggedFields();

            result = protocol.read(writer.result).ProduceResponseV10().result;
            result.correlationId.should.equal(30);
            result.topics.length.should.equal(1);
            result.topics[0].topicName.should.equal('my-topic');
            result.topics[0].partitions.length.should.equal(1);
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].currentLeader.leaderId.should.equal(2);
            result.topics[0].partitions[0].currentLeader.leaderEpoch.should.equal(15);
        });

        it('should decode ProduceResponseV10 without tagged fields (graceful fallback)', function () {
            var writer, result;

            writer = protocol.write();
            // correlationId
            writer.Int32BE(31);
            // response header TaggedFields
            writer.TaggedFields();
            // topics: 1 topic
            writer.compactArray([1], function () {
                this.compactString('my-topic');
                this.compactArray([1], function () {
                    this.Int32BE(0);  // partition
                    this.Int16BE(0);  // error code 0 = no error
                    this.Int64BE(42); // offset
                    this.Int64BE(-1); // timestamp
                    this.Int64BE(0);  // logStartOffset
                    this.compactArray([], function () {});
                    this.compactNullableString(null);
                    // No tagged fields
                    this.TaggedFields();
                });
                this.TaggedFields();
            });
            // throttleTime
            writer.Int32BE(0);
            // body TaggedFields (empty)
            writer.TaggedFields();

            result = protocol.read(writer.result).ProduceResponseV10().result;
            result.correlationId.should.equal(31);
            result.topics[0].partitions[0].partition.should.equal(0);
            // currentLeader should not be present when no tagged fields
            (result.topics[0].partitions[0].currentLeader === undefined).should.equal(true);
        });
    });

    //////////////////////////
    // Produce v11 Request  //
    //////////////////////////

    describe('Produce v11', function () {
        it('should encode ProduceRequestV11', function () {
            var encoded = protocol.write().ProduceRequestV11({
                correlationId: 40,
                clientId: 'test',
                transactionalId: null,
                requiredAcks: -1,
                timeout: 30000,
                topics: [{
                    topicName: 'my-topic',
                    partitions: [{
                        partition: 0,
                        batch: {
                            baseOffset: 0,
                            records: [{
                                key: null,
                                value: Buffer.from('world', 'utf8')
                            }],
                            codec: 0
                        }
                    }]
                }]
            }).result;
            encoded.length.should.be.above(0);
        });

        it('should decode ProduceResponseV10 with CurrentLeader and NodeEndpoints', function () {
            var writer, result, nw, nodeData;

            // Build NodeEndpoints data first (protocol.write() reuses same writer instance)
            nw = protocol.write();
            // compactArray with 1 item: UVarint(2) = 1+1
            nw.UVarint(2);
            // item 1: broker 3
            nw.Int32BE(3);
            nw.compactString('broker3.example.com');
            nw.Int32BE(9092);
            nw.compactNullableString(null);
            nw.TaggedFields();
            nodeData = Buffer.from(nw.result);

            // Now write the main response
            writer = protocol.write();
            // correlationId
            writer.Int32BE(40);
            // response header TaggedFields
            writer.TaggedFields();
            // topics: 1 topic
            writer.compactArray([1], function () {
                this.compactString('test-topic');
                this.compactArray([1], function () {
                    this.Int32BE(0);  // partition
                    this.Int16BE(6);  // error code 6 = NOT_LEADER_OR_FOLLOWER
                    this.Int64BE(0);  // offset
                    this.Int64BE(-1); // timestamp
                    this.Int64BE(0);  // logStartOffset
                    this.compactArray([], function () {});
                    this.compactNullableString(null);
                    // Tagged fields: tag 0 = CurrentLeader
                    this.UVarint(1);
                    this.UVarint(0);    // tag id
                    this.UVarint(9);    // size
                    this.Int32BE(3);    // leaderId = broker 3
                    this.Int32BE(20);   // leaderEpoch = 20
                    this.UVarint(0);    // CurrentLeader TaggedFields
                });
                this.TaggedFields();
            });
            // throttleTime
            writer.Int32BE(100);

            // Body-level tagged fields: tag 0 = NodeEndpoints
            writer.UVarint(1);                // 1 tagged field
            writer.UVarint(0);                // tag id = 0
            writer.UVarint(nodeData.length);  // size
            writer.raw(nodeData);

            result = protocol.read(writer.result).ProduceResponseV10().result;
            result.correlationId.should.equal(40);
            result.throttleTime.should.equal(100);
            // CurrentLeader
            result.topics[0].partitions[0].currentLeader.leaderId.should.equal(3);
            result.topics[0].partitions[0].currentLeader.leaderEpoch.should.equal(20);
            // NodeEndpoints
            result.nodeEndpoints.length.should.equal(1);
            result.nodeEndpoints[0].nodeId.should.equal(3);
            result.nodeEndpoints[0].host.should.equal('broker3.example.com');
            result.nodeEndpoints[0].port.should.equal(9092);
            (result.nodeEndpoints[0].rack === null).should.equal(true);
        });

        it('should decode ProduceResponseV10 with multiple NodeEndpoints', function () {
            var writer, result, nw, nodeData;

            // Build NodeEndpoints data first (protocol.write() reuses same writer instance)
            nw = protocol.write();
            // compactArray with 2 items: UVarint(3) = 2+1
            nw.UVarint(3);
            // item 1: broker 1
            nw.Int32BE(1);
            nw.compactString('broker1.local');
            nw.Int32BE(9092);
            nw.compactNullableString('rack-a');
            nw.TaggedFields();
            // item 2: broker 2
            nw.Int32BE(2);
            nw.compactString('broker2.local');
            nw.Int32BE(9093);
            nw.compactNullableString(null);
            nw.TaggedFields();
            nodeData = Buffer.from(nw.result);

            // Now write the main response
            writer = protocol.write();
            // correlationId
            writer.Int32BE(41);
            // response header TaggedFields
            writer.TaggedFields();
            // topics: 1 topic, 1 partition, no error, no partition tagged fields
            writer.compactArray([1], function () {
                this.compactString('my-topic');
                this.compactArray([1], function () {
                    this.Int32BE(0);
                    this.Int16BE(0);
                    this.Int64BE(100);
                    this.Int64BE(-1);
                    this.Int64BE(0);
                    this.compactArray([], function () {});
                    this.compactNullableString(null);
                    this.TaggedFields();
                });
                this.TaggedFields();
            });
            // throttleTime
            writer.Int32BE(0);

            // Body-level tagged fields: tag 0 = NodeEndpoints (2 endpoints)
            writer.UVarint(1);                // 1 tagged field
            writer.UVarint(0);                // tag id = 0
            writer.UVarint(nodeData.length);  // size
            writer.raw(nodeData);

            result = protocol.read(writer.result).ProduceResponseV10().result;
            result.nodeEndpoints.length.should.equal(2);
            result.nodeEndpoints[0].nodeId.should.equal(1);
            result.nodeEndpoints[0].host.should.equal('broker1.local');
            result.nodeEndpoints[0].port.should.equal(9092);
            result.nodeEndpoints[0].rack.should.equal('rack-a');
            result.nodeEndpoints[1].nodeId.should.equal(2);
            result.nodeEndpoints[1].host.should.equal('broker2.local');
            result.nodeEndpoints[1].port.should.equal(9093);
            (result.nodeEndpoints[1].rack === null).should.equal(true);
        });
    });

    ///////////////////////////////////
    // Fetch v12 CurrentLeader       //
    ///////////////////////////////////

    describe('Fetch v12 CurrentLeader', function () {
        it('should decode FetchResponseV12 with CurrentLeader tagged field (tag 1)', function () {
            var writer, result;

            writer = protocol.write();
            // correlationId
            writer.Int32BE(50);
            // response header TaggedFields (flexible)
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // errorCode
            writer.Int16BE(0);
            // sessionId
            writer.Int32BE(0);
            // topics: 1 topic
            writer.compactArray([1], function () {
                this.compactString('my-topic');
                this.compactArray([1], function () {
                    this.Int32BE(0);   // partition
                    this.Int16BE(6);   // error code 6 = NOT_LEADER_OR_FOLLOWER
                    this.Int64BE(100); // highwaterMarkOffset
                    this.Int64BE(90);  // lastStableOffset
                    this.Int64BE(0);   // logStartOffset
                    // abortedTransactions: empty
                    this.compactArray([], function () {});
                    // preferredReadReplica
                    this.Int32BE(-1);
                    // records: null (UVarint 0)
                    this.UVarint(0);
                    // Tagged fields: tag 1 = CurrentLeader (tag 0 is DivergingEpoch)
                    this.UVarint(1);    // 1 tagged field
                    this.UVarint(1);    // tag id = 1
                    this.UVarint(9);    // size = 4 + 4 + 1
                    this.Int32BE(5);    // leaderId = broker 5
                    this.Int32BE(42);   // leaderEpoch = 42
                    this.UVarint(0);    // CurrentLeader's own TaggedFields
                });
                this.TaggedFields();
            });
            // body TaggedFields
            writer.TaggedFields();

            result = protocol.read(writer.result).FetchResponseV12().result;
            result.correlationId.should.equal(50);
            result.topics.length.should.equal(1);
            result.topics[0].topicName.should.equal('my-topic');
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].currentLeader.leaderId.should.equal(5);
            result.topics[0].partitions[0].currentLeader.leaderEpoch.should.equal(42);
        });

        it('should decode FetchResponseV12 without CurrentLeader (graceful fallback)', function () {
            var writer, result;

            writer = protocol.write();
            writer.Int32BE(51);
            writer.TaggedFields();
            writer.Int32BE(0);
            writer.Int16BE(0);
            writer.Int32BE(0);
            writer.compactArray([1], function () {
                this.compactString('my-topic');
                this.compactArray([1], function () {
                    this.Int32BE(0);
                    this.Int16BE(0);
                    this.Int64BE(100);
                    this.Int64BE(90);
                    this.Int64BE(0);
                    this.compactArray([], function () {});
                    this.Int32BE(-1);
                    this.UVarint(0);
                    // No tagged fields
                    this.TaggedFields();
                });
                this.TaggedFields();
            });
            writer.TaggedFields();

            result = protocol.read(writer.result).FetchResponseV12().result;
            result.topics[0].partitions[0].partition.should.equal(0);
            (result.topics[0].partitions[0].currentLeader === undefined).should.equal(true);
        });
    });

    /////////////////////////////
    // _applyLeaderHints       //
    /////////////////////////////

    describe('_applyLeaderHints', function () {
        it('should update topicMetadata leader from currentLeader hints', function () {
            var client = new Client({ connectionString: '127.0.0.1:9092', logger: { logFunction: function () {} } });

            // Set up existing topicMetadata
            client.topicMetadata = {
                'my-topic': {
                    0: { leader: 1, partitionId: 0 },
                    1: { leader: 1, partitionId: 1 }
                }
            };

            // Simulate response with currentLeader hints
            client._applyLeaderHints([{
                topicName: 'my-topic',
                partitions: [
                    { partition: 0, currentLeader: { leaderId: 3, leaderEpoch: 10 } },
                    { partition: 1, currentLeader: { leaderId: 5, leaderEpoch: 12 } }
                ]
            }], null);

            client.topicMetadata['my-topic'][0].leader.should.equal(3);
            client.topicMetadata['my-topic'][1].leader.should.equal(5);
        });

        it('should ignore currentLeader with leaderId -1', function () {
            var client = new Client({ connectionString: '127.0.0.1:9092', logger: { logFunction: function () {} } });

            client.topicMetadata = {
                'my-topic': {
                    0: { leader: 1, partitionId: 0 }
                }
            };

            client._applyLeaderHints([{
                topicName: 'my-topic',
                partitions: [
                    { partition: 0, currentLeader: { leaderId: -1, leaderEpoch: -1 } }
                ]
            }], null);

            // Leader should remain unchanged
            client.topicMetadata['my-topic'][0].leader.should.equal(1);
        });

        it('should create broker connections from nodeEndpoints', function () {
            var client = new Client({ connectionString: '127.0.0.1:9092', logger: { logFunction: function () {} } });

            client.topicMetadata = {};
            client.brokerConnections = {};
            client.brokerRacks = {};
            // stub apiVersionsRequest to be a no-op
            client.apiVersionsRequest = function () {};

            client._applyLeaderHints([], [
                { nodeId: 3, host: 'broker3.example.com', port: 9092, rack: 'rack-a' }
            ]);

            // Broker connection should have been created
            client.brokerConnections[3].should.be.ok; // eslint-disable-line no-unused-expressions
            client.brokerRacks[3].should.equal('rack-a');
        });
    });

    ///////////////////////////////////
    // Fetch v14/v15/v16             //
    ///////////////////////////////////

    describe('Fetch v14 (KIP-405 Tiered Storage)', function () {
        it('should encode FetchRequestV14 (same body as v13 but apiVersion=14)', function () {
            var encoded = protocol.write().FetchRequestV14({
                correlationId: 60,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 65536,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [{
                    topicId: '00000000-0000-0000-0000-000000000001',
                    partitions: [{
                        partition: 0,
                        currentLeaderEpoch: -1,
                        offset: 0,
                        lastFetchedEpoch: -1,
                        logStartOffset: -1,
                        maxBytes: 65536
                    }]
                }],
                forgottenTopicsData: [],
                rackId: ''
            }).result;
            encoded.length.should.be.above(0);
            // apiVersion should be 14 (bytes at offset 6-7 in the request header)
            encoded.readInt16BE(2).should.equal(14);
        });
    });

    describe('Fetch v15 (KIP-903 Broker Epoch)', function () {
        it('should encode FetchRequestV15 (no replicaId field)', function () {
            var v14Encoded, v15Encoded;

            v14Encoded = protocol.write().FetchRequestV14({
                correlationId: 61,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 65536,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [{
                    topicId: '00000000-0000-0000-0000-000000000001',
                    partitions: [{
                        partition: 0,
                        currentLeaderEpoch: -1,
                        offset: 0,
                        lastFetchedEpoch: -1,
                        logStartOffset: -1,
                        maxBytes: 65536
                    }]
                }],
                forgottenTopicsData: [],
                rackId: ''
            }).result;

            v15Encoded = protocol.write().FetchRequestV15({
                correlationId: 61,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 65536,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [{
                    topicId: '00000000-0000-0000-0000-000000000001',
                    partitions: [{
                        partition: 0,
                        currentLeaderEpoch: -1,
                        offset: 0,
                        lastFetchedEpoch: -1,
                        logStartOffset: -1,
                        maxBytes: 65536
                    }]
                }],
                forgottenTopicsData: [],
                rackId: ''
            }).result;

            v15Encoded.length.should.be.above(0);
            // apiVersion should be 15
            v15Encoded.readInt16BE(2).should.equal(15);
            // v15 should be 4 bytes shorter than v14 (missing replicaId Int32)
            v15Encoded.length.should.equal(v14Encoded.length - 4);
        });
    });

    describe('Fetch v16 (KIP-951 Leader Discovery)', function () {
        it('should encode FetchRequestV16 (same body as v15 but apiVersion=16)', function () {
            var v15Encoded, v16Encoded;

            v15Encoded = protocol.write().FetchRequestV15({
                correlationId: 62,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 65536,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [{
                    topicId: '00000000-0000-0000-0000-000000000001',
                    partitions: [{
                        partition: 0,
                        currentLeaderEpoch: -1,
                        offset: 0,
                        lastFetchedEpoch: -1,
                        logStartOffset: -1,
                        maxBytes: 65536
                    }]
                }],
                forgottenTopicsData: [],
                rackId: ''
            }).result;

            v16Encoded = protocol.write().FetchRequestV16({
                correlationId: 62,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 65536,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [{
                    topicId: '00000000-0000-0000-0000-000000000001',
                    partitions: [{
                        partition: 0,
                        currentLeaderEpoch: -1,
                        offset: 0,
                        lastFetchedEpoch: -1,
                        logStartOffset: -1,
                        maxBytes: 65536
                    }]
                }],
                forgottenTopicsData: [],
                rackId: ''
            }).result;

            v16Encoded.length.should.be.above(0);
            // apiVersion should be 16
            v16Encoded.readInt16BE(2).should.equal(16);
            // v16 and v15 should have the same body length (identical wire format)
            v16Encoded.length.should.equal(v15Encoded.length);
        });

        it('should decode FetchResponseV16 with NodeEndpoints tagged field (tag 0)', function () {
            var writer, result, nw, nodeData;

            // Build NodeEndpoints data first
            nw = protocol.write();
            // compactArray with 1 item: UVarint(2) = 1+1
            nw.UVarint(2);
            // item 1: broker 7
            nw.Int32BE(7);
            nw.compactString('broker7.example.com');
            nw.Int32BE(9092);
            nw.compactNullableString('rack-b');
            nw.TaggedFields();
            nodeData = Buffer.from(nw.result);

            writer = protocol.write();
            // correlationId
            writer.Int32BE(63);
            // response header TaggedFields (flexible)
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(50);
            // errorCode
            writer.Int16BE(0);
            // sessionId
            writer.Int32BE(12345);
            // topics: 1 topic with topicId
            writer.compactArray([1], function () {
                this.uuid('00000000-0000-0000-0000-000000000001');
                this.compactArray([1], function () {
                    this.Int32BE(0);   // partition
                    this.Int16BE(6);   // error code 6 = NOT_LEADER_OR_FOLLOWER
                    this.Int64BE(200); // highwaterMarkOffset
                    this.Int64BE(180); // lastStableOffset
                    this.Int64BE(0);   // logStartOffset
                    // abortedTransactions: empty
                    this.compactArray([], function () {});
                    // preferredReadReplica
                    this.Int32BE(-1);
                    // records: null (UVarint 0)
                    this.UVarint(0);
                    // Tagged fields: tag 1 = CurrentLeader
                    this.UVarint(1);    // 1 tagged field
                    this.UVarint(1);    // tag id = 1
                    this.UVarint(9);    // size = 4 + 4 + 1
                    this.Int32BE(7);    // leaderId = broker 7
                    this.Int32BE(55);   // leaderEpoch = 55
                    this.UVarint(0);    // CurrentLeader's own TaggedFields
                });
                this.TaggedFields();
            });

            // Body-level tagged fields: tag 0 = NodeEndpoints
            writer.UVarint(1);                // 1 tagged field
            writer.UVarint(0);                // tag id = 0
            writer.UVarint(nodeData.length);  // size
            writer.raw(nodeData);

            result = protocol.read(writer.result).FetchResponseV16().result;
            result.correlationId.should.equal(63);
            result.throttleTime.should.equal(50);
            result.sessionId.should.equal(12345);
            // CurrentLeader from partition
            result.topics[0].partitions[0].partition.should.equal(0);
            result.topics[0].partitions[0].currentLeader.leaderId.should.equal(7);
            result.topics[0].partitions[0].currentLeader.leaderEpoch.should.equal(55);
            // NodeEndpoints
            result.nodeEndpoints.length.should.equal(1);
            result.nodeEndpoints[0].nodeId.should.equal(7);
            result.nodeEndpoints[0].host.should.equal('broker7.example.com');
            result.nodeEndpoints[0].port.should.equal(9092);
            result.nodeEndpoints[0].rack.should.equal('rack-b');
        });

        it('should decode FetchResponseV16 without NodeEndpoints (graceful fallback)', function () {
            var writer, result;

            writer = protocol.write();
            // correlationId
            writer.Int32BE(64);
            // response header TaggedFields (flexible)
            writer.TaggedFields();
            // throttleTime
            writer.Int32BE(0);
            // errorCode
            writer.Int16BE(0);
            // sessionId
            writer.Int32BE(0);
            // topics: 1 topic, no error, no partition tagged fields
            writer.compactArray([1], function () {
                this.uuid('00000000-0000-0000-0000-000000000002');
                this.compactArray([1], function () {
                    this.Int32BE(0);
                    this.Int16BE(0);
                    this.Int64BE(100);
                    this.Int64BE(90);
                    this.Int64BE(0);
                    this.compactArray([], function () {});
                    this.Int32BE(-1);
                    this.UVarint(0);
                    // No tagged fields
                    this.TaggedFields();
                });
                this.TaggedFields();
            });
            // body TaggedFields (empty — no NodeEndpoints)
            writer.TaggedFields();

            result = protocol.read(writer.result).FetchResponseV16().result;
            result.correlationId.should.equal(64);
            result.topics[0].partitions[0].partition.should.equal(0);
            // nodeEndpoints should not be present when no body tagged fields
            (result.nodeEndpoints === undefined).should.equal(true);
            // currentLeader should not be present when no partition tagged fields
            (result.topics[0].partitions[0].currentLeader === undefined).should.equal(true);
        });
    });
});
