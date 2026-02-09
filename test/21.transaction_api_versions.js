'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');

describe('Transaction API Version Bumps (v1-v3)', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    /////////////////////////////////////
    // v0 defs with apiVersion passed  //
    /////////////////////////////////////

    describe('v0 definitions with apiVersion override', function () {
        it('AddPartitionsToTxnRequest should accept apiVersion: 2', function () {
            var encoded = protocol.write().AddPartitionsToTxnRequest({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 2,
                transactionalId: 'txn1',
                producerId: 1,
                producerEpoch: 0,
                topics: [{ topic: 't1', partitions: [0] }]
            }).result;

            // apiKey=24 (Int16BE), apiVersion=2 (Int16BE)
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(24);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(2);
        });

        it('AddOffsetsToTxnRequest should accept apiVersion: 2', function () {
            var encoded = protocol.write().AddOffsetsToTxnRequest({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 2,
                transactionalId: 'txn1',
                producerId: 1,
                producerEpoch: 0,
                groupId: 'g1'
            }).result;

            encoded[0].should.equal(0x00);
            encoded[1].should.equal(25);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(2);
        });

        it('EndTxnRequest should accept apiVersion: 2', function () {
            var encoded = protocol.write().EndTxnRequest({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 2,
                transactionalId: 'txn1',
                producerId: 1,
                producerEpoch: 0,
                committed: true
            }).result;

            encoded[0].should.equal(0x00);
            encoded[1].should.equal(26);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(2);
        });

        it('TxnOffsetCommitRequest should accept apiVersion: 1', function () {
            var encoded = protocol.write().TxnOffsetCommitRequest({
                correlationId: 1,
                clientId: 'test',
                apiVersion: 1,
                transactionalId: 'txn1',
                groupId: 'g1',
                producerId: 1,
                producerEpoch: 0,
                topics: [{ topic: 't1', partitions: [{ partition: 0, offset: 100, metadata: null }] }]
            }).result;

            encoded[0].should.equal(0x00);
            encoded[1].should.equal(28);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(1);
        });
    });

    ////////////////////////////
    // TxnOffsetCommit v2     //
    ////////////////////////////

    describe('TxnOffsetCommitRequestV2', function () {
        it('should encode committedLeaderEpoch per partition', function () {
            var encoded = protocol.write().TxnOffsetCommitRequestV2({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'txn1',
                groupId: 'g1',
                producerId: 1,
                producerEpoch: 0,
                topics: [{
                    topic: 't1',
                    partitions: [{ partition: 0, offset: 100, committedLeaderEpoch: 5, metadata: null }]
                }]
            }).result;

            // apiKey=28, apiVersion=2
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(28);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(2);
        });

        it('should default committedLeaderEpoch to -1', function () {
            var encoded = protocol.write().TxnOffsetCommitRequestV2({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'txn1',
                groupId: 'g1',
                producerId: 1,
                producerEpoch: 0,
                topics: [{
                    topic: 't1',
                    partitions: [{ partition: 0, offset: 100, metadata: null }]
                }]
            }).result;

            // Should encode without error (committedLeaderEpoch defaults to -1)
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(28);
        });
    });

    ///////////////////////////////////////
    // AddPartitionsToTxn v3 (flexible)  //
    ///////////////////////////////////////

    describe('AddPartitionsToTxnRequestV3', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().AddPartitionsToTxnRequestV3({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'txn1',
                producerId: 1,
                producerEpoch: 0,
                topics: [{ topic: 't1', partitions: [0] }]
            }).result;

            // apiKey=24, apiVersion=3
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(24);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(3);
        });
    });

    describe('AddPartitionsToTxnResponseV3', function () {
        it('should parse flexible response', function () {
            var buf = Buffer.concat([
                new Buffer([0x00, 0x00, 0x00, 0x01]),  // correlationId = 1
                new Buffer([0x00]),                     // TaggedFields (empty)
                new Buffer([0x00, 0x00, 0x00, 0x00]),  // throttleTime = 0
                new Buffer([0x02]),                     // compactArray length = 2 (1 item)
                // topic result
                new Buffer([0x03]),                     // compactString len=2+1 = 3
                new Buffer('t1', 'utf8'),
                new Buffer([0x02]),                     // compactArray length = 2 (1 partition)
                // partition result
                new Buffer([0x00, 0x00, 0x00, 0x00]),  // partition = 0
                new Buffer([0x00, 0x00]),               // error = 0
                new Buffer([0x00]),                     // TaggedFields (partition)
                new Buffer([0x00]),                     // TaggedFields (topic)
                new Buffer([0x00])                      // TaggedFields (top-level)
            ]);
            var result = protocol.read(buf).AddPartitionsToTxnResponseV3().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
            result.results.length.should.equal(1);
            result.results[0].topic.should.equal('t1');
            result.results[0].partitions.length.should.equal(1);
            result.results[0].partitions[0].partition.should.equal(0);
        });
    });

    //////////////////////////////////
    // AddOffsetsToTxn v3 (flexible) //
    //////////////////////////////////

    describe('AddOffsetsToTxnRequestV3', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().AddOffsetsToTxnRequestV3({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'txn1',
                producerId: 1,
                producerEpoch: 0,
                groupId: 'g1'
            }).result;

            // apiKey=25, apiVersion=3
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(25);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(3);
        });
    });

    describe('AddOffsetsToTxnResponseV3', function () {
        it('should parse flexible response', function () {
            var buf = Buffer.concat([
                new Buffer([0x00, 0x00, 0x00, 0x01]),  // correlationId = 1
                new Buffer([0x00]),                     // TaggedFields (empty)
                new Buffer([0x00, 0x00, 0x00, 0x00]),  // throttleTime = 0
                new Buffer([0x00, 0x00]),               // error = 0
                new Buffer([0x00])                      // TaggedFields (empty)
            ]);
            var result = protocol.read(buf).AddOffsetsToTxnResponseV3().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
        });
    });

    ///////////////////////////
    // EndTxn v3 (flexible)  //
    ///////////////////////////

    describe('EndTxnRequestV3', function () {
        it('should use FlexibleRequestHeader and compact types', function () {
            var encoded = protocol.write().EndTxnRequestV3({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'txn1',
                producerId: 1,
                producerEpoch: 0,
                committed: true
            }).result;

            // apiKey=26, apiVersion=3
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(26);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(3);
        });
    });

    describe('EndTxnResponseV3', function () {
        it('should parse flexible response', function () {
            var buf = Buffer.concat([
                new Buffer([0x00, 0x00, 0x00, 0x01]),  // correlationId = 1
                new Buffer([0x00]),                     // TaggedFields (empty)
                new Buffer([0x00, 0x00, 0x00, 0x00]),  // throttleTime = 0
                new Buffer([0x00, 0x00]),               // error = 0
                new Buffer([0x00])                      // TaggedFields (empty)
            ]);
            var result = protocol.read(buf).EndTxnResponseV3().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
        });
    });

    ////////////////////////////////////
    // TxnOffsetCommit v3 (flexible)  //
    ////////////////////////////////////

    describe('TxnOffsetCommitRequestV3', function () {
        it('should use FlexibleRequestHeader and include generationId/memberId/groupInstanceId', function () {
            var encoded = protocol.write().TxnOffsetCommitRequestV3({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'txn1',
                groupId: 'g1',
                producerId: 1,
                producerEpoch: 0,
                generationId: 5,
                memberId: 'member-1',
                groupInstanceId: 'instance-1',
                topics: [{
                    topic: 't1',
                    partitions: [{ partition: 0, offset: 100, committedLeaderEpoch: 3, metadata: null }]
                }]
            }).result;

            // apiKey=28, apiVersion=3
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(28);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(3);
        });

        it('should default generationId to -1 and memberId to empty', function () {
            var encoded = protocol.write().TxnOffsetCommitRequestV3({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'txn1',
                groupId: 'g1',
                producerId: 1,
                producerEpoch: 0,
                topics: [{
                    topic: 't1',
                    partitions: [{ partition: 0, offset: 100, metadata: null }]
                }]
            }).result;

            // Should encode without error
            encoded[0].should.equal(0x00);
            encoded[1].should.equal(28);
            encoded[2].should.equal(0x00);
            encoded[3].should.equal(3);
        });
    });

    describe('TxnOffsetCommitResponseV3', function () {
        it('should parse flexible response', function () {
            var buf = Buffer.concat([
                new Buffer([0x00, 0x00, 0x00, 0x01]),  // correlationId = 1
                new Buffer([0x00]),                     // TaggedFields (empty)
                new Buffer([0x00, 0x00, 0x00, 0x00]),  // throttleTime = 0
                new Buffer([0x02]),                     // compactArray length = 2 (1 item)
                // topic result
                new Buffer([0x03]),                     // compactString len=2+1 = 3
                new Buffer('t1', 'utf8'),
                new Buffer([0x02]),                     // compactArray length = 2 (1 partition)
                // partition result
                new Buffer([0x00, 0x00, 0x00, 0x00]),  // partition = 0
                new Buffer([0x00, 0x00]),               // error = 0
                new Buffer([0x00]),                     // TaggedFields (partition)
                new Buffer([0x00]),                     // TaggedFields (topic)
                new Buffer([0x00])                      // TaggedFields (top-level)
            ]);
            var result = protocol.read(buf).TxnOffsetCommitResponseV3().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
            result.results.length.should.equal(1);
            result.results[0].topic.should.equal('t1');
            result.results[0].partitions.length.should.equal(1);
            result.results[0].partitions[0].partition.should.equal(0);
        });
    });

    //////////////////////////////////
    // FLEXIBLE_VERSION_THRESHOLDS  //
    //////////////////////////////////

    describe('FLEXIBLE_VERSION_THRESHOLDS for transaction APIs', function () {
        it('should have correct thresholds', function () {
            var globals = require('../lib/protocol/globals');
            var thresholds = globals.FLEXIBLE_VERSION_THRESHOLDS;
            thresholds[24].should.equal(3);  // AddPartitionsToTxn
            thresholds[25].should.equal(3);  // AddOffsetsToTxn
            thresholds[26].should.equal(3);  // EndTxn
            thresholds[28].should.equal(3);  // TxnOffsetCommit
        });
    });
});
