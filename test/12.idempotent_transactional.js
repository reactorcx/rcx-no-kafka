'use strict';

/* global describe, it, before, should */

var Protocol = require('../lib/protocol');
var errors   = require('../lib/errors');
var globals  = require('../lib/protocol/globals');
var Kafka    = require('../lib/index');

describe('Idempotent & Transactional Producer - Unit Tests', function () {
    var protocol;

    before(function () {
        protocol = new Protocol({ bufferSize: 256 * 1024 });
    });

    //////////////////////////
    // RecordBatch Bug Fix  //
    //////////////////////////

    describe('RecordBatch producerId=0 bug fix', function () {
        it('should write producerId=0 as 0 (not -1)', function () {
            var buf = protocol.write().RecordBatch({
                baseOffset: 0,
                producerId: 0,
                producerEpoch: 0,
                baseSequence: 0,
                records: [{ key: null, value: Buffer.from('hello'), headers: [] }],
                codec: 0
            }).result;

            // offset 43: 8(baseOffset)+4(batchLen)+4(partLeaderEpoch)+1(magic)+4(crc)+2(attrs)+4(lastOffsetDelta)+8(firstTs)+8(maxTs)
            var pidHi = buf.readInt32BE(43);
            var pidLo = buf.readUInt32BE(47);
            pidHi.should.equal(0);
            pidLo.should.equal(0);
        });

        it('should write producerEpoch=0 as 0 (not -1)', function () {
            var buf = protocol.write().RecordBatch({
                baseOffset: 0,
                producerId: 0,
                producerEpoch: 0,
                baseSequence: 0,
                records: [{ key: null, value: Buffer.from('hello'), headers: [] }],
                codec: 0
            }).result;

            buf.readInt16BE(51).should.equal(0);
        });

        it('should write baseSequence=0 as 0 (not -1)', function () {
            var buf = protocol.write().RecordBatch({
                baseOffset: 0,
                producerId: 0,
                producerEpoch: 0,
                baseSequence: 0,
                records: [{ key: null, value: Buffer.from('hello'), headers: [] }],
                codec: 0
            }).result;

            buf.readInt32BE(53).should.equal(0);
        });

        it('should still write -1 when producerId is undefined', function () {
            var buf = protocol.write().RecordBatch({
                baseOffset: 0,
                records: [{ key: null, value: Buffer.from('hello'), headers: [] }],
                codec: 0
            }).result;

            buf.readInt32BE(43).should.equal(-1); // hi word of -1 as Int64BE
            buf.readInt16BE(51).should.equal(-1);
            buf.readInt32BE(53).should.equal(-1);
        });

        it('should still write -1 when producerId is null', function () {
            var buf = protocol.write().RecordBatch({
                baseOffset: 0,
                producerId: null,
                producerEpoch: null,
                baseSequence: null,
                records: [{ key: null, value: Buffer.from('hello'), headers: [] }],
                codec: 0
            }).result;

            buf.readInt32BE(43).should.equal(-1);
            buf.readInt16BE(51).should.equal(-1);
            buf.readInt32BE(53).should.equal(-1);
        });

        it('should write a non-zero producerId correctly', function () {
            var buf = protocol.write().RecordBatch({
                baseOffset: 0,
                producerId: 12345,
                producerEpoch: 5,
                baseSequence: 42,
                records: [{ key: null, value: Buffer.from('hello'), headers: [] }],
                codec: 0
            }).result;

            // producerId=12345: hi word=0, lo word=12345
            buf.readInt32BE(43).should.equal(0);
            buf.readUInt32BE(47).should.equal(12345);
            buf.readInt16BE(51).should.equal(5);
            buf.readInt32BE(53).should.equal(42);
        });
    });

    //////////////////////////
    // Error Codes          //
    //////////////////////////

    describe('Error Codes', function () {
        it('should resolve error code 53 (TransactionalIdAuthorizationFailed)', function () {
            var err = errors.byCode(53);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('TransactionalIdAuthorizationFailed');
        });

        it('should resolve error code 67 (ProducerFenced)', function () {
            var err = errors.byCode(67);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('ProducerFenced');
        });

        it('should resolve error code 45 (OutOfOrderSequenceNumber)', function () {
            var err = errors.byCode(45);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('OutOfOrderSequenceNumber');
        });

        it('should resolve error code 46 (DuplicateSequenceNumber)', function () {
            var err = errors.byCode(46);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('DuplicateSequenceNumber');
        });

        it('should resolve error code 48 (InvalidTxnState)', function () {
            var err = errors.byCode(48);
            err.should.be.an.instanceOf(errors.KafkaError);
            err.code.should.equal('InvalidTxnState');
        });
    });

    //////////////////////////
    // API Keys             //
    //////////////////////////

    describe('API Keys', function () {
        it('should have InitProducerIdRequest key = 22', function () {
            globals.API_KEYS.InitProducerIdRequest.should.equal(22);
        });

        it('should have AddPartitionsToTxnRequest key = 24', function () {
            globals.API_KEYS.AddPartitionsToTxnRequest.should.equal(24);
        });

        it('should have AddOffsetsToTxnRequest key = 25', function () {
            globals.API_KEYS.AddOffsetsToTxnRequest.should.equal(25);
        });

        it('should have EndTxnRequest key = 26', function () {
            globals.API_KEYS.EndTxnRequest.should.equal(26);
        });

        it('should have TxnOffsetCommitRequest key = 28', function () {
            globals.API_KEYS.TxnOffsetCommitRequest.should.equal(28);
        });
    });

    //////////////////////////
    // InitProducerId       //
    //////////////////////////

    describe('InitProducerIdRequest', function () {
        it('should serialize request with null transactionalId', function () {
            var buf = protocol.write().InitProducerIdRequest({
                correlationId: 1,
                clientId: 'test',
                transactionalId: null,
                transactionTimeoutMs: 30000
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(0).should.equal(22); // apiKey
            buf.readInt16BE(2).should.equal(0);  // apiVersion
            buf.readInt32BE(4).should.equal(1);  // correlationId
        });

        it('should serialize request with transactionalId', function () {
            var buf = protocol.write().InitProducerIdRequest({
                correlationId: 2,
                clientId: 'test',
                transactionalId: 'my-txn-id',
                transactionTimeoutMs: 60000
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(0).should.equal(22);
            buf.length.should.be.above(20);
        });
    });

    describe('InitProducerIdResponse', function () {
        it('should deserialize a success response', function () {
            // correlationId(4) + throttleTime(4) + errorCode(2) + producerId(8) + producerEpoch(2)
            var buf = Buffer.alloc(20);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;   // correlationId
            buf.writeInt32BE(0, offset); offset += 4;   // throttleTime
            buf.writeInt16BE(0, offset); offset += 2;   // errorCode (0 = no error)
            buf.writeInt32BE(0, offset); offset += 4;   // producerId hi
            buf.writeInt32BE(12345, offset); offset += 4; // producerId lo
            buf.writeInt16BE(0, offset); offset += 2;   // producerEpoch

            result = protocol.read(buf).InitProducerIdResponse().result;
            result.correlationId.should.equal(1);
            result.throttleTime.should.equal(0);
            should.equal(result.error, null);
            result.producerEpoch.should.equal(0);
        });

        it('should deserialize an error response', function () {
            var buf = Buffer.alloc(20);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;   // correlationId
            buf.writeInt32BE(0, offset); offset += 4;   // throttleTime
            buf.writeInt16BE(53, offset); offset += 2;  // errorCode = TransactionalIdAuthorizationFailed
            buf.writeInt32BE(0, offset); offset += 4;   // producerId hi
            buf.writeInt32BE(-1, offset); offset += 4;  // producerId lo
            buf.writeInt16BE(-1, offset); offset += 2;  // producerEpoch

            result = protocol.read(buf).InitProducerIdResponse().result;
            result.error.should.be.an.instanceOf(errors.KafkaError);
            result.error.code.should.equal('TransactionalIdAuthorizationFailed');
        });
    });

    //////////////////////////
    // FindCoordinator v1   //
    //////////////////////////

    describe('FindCoordinatorRequest v1', function () {
        it('should serialize with coordinatorType=1 for transactions', function () {
            var buf = protocol.write().FindCoordinatorRequest({
                correlationId: 1,
                clientId: 'test',
                key: 'my-txn-id',
                coordinatorType: 1
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(0).should.equal(10); // apiKey (GroupCoordinator)
            buf.readInt16BE(2).should.equal(1);  // apiVersion
        });

        it('should serialize with coordinatorType=0 for groups', function () {
            var buf = protocol.write().FindCoordinatorRequest({
                correlationId: 1,
                clientId: 'test',
                key: 'my-group',
                coordinatorType: 0
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(2).should.equal(1); // apiVersion
        });
    });

    describe('FindCoordinatorResponse v1', function () {
        it('should deserialize a success response', function () {
            // correlationId(4) + throttleTime(4) + errorCode(2) + errorMessage(2 for null) +
            // coordinatorId(4) + coordinatorHost(2+9) + coordinatorPort(4)
            var hostBuf = Buffer.from('localhost', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 2 + 2 + 4 + 2 + hostBuf.length + 4);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;   // correlationId
            buf.writeInt32BE(0, offset); offset += 4;   // throttleTime
            buf.writeInt16BE(0, offset); offset += 2;   // errorCode
            buf.writeInt16BE(-1, offset); offset += 2;  // errorMessage (null)
            buf.writeInt32BE(0, offset); offset += 4;   // coordinatorId
            buf.writeInt16BE(hostBuf.length, offset); offset += 2;
            hostBuf.copy(buf, offset); offset += hostBuf.length;
            buf.writeInt32BE(9092, offset); offset += 4; // coordinatorPort

            result = protocol.read(buf).FindCoordinatorResponse().result;
            result.correlationId.should.equal(1);
            should.equal(result.error, null);
            result.coordinatorId.should.equal(0);
            result.coordinatorHost.should.equal('localhost');
            result.coordinatorPort.should.equal(9092);
        });
    });

    //////////////////////////
    // Transaction Protocols //
    //////////////////////////

    describe('AddPartitionsToTxnRequest', function () {
        it('should serialize request with topics and partitions', function () {
            var buf = protocol.write().AddPartitionsToTxnRequest({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'my-txn',
                producerId: 12345,
                producerEpoch: 0,
                topics: [
                    { topic: 'topic-a', partitions: [0, 1, 2] },
                    { topic: 'topic-b', partitions: [0] }
                ]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(0).should.equal(24); // apiKey
            buf.readInt16BE(2).should.equal(0);  // apiVersion
        });
    });

    describe('AddPartitionsToTxnResponse', function () {
        it('should deserialize a success response', function () {
            // Build: correlationId(4) + throttleTime(4) + results array
            var topicBuf = Buffer.from('topic-a', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 4 + 2 + topicBuf.length + 4 + 4 + 2);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;   // correlationId
            buf.writeInt32BE(0, offset); offset += 4;   // throttleTime
            buf.writeInt32BE(1, offset); offset += 4;   // results array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4;   // partitions array length
            buf.writeInt32BE(0, offset); offset += 4;   // partition
            buf.writeInt16BE(0, offset); offset += 2;   // errorCode (no error)

            result = protocol.read(buf).AddPartitionsToTxnResponse().result;
            result.correlationId.should.equal(1);
            result.results.should.be.an('array').with.length(1);
            result.results[0].topic.should.equal('topic-a');
            result.results[0].partitions.should.be.an('array').with.length(1);
            result.results[0].partitions[0].partition.should.equal(0);
            should.equal(result.results[0].partitions[0].error, null);
        });
    });

    describe('AddOffsetsToTxnRequest', function () {
        it('should serialize request', function () {
            var buf = protocol.write().AddOffsetsToTxnRequest({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'my-txn',
                producerId: 12345,
                producerEpoch: 0,
                groupId: 'my-group'
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(0).should.equal(25); // apiKey
        });
    });

    describe('AddOffsetsToTxnResponse', function () {
        it('should deserialize a success response', function () {
            var buf = Buffer.alloc(10);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;   // correlationId
            buf.writeInt32BE(0, offset); offset += 4;   // throttleTime
            buf.writeInt16BE(0, offset); offset += 2;   // errorCode

            result = protocol.read(buf).AddOffsetsToTxnResponse().result;
            result.correlationId.should.equal(1);
            should.equal(result.error, null);
        });

        it('should deserialize an error response', function () {
            var buf = Buffer.alloc(10);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt16BE(48, offset); offset += 2; // InvalidTxnState

            result = protocol.read(buf).AddOffsetsToTxnResponse().result;
            result.error.should.be.an.instanceOf(errors.KafkaError);
            result.error.code.should.equal('InvalidTxnState');
        });
    });

    describe('EndTxnRequest', function () {
        it('should serialize commit request', function () {
            var buf = protocol.write().EndTxnRequest({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'my-txn',
                producerId: 12345,
                producerEpoch: 0,
                committed: true
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(0).should.equal(26); // apiKey
        });

        it('should serialize abort request', function () {
            var buf = protocol.write().EndTxnRequest({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'my-txn',
                producerId: 12345,
                producerEpoch: 0,
                committed: false
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(0).should.equal(26);
        });
    });

    describe('EndTxnResponse', function () {
        it('should deserialize a success response', function () {
            var buf = Buffer.alloc(10);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt16BE(0, offset); offset += 2;

            result = protocol.read(buf).EndTxnResponse().result;
            result.correlationId.should.equal(1);
            should.equal(result.error, null);
        });

        it('should deserialize a ProducerFenced error', function () {
            var buf = Buffer.alloc(10);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;
            buf.writeInt32BE(0, offset); offset += 4;
            buf.writeInt16BE(67, offset); offset += 2; // ProducerFenced

            result = protocol.read(buf).EndTxnResponse().result;
            result.error.should.be.an.instanceOf(errors.KafkaError);
            result.error.code.should.equal('ProducerFenced');
        });
    });

    describe('TxnOffsetCommitRequest', function () {
        it('should serialize request with offsets', function () {
            var buf = protocol.write().TxnOffsetCommitRequest({
                correlationId: 1,
                clientId: 'test',
                transactionalId: 'my-txn',
                groupId: 'my-group',
                producerId: 12345,
                producerEpoch: 0,
                topics: [{
                    topic: 'topic-a',
                    partitions: [
                        { partition: 0, offset: 100, metadata: null },
                        { partition: 1, offset: 200, metadata: 'meta' }
                    ]
                }]
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            buf.readInt16BE(0).should.equal(28); // apiKey
        });
    });

    describe('TxnOffsetCommitResponse', function () {
        it('should deserialize a success response', function () {
            var topicBuf = Buffer.from('topic-a', 'utf8');
            var buf = Buffer.alloc(4 + 4 + 4 + 2 + topicBuf.length + 4 + 4 + 2);
            var offset = 0;
            var result;

            buf.writeInt32BE(1, offset); offset += 4;   // correlationId
            buf.writeInt32BE(0, offset); offset += 4;   // throttleTime
            buf.writeInt32BE(1, offset); offset += 4;   // results array length
            buf.writeInt16BE(topicBuf.length, offset); offset += 2;
            topicBuf.copy(buf, offset); offset += topicBuf.length;
            buf.writeInt32BE(1, offset); offset += 4;   // partitions array length
            buf.writeInt32BE(0, offset); offset += 4;   // partition
            buf.writeInt16BE(0, offset); offset += 2;   // errorCode

            result = protocol.read(buf).TxnOffsetCommitResponse().result;
            result.correlationId.should.equal(1);
            result.results.should.be.an('array').with.length(1);
            result.results[0].topic.should.equal('topic-a');
            result.results[0].partitions[0].partition.should.equal(0);
            should.equal(result.results[0].partitions[0].error, null);
        });
    });

    //////////////////////////
    // Producer Constructor //
    //////////////////////////

    describe('Producer constructor', function () {
        it('should default idempotent to false', function () {
            var p = new Kafka.Producer();
            p.options.idempotent.should.equal(false);
            p.options.requiredAcks.should.equal(1);
        });

        it('should force requiredAcks=-1 when idempotent=true', function () {
            var p = new Kafka.Producer({ idempotent: true });
            p.options.idempotent.should.equal(true);
            p.options.requiredAcks.should.equal(-1);
        });

        it('should force idempotent=true when transactionalId is set', function () {
            var p = new Kafka.Producer({ transactionalId: 'my-txn' });
            p.options.idempotent.should.equal(true);
            p.options.requiredAcks.should.equal(-1);
            p.options.transactionalId.should.equal('my-txn');
        });

        it('should not override requiredAcks=-1 even if user passes acks=1 with idempotent', function () {
            var p = new Kafka.Producer({ idempotent: true, requiredAcks: 1 });
            p.options.requiredAcks.should.equal(-1);
        });

        it('should initialize sequence tracking', function () {
            var p = new Kafka.Producer({ idempotent: true });
            p.sequenceNumbers.should.be.an('object');
            p._inTransaction.should.equal(false);
            p._txnPartitions.should.be.an('object');
        });
    });

    //////////////////////////
    // Transaction State    //
    //////////////////////////

    describe('Producer transaction state', function () {
        it('beginTransaction should throw without transactionalId', function () {
            var p = new Kafka.Producer();
            (function () { p.beginTransaction(); }).should.throw('Cannot begin transaction without transactionalId');
        });

        it('beginTransaction should set _inTransaction=true', function () {
            var p = new Kafka.Producer({ transactionalId: 'txn-1' });
            p._inTransaction.should.equal(false);
            p.beginTransaction();
            p._inTransaction.should.equal(true);
        });

        it('beginTransaction should throw if already in transaction', function () {
            var p = new Kafka.Producer({ transactionalId: 'txn-1' });
            p.beginTransaction();
            (function () { p.beginTransaction(); }).should.throw('Transaction already in progress');
        });

        it('beginTransaction should reset _txnPartitions', function () {
            var p = new Kafka.Producer({ transactionalId: 'txn-1' });
            p._txnPartitions['topic:0'] = true;
            p.beginTransaction();
            p._txnPartitions.should.deep.equal({});
        });

        it('commitTransaction should reject without transactionalId', function () {
            var p = new Kafka.Producer();
            return p.commitTransaction().should.be.rejectedWith('Cannot commit transaction without transactionalId');
        });

        it('commitTransaction should reject if not in transaction', function () {
            var p = new Kafka.Producer({ transactionalId: 'txn-1' });
            return p.commitTransaction().should.be.rejectedWith('No transaction in progress');
        });

        it('abortTransaction should reject without transactionalId', function () {
            var p = new Kafka.Producer();
            return p.abortTransaction().should.be.rejectedWith('Cannot abort transaction without transactionalId');
        });

        it('abortTransaction should reject if not in transaction', function () {
            var p = new Kafka.Producer({ transactionalId: 'txn-1' });
            return p.abortTransaction().should.be.rejectedWith('No transaction in progress');
        });

        it('sendOffsets should reject without transactionalId', function () {
            var p = new Kafka.Producer();
            return p.sendOffsets([], 'group').should.be.rejectedWith('Cannot send offsets without transactionalId');
        });

        it('sendOffsets should reject if not in transaction', function () {
            var p = new Kafka.Producer({ transactionalId: 'txn-1' });
            return p.sendOffsets([], 'group').should.be.rejectedWith('No transaction in progress');
        });
    });

    //////////////////////////
    // Client fields        //
    //////////////////////////

    describe('Client idempotent fields', function () {
        var Client = require('../lib/client');

        it('should initialize producerId to -1', function () {
            var client = new Client();
            client.producerId.should.equal(-1);
        });

        it('should initialize producerEpoch to -1', function () {
            var client = new Client();
            client.producerEpoch.should.equal(-1);
        });

        it('should initialize transactionCoordinators as empty object', function () {
            var client = new Client();
            client.transactionCoordinators.should.be.an('object');
            Object.keys(client.transactionCoordinators).should.have.length(0);
        });
    });

    //////////////////////////
    // Consumer Isolation   //
    //////////////////////////

    describe('Consumer isolationLevel', function () {
        it('should default isolationLevel to 0', function () {
            var consumer = new Kafka.SimpleConsumer();
            consumer.options.isolationLevel.should.equal(0);
        });

        it('should accept isolationLevel=1', function () {
            var consumer = new Kafka.SimpleConsumer({ isolationLevel: 1 });
            consumer.options.isolationLevel.should.equal(1);
        });

        it('should default isolationLevel to 0 for GroupConsumer', function () {
            var consumer = new Kafka.GroupConsumer();
            consumer.options.isolationLevel.should.equal(0);
        });

        it('FetchRequestV4 should include isolationLevel=1 when set', function () {
            var buf = protocol.write().FetchRequestV4({
                correlationId: 1,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 1048576,
                isolationLevel: 1,
                topics: []
            }).result;

            buf.should.be.an.instanceOf(Buffer);
            // isolationLevel byte is at:
            // apiKey(2)+apiVersion(2)+correlationId(4)+clientId(2+4='test')+replicaId(4)+maxWaitTime(4)+minBytes(4)+maxBytes(4)
            // = 2+2+4+2+4+4+4+4+4 = 30
            buf.readInt8(30).should.equal(1);
        });

        it('FetchRequestV4 should default isolationLevel to 0', function () {
            var buf = protocol.write().FetchRequestV4({
                correlationId: 1,
                clientId: 'test',
                maxWaitTime: 100,
                minBytes: 1,
                maxBytes: 1048576,
                isolationLevel: 0,
                topics: []
            }).result;

            buf.readInt8(30).should.equal(0);
        });
    });
});
