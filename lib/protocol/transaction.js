'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

///////////////////////
// TRANSACTION APIs  //
///////////////////////

// AddPartitionsToTxn (apiKey=24)

Protocol.define('AddPartitionsToTxnRequest_TopicItem', {
    write: function (data) { // { topic, partitions }
        this
            .string(data.topic)
            .array(data.partitions, this.Int32BE);
    }
});

Protocol.define('AddPartitionsToTxnRequest', {
    write: function (data) { // { transactionalId, producerId, producerEpoch, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.AddPartitionsToTxnRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .array(data.topics, this.AddPartitionsToTxnRequest_TopicItem);
    }
});

Protocol.define('AddPartitionsToTxnResponse_PartitionResult', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error');
    }
});

Protocol.define('AddPartitionsToTxnResponse_TopicResult', {
    read: function () {
        this
            .string('topic')
            .array('partitions', this.AddPartitionsToTxnResponse_PartitionResult);
    }
});

Protocol.define('AddPartitionsToTxnResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('results', this.AddPartitionsToTxnResponse_TopicResult);
    }
});

// AddOffsetsToTxn (apiKey=25)

Protocol.define('AddOffsetsToTxnRequest', {
    write: function (data) { // { transactionalId, producerId, producerEpoch, groupId }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.AddOffsetsToTxnRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .string(data.groupId);
    }
});

Protocol.define('AddOffsetsToTxnResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error');
    }
});

// EndTxn (apiKey=26)

Protocol.define('EndTxnRequest', {
    write: function (data) { // { transactionalId, producerId, producerEpoch, committed }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.EndTxnRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .Int8(data.committed ? 1 : 0);
    }
});

Protocol.define('EndTxnResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error');
    }
});

// TxnOffsetCommit (apiKey=28)

Protocol.define('TxnOffsetCommitRequest_PartitionItem', {
    write: function (data) { // { partition, offset, metadata }
        this
            .Int32BE(data.partition)
            .Int64BE(data.offset)
            .string(data.metadata || null);
    }
});

Protocol.define('TxnOffsetCommitRequest_TopicItem', {
    write: function (data) { // { topic, partitions }
        this
            .string(data.topic)
            .array(data.partitions, this.TxnOffsetCommitRequest_PartitionItem);
    }
});

Protocol.define('TxnOffsetCommitRequest', {
    write: function (data) { // { transactionalId, groupId, producerId, producerEpoch, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.TxnOffsetCommitRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId)
            .string(data.groupId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .array(data.topics, this.TxnOffsetCommitRequest_TopicItem);
    }
});

Protocol.define('TxnOffsetCommitResponse_PartitionResult', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error');
    }
});

Protocol.define('TxnOffsetCommitResponse_TopicResult', {
    read: function () {
        this
            .string('topic')
            .array('partitions', this.TxnOffsetCommitResponse_PartitionResult);
    }
});

Protocol.define('TxnOffsetCommitResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('results', this.TxnOffsetCommitResponse_TopicResult);
    }
});
