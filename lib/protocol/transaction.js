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
                apiVersion: data.apiVersion || 0,
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
                apiVersion: data.apiVersion || 0,
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
                apiVersion: data.apiVersion || 0,
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
                apiVersion: data.apiVersion || 0,
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

// TxnOffsetCommit v2 — adds committedLeaderEpoch per partition

Protocol.define('TxnOffsetCommitRequestV2_PartitionItem', {
    write: function (data) { // { partition, offset, committedLeaderEpoch, metadata }
        this
            .Int32BE(data.partition)
            .Int64BE(data.offset)
            .Int32BE(data.committedLeaderEpoch !== undefined ? data.committedLeaderEpoch : -1)
            .string(data.metadata || null);
    }
});

Protocol.define('TxnOffsetCommitRequestV2_TopicItem', {
    write: function (data) { // { topic, partitions }
        this
            .string(data.topic)
            .array(data.partitions, this.TxnOffsetCommitRequestV2_PartitionItem);
    }
});

Protocol.define('TxnOffsetCommitRequestV2', {
    write: function (data) { // { transactionalId, groupId, producerId, producerEpoch, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.TxnOffsetCommitRequest,
                apiVersion: data.apiVersion || 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId)
            .string(data.groupId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .array(data.topics, this.TxnOffsetCommitRequestV2_TopicItem);
    }
});

// AddPartitionsToTxn v3 — first flexible version (KIP-482)

Protocol.define('AddPartitionsToTxnRequestV3_TopicItem', {
    write: function (data) { // { topic, partitions }
        this
            .compactString(data.topic)
            .compactArray(data.partitions, this.Int32BE)
            .TaggedFields();
    }
});

Protocol.define('AddPartitionsToTxnRequestV3', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.AddPartitionsToTxnRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.transactionalId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .compactArray(data.topics, this.AddPartitionsToTxnRequestV3_TopicItem)
            .TaggedFields();
    }
});

Protocol.define('AddPartitionsToTxnResponseV3_PartitionResult', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .TaggedFields();
    }
});

Protocol.define('AddPartitionsToTxnResponseV3_TopicResult', {
    read: function () {
        this
            .compactString('topic')
            .compactArray('partitions', this.AddPartitionsToTxnResponseV3_PartitionResult)
            .TaggedFields();
    }
});

Protocol.define('AddPartitionsToTxnResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('results', this.AddPartitionsToTxnResponseV3_TopicResult)
            .TaggedFields();
    }
});

// AddOffsetsToTxn v3 — first flexible version (KIP-482)

Protocol.define('AddOffsetsToTxnRequestV3', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.AddOffsetsToTxnRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.transactionalId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .compactString(data.groupId)
            .TaggedFields();
    }
});

Protocol.define('AddOffsetsToTxnResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .TaggedFields();
    }
});

// EndTxn v3 — first flexible version (KIP-482)

Protocol.define('EndTxnRequestV3', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.EndTxnRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.transactionalId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .Int8(data.committed ? 1 : 0)
            .TaggedFields();
    }
});

Protocol.define('EndTxnResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .TaggedFields();
    }
});

// EndTxn v5 response — adds ProducerId and ProducerEpoch (broker returns new epoch after each txn)
Protocol.define('EndTxnResponseV5', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .Int64BE('producerId')
            .Int16BE('producerEpoch')
            .TaggedFields();
    }
});

// EndTxn v4/v5 (KIP-890) — v4 adds TRANSACTION_ABORTABLE error, v5 bumps epoch per txn
Protocol.define('EndTxnRequestV4', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.EndTxnRequest,
                apiVersion: data.apiVersion || 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.transactionalId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .Int8(data.committed ? 1 : 0)
            .TaggedFields();
    }
});

// AddOffsetsToTxn v4 (KIP-890) — wire-identical to v3; adds TRANSACTION_ABORTABLE error
Protocol.define('AddOffsetsToTxnRequestV4', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.AddOffsetsToTxnRequest,
                apiVersion: 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.transactionalId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .compactString(data.groupId)
            .TaggedFields();
    }
});

// TxnOffsetCommit v4/v5 (KIP-890) — wire-identical to v3; v4 adds TRANSACTION_ABORTABLE, v5 incorporates AddOffsetsToTxn
Protocol.define('TxnOffsetCommitRequestV4', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.TxnOffsetCommitRequest,
                apiVersion: data.apiVersion || 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.transactionalId)
            .compactString(data.groupId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .Int32BE(data.generationId !== undefined ? data.generationId : -1)
            .compactString(data.memberId !== undefined ? data.memberId : '')
            .compactNullableString(data.groupInstanceId !== undefined ? data.groupInstanceId : null)
            .compactArray(data.topics, this.TxnOffsetCommitRequestV3_TopicItem)
            .TaggedFields();
    }
});

// TxnOffsetCommit v3 — first flexible version (KIP-482)
// Adds generationId, memberId, groupInstanceId to request; committedLeaderEpoch per partition (from v2)

Protocol.define('TxnOffsetCommitRequestV3_PartitionItem', {
    write: function (data) { // { partition, offset, committedLeaderEpoch, metadata }
        this
            .Int32BE(data.partition)
            .Int64BE(data.offset)
            .Int32BE(data.committedLeaderEpoch !== undefined ? data.committedLeaderEpoch : -1)
            .compactNullableString(data.metadata || null)
            .TaggedFields();
    }
});

Protocol.define('TxnOffsetCommitRequestV3_TopicItem', {
    write: function (data) { // { topic, partitions }
        this
            .compactString(data.topic)
            .compactArray(data.partitions, this.TxnOffsetCommitRequestV3_PartitionItem)
            .TaggedFields();
    }
});

Protocol.define('TxnOffsetCommitRequestV3', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.TxnOffsetCommitRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.transactionalId)
            .compactString(data.groupId)
            .Int64BE(data.producerId)
            .Int16BE(data.producerEpoch)
            .Int32BE(data.generationId !== undefined ? data.generationId : -1)
            .compactString(data.memberId !== undefined ? data.memberId : '')
            .compactNullableString(data.groupInstanceId !== undefined ? data.groupInstanceId : null)
            .compactArray(data.topics, this.TxnOffsetCommitRequestV3_TopicItem)
            .TaggedFields();
    }
});

Protocol.define('TxnOffsetCommitResponseV3_PartitionResult', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .TaggedFields();
    }
});

Protocol.define('TxnOffsetCommitResponseV3_TopicResult', {
    read: function () {
        this
            .compactString('topic')
            .compactArray('partitions', this.TxnOffsetCommitResponseV3_PartitionResult)
            .TaggedFields();
    }
});

Protocol.define('TxnOffsetCommitResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('results', this.TxnOffsetCommitResponseV3_TopicResult)
            .TaggedFields();
    }
});
