'use strict';

var Protocol = require('./index');
var globals  = require('./globals');
var errors   = require('../errors');
var _        = require('lodash');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

///////////////
// FETCH API //
///////////////

Protocol.define('FetchRequestPartitionItem', {
    write: function (data) { // {partition, offset, maxBytes}
        this
            .Int32BE(data.partition)
            .KafkaOffset(data.offset)
            .Int32BE(data.maxBytes);
    }
});

Protocol.define('FetchRequestTopicItem', {
    write: function (data) { // {topicName, partitions}
        this
            .string(data.topicName)
            .array(data.partitions, this.FetchRequestPartitionItem);
    }
});

Protocol.define('FetchRequest', {
    write: function (data) { // { maxWaitTime, minBytes, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.FetchRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(data.replicaId || -1) // ReplicaId
            .Int32BE(data.maxWaitTime)
            .Int32BE(data.minBytes)
            .array(data.topics, this.FetchRequestTopicItem);
    }
});

Protocol.define('FetchResponseTopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.FetchResponsePartitionItem);
    }
});

Protocol.define('FetchResponsePartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('highwaterMarkOffset')
            .Int32BE('messageSetSize')
            .MessageSet('messageSet', this.context.messageSetSize);

        if (this.context.messageSet.length === 1 && this.context.messageSet[0]._partial === true) {
            this.context.messageSetSize = 0;
            this.context.messageSet = [];
            this.context.error = errors.byName('MessageSizeTooLarge');
        } else {
            this.context.messageSet = _.dropRightWhile(this.context.messageSet, { _partial: true }); // drop partially read messages
        }
    }
});

Protocol.define('FetchResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            // .Int32BE('throttleTime')
            .array('topics', this.FetchResponseTopicItem);
    }
});

///////////////////
// FETCH v3/v4   //
///////////////////

Protocol.define('FetchRequestV3', {
    write: function (data) { // { maxWaitTime, minBytes, maxBytes, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.FetchRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(data.replicaId || -1)
            .Int32BE(data.maxWaitTime)
            .Int32BE(data.minBytes)
            .Int32BE(data.maxBytes || 0x7fffffff)
            .array(data.topics, this.FetchRequestTopicItem);
    }
});

Protocol.define('FetchResponseV3TopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.FetchResponsePartitionItem);
    }
});

Protocol.define('FetchResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('topics', this.FetchResponseV3TopicItem);
    }
});

Protocol.define('FetchRequestV4', {
    write: function (data) { // { maxWaitTime, minBytes, maxBytes, isolationLevel, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.FetchRequest,
                apiVersion: 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(data.replicaId || -1)
            .Int32BE(data.maxWaitTime)
            .Int32BE(data.minBytes)
            .Int32BE(data.maxBytes || 0x7fffffff)
            .Int8(data.isolationLevel || 0)
            .array(data.topics, this.FetchRequestTopicItem);
    }
});

Protocol.define('FetchResponseV4AbortedTransaction', {
    read: function () {
        this
            .KafkaOffset('producerId')
            .KafkaOffset('firstOffset');
    }
});

Protocol.define('FetchResponseV4PartitionItem', {
    read: function () {
        var peekOffset;
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('highwaterMarkOffset')
            .KafkaOffset('lastStableOffset')
            .array('abortedTransactions', this.FetchResponseV4AbortedTransaction)
            .Int32BE('messageSetSize');

        // format detection: peek at magic byte to decide MessageSet vs RecordBatch
        if (this.context.messageSetSize > 0) {
            // magic byte is at offset +16 from start of records: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4)
            peekOffset = this.offset + 16;
            if (peekOffset < this.offset + this.context.messageSetSize && this.buffer[peekOffset] === 2) {
                this.RecordBatch('messageSet', this.context.messageSetSize);
            } else {
                this.MessageSet('messageSet', this.context.messageSetSize);
                if (this.context.messageSet.length === 1 && this.context.messageSet[0]._partial === true) {
                    this.context.messageSetSize = 0;
                    this.context.messageSet = [];
                    this.context.error = errors.byName('MessageSizeTooLarge');
                } else {
                    this.context.messageSet = _.dropRightWhile(this.context.messageSet, { _partial: true });
                }
            }
        } else {
            this.context.messageSet = [];
        }
    }
});

Protocol.define('FetchResponseV4TopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.FetchResponseV4PartitionItem);
    }
});

Protocol.define('FetchResponseV4', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('topics', this.FetchResponseV4TopicItem);
    }
});

///////////////////
// FETCH v5/v6   //
///////////////////

// v5+ partition item adds logStartOffset
Protocol.define('FetchRequestV5PartitionItem', {
    write: function (data) { // {partition, offset, logStartOffset, maxBytes}
        this
            .Int32BE(data.partition)
            .KafkaOffset(data.offset)
            .KafkaOffset(data.logStartOffset || -1)
            .Int32BE(data.maxBytes);
    }
});

Protocol.define('FetchRequestV5TopicItem', {
    write: function (data) { // {topicName, partitions}
        this
            .string(data.topicName)
            .array(data.partitions, this.FetchRequestV5PartitionItem);
    }
});

Protocol.define('FetchRequestV6', {
    write: function (data) { // { maxWaitTime, minBytes, maxBytes, isolationLevel, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.FetchRequest,
                apiVersion: 6,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(data.replicaId || -1)
            .Int32BE(data.maxWaitTime)
            .Int32BE(data.minBytes)
            .Int32BE(data.maxBytes || 0x7fffffff)
            .Int8(data.isolationLevel || 0)
            .array(data.topics, this.FetchRequestV5TopicItem);
    }
});

Protocol.define('FetchResponseV5PartitionItem', {
    read: function () {
        var peekOffset;
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('highwaterMarkOffset')
            .KafkaOffset('lastStableOffset')
            .KafkaOffset('logStartOffset')
            .array('abortedTransactions', this.FetchResponseV4AbortedTransaction)
            .Int32BE('messageSetSize');

        // format detection: peek at magic byte to decide MessageSet vs RecordBatch
        if (this.context.messageSetSize > 0) {
            peekOffset = this.offset + 16;
            if (peekOffset < this.offset + this.context.messageSetSize && this.buffer[peekOffset] === 2) {
                this.RecordBatch('messageSet', this.context.messageSetSize);
            } else {
                this.MessageSet('messageSet', this.context.messageSetSize);
                if (this.context.messageSet.length === 1 && this.context.messageSet[0]._partial === true) {
                    this.context.messageSetSize = 0;
                    this.context.messageSet = [];
                    this.context.error = errors.byName('MessageSizeTooLarge');
                } else {
                    this.context.messageSet = _.dropRightWhile(this.context.messageSet, { _partial: true });
                }
            }
        } else {
            this.context.messageSet = [];
        }
    }
});

Protocol.define('FetchResponseV5TopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.FetchResponseV5PartitionItem);
    }
});

Protocol.define('FetchResponseV5', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('topics', this.FetchResponseV5TopicItem);
    }
});

///////////////////
// FETCH v7      //
///////////////////

// v7 (KIP-227): fetch sessions — adds sessionId, sessionEpoch, forgottenTopicsData
Protocol.define('FetchRequestV7ForgottenTopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.Int32BE);
    }
});

Protocol.define('FetchRequestV7', {
    write: function (data) { // { maxWaitTime, minBytes, maxBytes, isolationLevel, sessionId, sessionEpoch, topics, forgottenTopicsData }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.FetchRequest,
                apiVersion: 7,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(data.replicaId || -1)
            .Int32BE(data.maxWaitTime)
            .Int32BE(data.minBytes)
            .Int32BE(data.maxBytes || 0x7fffffff)
            .Int8(data.isolationLevel || 0)
            .Int32BE(data.sessionId !== undefined ? data.sessionId : 0)
            .Int32BE(data.sessionEpoch !== undefined ? data.sessionEpoch : -1)
            .array(data.topics, this.FetchRequestV5TopicItem) // reuse v5 topic items
            .array(data.forgottenTopicsData || [], this.FetchRequestV7ForgottenTopicItem);
    }
});

// v7 response adds errorCode + sessionId after throttleTime
Protocol.define('FetchResponseV7', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .Int32BE('sessionId')
            .array('topics', this.FetchResponseV5TopicItem); // reuse v5 partition items
    }
});

///////////////////
// FETCH v9/v10  //
///////////////////

// v9 (KIP-320): partition item adds currentLeaderEpoch before fetchOffset
Protocol.define('FetchRequestV9PartitionItem', {
    write: function (data) { // {partition, currentLeaderEpoch, offset, logStartOffset, maxBytes}
        this
            .Int32BE(data.partition)
            .Int32BE(data.currentLeaderEpoch !== undefined ? data.currentLeaderEpoch : -1)
            .KafkaOffset(data.offset)
            .KafkaOffset(data.logStartOffset || -1)
            .Int32BE(data.maxBytes);
    }
});

Protocol.define('FetchRequestV9TopicItem', {
    write: function (data) { // {topicName, partitions}
        this
            .string(data.topicName)
            .array(data.partitions, this.FetchRequestV9PartitionItem);
    }
});

// v10 — apiVersion 10, same as v7 structure but uses v9 topic items
Protocol.define('FetchRequestV10', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.FetchRequest,
                apiVersion: 10,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(data.replicaId || -1)
            .Int32BE(data.maxWaitTime)
            .Int32BE(data.minBytes)
            .Int32BE(data.maxBytes || 0x7fffffff)
            .Int8(data.isolationLevel || 0)
            .Int32BE(data.sessionId !== undefined ? data.sessionId : 0)
            .Int32BE(data.sessionEpoch !== undefined ? data.sessionEpoch : -1)
            .array(data.topics, this.FetchRequestV9TopicItem) // v9 partition items with currentLeaderEpoch
            .array(data.forgottenTopicsData || [], this.FetchRequestV7ForgottenTopicItem);
    }
});
