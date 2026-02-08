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
