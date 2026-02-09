'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

/////////////////
// PRODUCE API //
/////////////////

Protocol.define('ProduceRequestPartitionItem', {
    write: function (data) { // {partition, messageSet}
        var _o1, _o2;
        this.Int32BE(data.partition);
        _o1 = this.offset;
        this
            .skip(4)
            .MessageSet(data.messageSet);
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

Protocol.define('ProduceRequestTopicItem', {
    write: function (data) { // {topicName, partitions}
        this
            .string(data.topicName)
            .array(data.partitions, this.ProduceRequestPartitionItem);
    }
});

Protocol.define('ProduceRequest', {
    write: function (data) { // { requiredAcks, timeout, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .array(data.topics, this.ProduceRequestTopicItem);
    }
});

Protocol.define('ProduceResponseTopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.ProduceResponsePartitionItem);
    }
});

Protocol.define('ProduceResponsePartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('offset');
    }
});

Protocol.define('ProduceResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.ProduceResponseTopicItem)
            .Int32BE('throttleTime');
    }
});

/////////////////////
// PRODUCE v2/v3   //
/////////////////////

Protocol.define('ProduceResponseV2PartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('offset')
            .KafkaOffset('timestamp');
    }
});

Protocol.define('ProduceResponseV2TopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.ProduceResponseV2PartitionItem);
    }
});

Protocol.define('ProduceResponseV2', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.ProduceResponseV2TopicItem)
            .Int32BE('throttleTime');
    }
});

Protocol.define('ProduceRequestV3PartitionItem', {
    write: function (data) { // {partition, batch}
        var _o1, _o2;
        this.Int32BE(data.partition);
        _o1 = this.offset;
        this
            .skip(4)
            .RecordBatch(data.batch);
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

Protocol.define('ProduceRequestV3TopicItem', {
    write: function (data) { // {topicName, partitions}
        this
            .string(data.topicName)
            .array(data.partitions, this.ProduceRequestV3PartitionItem);
    }
});

Protocol.define('ProduceRequestV3', {
    write: function (data) { // { transactionalId, requiredAcks, timeout, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId || null) // null for non-transactional
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .array(data.topics, this.ProduceRequestV3TopicItem);
    }
});

/////////////////////
// PRODUCE v5      //
/////////////////////

Protocol.define('ProduceRequestV5', {
    write: function (data) { // { transactionalId, requiredAcks, timeout, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 5,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId || null)
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .array(data.topics, this.ProduceRequestV3TopicItem); // same wire format as v3
    }
});

Protocol.define('ProduceResponseV5PartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('offset')
            .KafkaOffset('timestamp')
            .KafkaOffset('logStartOffset');
    }
});

Protocol.define('ProduceResponseV5TopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.ProduceResponseV5PartitionItem);
    }
});

Protocol.define('ProduceResponseV5', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.ProduceResponseV5TopicItem)
            .Int32BE('throttleTime');
    }
});

/////////////////////
// PRODUCE v7      //
/////////////////////

Protocol.define('ProduceRequestV7', {
    write: function (data) { // { transactionalId, requiredAcks, timeout, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 7,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId || null)
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .array(data.topics, this.ProduceRequestV3TopicItem); // same wire format as v3/v5
    }
});

Protocol.define('ProduceResponseV7RecordError', {
    read: function () {
        this
            .Int32BE('batchIndex')
            .string('batchIndexErrorMessage');
    }
});

Protocol.define('ProduceResponseV7PartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('offset')
            .KafkaOffset('timestamp')
            .KafkaOffset('logStartOffset')
            .array('recordErrors', this.ProduceResponseV7RecordError)
            .string('errorMessage');
    }
});

Protocol.define('ProduceResponseV7TopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.ProduceResponseV7PartitionItem);
    }
});

Protocol.define('ProduceResponseV7', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.ProduceResponseV7TopicItem)
            .Int32BE('throttleTime');
    }
});

/////////////////////
// PRODUCE v8      //
/////////////////////

Protocol.define('ProduceRequestV8', {
    write: function (data) { // { transactionalId, requiredAcks, timeout, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 8,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.transactionalId || null)
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .array(data.topics, this.ProduceRequestV3TopicItem); // same wire format as v3/v5/v7
    }
});
