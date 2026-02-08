'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

////////////////
// OFFSET API //
////////////////

Protocol.define('OffsetRequestPartitionItem', {
    write: function (data) { // { partition, time, maxNumberOfOffsets }
        this
            .Int32BE(data.partition)
            .Int64BE(data.time) // Used to ask for all messages before a certain time (ms).
            .Int32BE(data.maxNumberOfOffsets);
    }
});

Protocol.define('OffsetRequestTopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.OffsetRequestPartitionItem);
    }
});

Protocol.define('OffsetRequest', {
    write: function (data) { // { replicaId, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(data.replicaId || -1) // ReplicaId
            .array(data.topics, this.OffsetRequestTopicItem);
    }
});

Protocol.define('OffsetResponseTopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.OffsetResponsePartitionItem);
    }
});

Protocol.define('OffsetResponsePartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .array('offset', this.KafkaOffset);
    }
});

Protocol.define('OffsetResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.OffsetResponseTopicItem);
    }
});

//////////////////
// OFFSET v1    //
//////////////////

Protocol.define('OffsetRequestV1PartitionItem', {
    write: function (data) { // { partition, time } — no maxNumberOfOffsets in v1
        this
            .Int32BE(data.partition)
            .Int64BE(data.time);
    }
});

Protocol.define('OffsetRequestV1TopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.OffsetRequestV1PartitionItem);
    }
});

Protocol.define('OffsetRequestV1', {
    write: function (data) { // { replicaId, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int32BE(data.replicaId || -1)
            .array(data.topics, this.OffsetRequestV1TopicItem);
    }
});

Protocol.define('OffsetResponseV1PartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('timestamp')
            .KafkaOffset('offset');
    }
});

Protocol.define('OffsetResponseV1TopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.OffsetResponseV1PartitionItem);
    }
});

Protocol.define('OffsetResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.OffsetResponseV1TopicItem);
    }
});
