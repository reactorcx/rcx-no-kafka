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

/////////////////////
// PRODUCE v9      //
/////////////////////

// v9 — first flexible version (KIP-482), same fields as v8 but compact encoding
// records field uses compactBytes encoding: UVarint(N+1) prefix instead of Int32BE(N)
Protocol.define('ProduceRequestV9PartitionItem', {
    write: function (data) { // {partition, batch}
        var _o1, _o2, batchLength, batchBytes;
        this.Int32BE(data.partition);
        // serialize RecordBatch to capture its raw bytes
        _o1 = this.offset;
        this.RecordBatch(data.batch);
        _o2 = this.offset;
        batchLength = _o2 - _o1;
        // copy serialized bytes, rewind, and re-write with compact prefix
        batchBytes = Buffer.alloc(batchLength);
        this.buffer.copy(batchBytes, 0, _o1, _o2);
        this.offset = _o1;
        this
            .UVarint(batchLength + 1)
            .raw(batchBytes);
        this.TaggedFields();
    }
});

Protocol.define('ProduceRequestV9TopicItem', {
    write: function (data) { // {topicName, partitions}
        this
            .compactString(data.topicName)
            .compactArray(data.partitions, this.ProduceRequestV9PartitionItem)
            .TaggedFields();
    }
});

Protocol.define('ProduceRequestV9', {
    write: function (data) { // { transactionalId, requiredAcks, timeout, topics }
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 9,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactNullableString(data.transactionalId || null)
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .compactArray(data.topics, this.ProduceRequestV9TopicItem)
            .TaggedFields();
    }
});

/////////////////////
// PRODUCE v10/v11 //
/////////////////////

// v10 — KIP-951: leader discovery optimisations (CurrentLeader tagged field in response)
// v11 — KIP-951: adds NodeEndpoints tagged field in response
// Request wire format is identical to v9; only apiVersion changes.

Protocol.define('ProduceRequestV10', {
    write: function (data) { // { transactionalId, requiredAcks, timeout, topics }
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 10,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactNullableString(data.transactionalId || null)
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .compactArray(data.topics, this.ProduceRequestV9TopicItem)
            .TaggedFields();
    }
});

Protocol.define('ProduceRequestV11', {
    write: function (data) { // { transactionalId, requiredAcks, timeout, topics }
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 11,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactNullableString(data.transactionalId || null)
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .compactArray(data.topics, this.ProduceRequestV9TopicItem)
            .TaggedFields();
    }
});

/////////////////////
// PRODUCE v12     //
/////////////////////

// v12 (KIP-890 Part 2) — wire-identical to v11; broker implicitly handles AddPartitionsToTxn
Protocol.define('ProduceRequestV12', {
    write: function (data) { // { transactionalId, requiredAcks, timeout, topics }
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 12,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactNullableString(data.transactionalId || null)
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .compactArray(data.topics, this.ProduceRequestV9TopicItem)
            .TaggedFields();
    }
});

/////////////////////
// PRODUCE v13     //
/////////////////////

// v13 (KIP-516) — topic items use uuid (topicId) instead of topicName
Protocol.define('ProduceRequestV13TopicItem', {
    write: function (data) { // {topicId, partitions}
        this
            .uuid(data.topicId)
            .compactArray(data.partitions, this.ProduceRequestV9PartitionItem)
            .TaggedFields();
    }
});

Protocol.define('ProduceRequestV13', {
    write: function (data) { // { transactionalId, requiredAcks, timeout, topics }
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ProduceRequest,
                apiVersion: 13,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactNullableString(data.transactionalId || null)
            .Int16BE(data.requiredAcks)
            .Int32BE(data.timeout)
            .compactArray(data.topics, this.ProduceRequestV13TopicItem)
            .TaggedFields();
    }
});

Protocol.define('ProduceResponseV13TopicItem', {
    read: function () {
        this
            .uuid('topicId')
            .compactArray('partitions', this.ProduceResponseV10PartitionItem)
            .TaggedFields();
    }
});

Protocol.define('ProduceResponseV13', {
    read: function () {
        var i;
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .compactArray('topics', this.ProduceResponseV13TopicItem)
            .Int32BE('throttleTime');
        // Parse body-level tagged fields — tag 0 = NodeEndpoints (KIP-951)
        this.UVarint('_tagCount');
        for (i = 0; i < this.context._tagCount; i++) {
            this.UVarint('_tag');
            this.UVarint('_tagSize');
            if (this.context._tag === 0) {
                this.compactArray('nodeEndpoints', this.ProduceResponseV10NodeEndpoint);
            } else {
                this.skip(this.context._tagSize);
            }
        }
    }
});

// v10/v11 response helper structs

Protocol.define('ProduceResponseV10CurrentLeader', {
    read: function () {
        this
            .Int32BE('leaderId')
            .Int32BE('leaderEpoch')
            .TaggedFields();
    }
});

Protocol.define('ProduceResponseV10NodeEndpoint', {
    read: function () {
        this
            .Int32BE('nodeId')
            .compactString('host')
            .Int32BE('port')
            .compactNullableString('rack')
            .TaggedFields();
    }
});

Protocol.define('ProduceResponseV10PartitionItem', {
    read: function () {
        var i;
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('offset')
            .KafkaOffset('timestamp')
            .KafkaOffset('logStartOffset')
            .compactArray('recordErrors', this.ProduceResponseV9RecordError)
            .compactNullableString('errorMessage');
        // Parse tagged fields — tag 0 = CurrentLeader (KIP-951)
        this.UVarint('_tagCount');
        for (i = 0; i < this.context._tagCount; i++) {
            this.UVarint('_tag');
            this.UVarint('_tagSize');
            if (this.context._tag === 0) {
                this.ProduceResponseV10CurrentLeader('currentLeader');
            } else {
                this.skip(this.context._tagSize);
            }
        }
    }
});

Protocol.define('ProduceResponseV10TopicItem', {
    read: function () {
        this
            .compactString('topicName')
            .compactArray('partitions', this.ProduceResponseV10PartitionItem)
            .TaggedFields();
    }
});

Protocol.define('ProduceResponseV10', {
    read: function () {
        var i;
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .compactArray('topics', this.ProduceResponseV10TopicItem)
            .Int32BE('throttleTime');
        // Parse body-level tagged fields — tag 0 = NodeEndpoints (KIP-951)
        this.UVarint('_tagCount');
        for (i = 0; i < this.context._tagCount; i++) {
            this.UVarint('_tag');
            this.UVarint('_tagSize');
            if (this.context._tag === 0) {
                this.compactArray('nodeEndpoints', this.ProduceResponseV10NodeEndpoint);
            } else {
                this.skip(this.context._tagSize);
            }
        }
    }
});

Protocol.define('ProduceResponseV9RecordError', {
    read: function () {
        this
            .Int32BE('batchIndex')
            .compactNullableString('batchIndexErrorMessage')
            .TaggedFields();
    }
});

Protocol.define('ProduceResponseV9PartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .KafkaOffset('offset')
            .KafkaOffset('timestamp')
            .KafkaOffset('logStartOffset')
            .compactArray('recordErrors', this.ProduceResponseV9RecordError)
            .compactNullableString('errorMessage')
            .TaggedFields();
    }
});

Protocol.define('ProduceResponseV9TopicItem', {
    read: function () {
        this
            .compactString('topicName')
            .compactArray('partitions', this.ProduceResponseV9PartitionItem)
            .TaggedFields();
    }
});

// ProduceResponseV11, V12 — reuse ProduceResponseV9 (identical wire format before NodeEndpoints)
Protocol.define('ProduceResponseV9', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .compactArray('topics', this.ProduceResponseV9TopicItem)
            .Int32BE('throttleTime')
            .TaggedFields();
    }
});
