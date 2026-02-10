'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

/////////////////////////////
// OFFSET COMMIT/FETCH API //
/////////////////////////////

// group coordinator request and response are defined in group_membership.js

// v0
Protocol.define('OffsetCommitRequestV0PartitionItem', {
    write: function (data) { // { partition, offset, metadata }
        this
            .Int32BE(data.partition)
            .Int64BE(data.offset)
            .string(data.metadata);
    }
});

Protocol.define('OffsetCommitRequestV0TopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.OffsetCommitRequestV0PartitionItem);
    }
});

Protocol.define('OffsetCommitRequestV0', {
    write: function (data) { // { groupId, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetCommitRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .array(data.topics, this.OffsetCommitRequestV0TopicItem);
    }
});

// v1
/* istanbul ignore next */
Protocol.define('OffsetCommitRequestV1PartitionItem', {
    write: function (data) { // { partition, offset, timestamp, metadata }
        this
            .Int32BE(data.partition)
            .Int64BE(data.offset)
            .Int64BE(data.timestamp)
            .string(data.metadata);
    }
});

/* istanbul ignore next */
Protocol.define('OffsetCommitRequestV1TopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.OffsetCommitRequestV1PartitionItem);
    }
});

/* istanbul ignore next */
Protocol.define('OffsetCommitRequestV1', {
    write: function (data) { // { groupId, generationId, memberId,  topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetCommitRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .string(data.generationId)
            .string(data.memberId)
            .array(data.topics, this.OffsetCommitRequestV1TopicItem);
    }
});

// v2
Protocol.define('OffsetCommitRequestV2', {
    write: function (data) { // { groupId, generationId, memberId, retentionTime, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetCommitRequest,
                apiVersion: 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .Int64BE(data.retentionTime)
            .array(data.topics, this.OffsetCommitRequestV0TopicItem); // same as in v0
    }
});


Protocol.define('OffsetCommitResponseTopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.OffsetCommitResponsePartitionItem);
    }
});

Protocol.define('OffsetCommitResponsePartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error');
    }
});

// v0, v1 and v2
Protocol.define('OffsetCommitResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.OffsetCommitResponseTopicItem);
    }
});

// v3 — retentionTime still present in wire format (broker ignores it), adds throttleTime to response
Protocol.define('OffsetCommitRequestV3', {
    write: function (data) { // { groupId, generationId, memberId, retentionTime, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetCommitRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .Int64BE(data.retentionTime || -1) // -1 = use broker/topic config
            .array(data.topics, this.OffsetCommitRequestV0TopicItem); // same as v0/v2
    }
});

Protocol.define('OffsetCommitResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('topics', this.OffsetCommitResponseTopicItem);
    }
});

// v6 — removes retentionTime from request; adds committedLeaderEpoch per partition
Protocol.define('OffsetCommitRequestV6PartitionItem', {
    write: function (data) { // { partition, offset, committedLeaderEpoch, metadata }
        this
            .Int32BE(data.partition)
            .Int64BE(data.offset)
            .Int32BE(data.committedLeaderEpoch !== undefined ? data.committedLeaderEpoch : -1)
            .string(data.metadata);
    }
});

Protocol.define('OffsetCommitRequestV6TopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.OffsetCommitRequestV6PartitionItem);
    }
});

Protocol.define('OffsetCommitRequestV6', {
    write: function (data) { // { groupId, generationId, memberId, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetCommitRequest,
                apiVersion: 6,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .array(data.topics, this.OffsetCommitRequestV6TopicItem);
    }
});

Protocol.define('OffsetCommitResponseV6PartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error');
    }
});

Protocol.define('OffsetCommitResponseV6TopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.OffsetCommitResponseV6PartitionItem);
    }
});

Protocol.define('OffsetCommitResponseV6', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('topics', this.OffsetCommitResponseV6TopicItem);
    }
});

// v7 (KIP-345) — adds groupInstanceId after memberId
Protocol.define('OffsetCommitRequestV7', {
    write: function (data) { // { groupId, generationId, memberId, groupInstanceId, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetCommitRequest,
                apiVersion: 7,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .string(data.groupInstanceId || null)
            .array(data.topics, this.OffsetCommitRequestV6TopicItem);
    }
});


// OffsetCommit v8 — first flexible version (KIP-482)
Protocol.define('OffsetCommitRequestV8PartitionItem', {
    write: function (data) {
        this
            .Int32BE(data.partition)
            .Int64BE(data.offset)
            .Int32BE(data.committedLeaderEpoch !== undefined ? data.committedLeaderEpoch : -1)
            .compactNullableString(data.metadata)
            .TaggedFields();
    }
});

Protocol.define('OffsetCommitRequestV8TopicItem', {
    write: function (data) {
        this
            .compactString(data.topicName)
            .compactArray(data.partitions, this.OffsetCommitRequestV8PartitionItem)
            .TaggedFields();
    }
});

Protocol.define('OffsetCommitRequestV8', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.OffsetCommitRequest,
                apiVersion: 8,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.groupId)
            .Int32BE(data.generationId)
            .compactString(data.memberId)
            .compactNullableString(data.groupInstanceId || null)
            .compactArray(data.topics, this.OffsetCommitRequestV8TopicItem)
            .TaggedFields();
    }
});

Protocol.define('OffsetCommitResponseV8PartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .ErrorCode('error')
            .TaggedFields();
    }
});

Protocol.define('OffsetCommitResponseV8TopicItem', {
    read: function () {
        this
            .compactString('topicName')
            .compactArray('partitions', this.OffsetCommitResponseV8PartitionItem)
            .TaggedFields();
    }
});

Protocol.define('OffsetCommitResponseV8', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('topics', this.OffsetCommitResponseV8TopicItem)
            .TaggedFields();
    }
});

Protocol.define('OffsetFetchRequestTopicItem', {
    write: function (data) { // { topicName, partitions }
        this
            .string(data.topicName)
            .array(data.partitions, this.Int32BE);
    }
});

Protocol.define('OffsetFetchRequest', {
    write: function (data) { // { apiVersion, groupId, topics }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetFetchRequest,
                apiVersion: data.apiVersion || 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .array(data.topics, this.OffsetFetchRequestTopicItem);
    }
});

Protocol.define('OffsetFetchResponseTopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.OffsetFetchResponsePartitionItem);
    }
});

Protocol.define('OffsetFetchResponsePartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .KafkaOffset('offset')
            .string('metadata')
            .ErrorCode('error');
    }
});

Protocol.define('OffsetFetchResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.OffsetFetchResponseTopicItem);
    }
});

// v2 response adds top-level ErrorCode
Protocol.define('OffsetFetchResponseV2', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('topics', this.OffsetFetchResponseTopicItem)
            .ErrorCode('error');
    }
});

// v3 response adds throttleTime before topics
Protocol.define('OffsetFetchResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('topics', this.OffsetFetchResponseTopicItem)
            .ErrorCode('error');
    }
});

// v5 — response adds committedLeaderEpoch per partition
Protocol.define('OffsetFetchRequestV5', {
    write: function (data) { // { groupId, topics (null = all) }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetFetchRequest,
                apiVersion: 5,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId);
        if (data.topics === null || data.topics === undefined) {
            this.Int32BE(-1); // null array = all committed offsets
        } else {
            this.array(data.topics, this.OffsetFetchRequestTopicItem);
        }
    }
});

Protocol.define('OffsetFetchResponseV5PartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .KafkaOffset('offset')
            .Int32BE('committedLeaderEpoch')
            .string('metadata')
            .ErrorCode('error');
    }
});

Protocol.define('OffsetFetchResponseV5TopicItem', {
    read: function () {
        this
            .string('topicName')
            .array('partitions', this.OffsetFetchResponseV5PartitionItem);
    }
});

Protocol.define('OffsetFetchResponseV5', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('topics', this.OffsetFetchResponseV5TopicItem)
            .ErrorCode('error');
    }
});

// OffsetFetch v6 — first flexible version (KIP-482)
Protocol.define('OffsetFetchRequestV6_TopicItem', {
    write: function (data) {
        this
            .compactString(data.topicName)
            .compactArray(data.partitions, this.Int32BE)
            .TaggedFields();
    }
});

Protocol.define('OffsetFetchRequestV6', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.OffsetFetchRequest,
                apiVersion: 6,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.groupId);
        if (data.topics === null || data.topics === undefined) {
            this.UVarint(0); // compact null array
        } else {
            this.compactArray(data.topics, this.OffsetFetchRequestV6_TopicItem);
        }
        this.TaggedFields();
    }
});

Protocol.define('OffsetFetchResponseV6PartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .KafkaOffset('offset')
            .Int32BE('committedLeaderEpoch')
            .compactNullableString('metadata')
            .ErrorCode('error')
            .TaggedFields();
    }
});

Protocol.define('OffsetFetchResponseV6TopicItem', {
    read: function () {
        this
            .compactString('topicName')
            .compactArray('partitions', this.OffsetFetchResponseV6PartitionItem)
            .TaggedFields();
    }
});

Protocol.define('OffsetFetchResponseV6', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('topics', this.OffsetFetchResponseV6TopicItem)
            .ErrorCode('error')
            .TaggedFields();
    }
});

// OffsetFetch v7 (KIP-447) — adds requireStable flag; response same as v6
Protocol.define('OffsetFetchRequestV7', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.OffsetFetchRequest,
                apiVersion: 7,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.groupId);
        if (data.topics === null || data.topics === undefined) {
            this.UVarint(0);
        } else {
            this.compactArray(data.topics, this.OffsetFetchRequestV6_TopicItem);
        }
        this
            .Int8(data.requireStable ? 1 : 0)
            .TaggedFields();
    }
});

// OffsetFetch v8 (KIP-709) — multi-group batch; request/response restructured to groups array
Protocol.define('OffsetFetchRequestV8_GroupItem', {
    write: function (data) {
        this.compactString(data.groupId);
        // v9 adds memberId + memberEpoch inside each group item (KIP-848)
        if (data.apiVersion >= 9) {
            this
                .compactNullableString(data.memberId || null)
                .Int32BE(data.memberEpoch !== undefined ? data.memberEpoch : -1);
        }
        if (data.topics === null || data.topics === undefined) {
            this.UVarint(0);
        } else {
            this.compactArray(data.topics, this.OffsetFetchRequestV6_TopicItem);
        }
        this.TaggedFields();
    }
});

Protocol.define('OffsetFetchRequestV8', {
    write: function (data) { // { apiVersion, groups: [{ groupId, topics, apiVersion? }], requireStable }
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.OffsetFetchRequest,
                apiVersion: data.apiVersion || 8,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactArray(data.groups, this.OffsetFetchRequestV8_GroupItem)
            .Int8(data.requireStable ? 1 : 0)
            .TaggedFields();
    }
});

Protocol.define('OffsetFetchResponseV8PartitionItem', {
    read: function () {
        this
            .Int32BE('partition')
            .KafkaOffset('offset')
            .Int32BE('committedLeaderEpoch')
            .compactNullableString('metadata')
            .ErrorCode('error')
            .TaggedFields();
    }
});

Protocol.define('OffsetFetchResponseV8TopicItem', {
    read: function () {
        this
            .compactString('topicName')
            .compactArray('partitions', this.OffsetFetchResponseV8PartitionItem)
            .TaggedFields();
    }
});

Protocol.define('OffsetFetchResponseV8_GroupItem', {
    read: function () {
        this
            .compactString('groupId')
            .compactArray('topics', this.OffsetFetchResponseV8TopicItem)
            .ErrorCode('error')
            .TaggedFields();
    }
});

Protocol.define('OffsetFetchResponseV8', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('groups', this.OffsetFetchResponseV8_GroupItem)
            .TaggedFields();
    }
});

// v3 request supports null topics (fetch all committed offsets)
Protocol.define('OffsetFetchRequestV3', {
    write: function (data) { // { groupId, topics (null = all) }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.OffsetFetchRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId);
        if (data.topics === null || data.topics === undefined) {
            this.Int32BE(-1); // null array = all committed offsets
        } else {
            this.array(data.topics, this.OffsetFetchRequestTopicItem);
        }
    }
});
