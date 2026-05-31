'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// KIP-848: ConsumerGroupHeartbeat (apiKey 68) — next-gen consumer rebalance protocol.
// Flexible from v0+. We implement v1 (KIP-1082: client-generated UUID member IDs,
// adds SubscribedTopicRegex).

// -- Helper: TopicPartitions item (used in request owned set and response assignment) --
Protocol.define('ConsumerGroupHeartbeat_TopicPartitionsItem', {
    read: function () {
        this
            .uuid('topicId')
            .compactArray('partitions', this.Int32BE)
            .TaggedFields();
    },
    write: function (data) {
        this
            .uuid(data.topicId)
            .compactArray(data.partitions, this.Int32BE)
            .TaggedFields();
    }
});

// -- Helper: nullable struct reader/writer for Assignment --
// Kafka encodes non-tagged nullable structs with Int8: -1 = null, 1 = present
Protocol.define('ConsumerGroupHeartbeat_Assignment', {
    read: function () {
        this.Int8('_present');
        if (this.context._present < 0) {
            return null;
        }
        this
            .compactArray('topicPartitions', this.ConsumerGroupHeartbeat_TopicPartitionsItem)
            .TaggedFields();
        return undefined;
    },
    write: function (data) {
        if (data === null || data === undefined) {
            this.Int8(-1);
        } else {
            this
                .Int8(1)
                .compactArray(data.topicPartitions, this.ConsumerGroupHeartbeat_TopicPartitionsItem)
                .TaggedFields();
        }
    }
});

// -- ConsumerGroupHeartbeatRequest v1 --
Protocol.define('ConsumerGroupHeartbeatRequestV1', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ConsumerGroupHeartbeatRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.groupId)
            .compactString(data.memberId || '')
            .Int32BE(data.memberEpoch)
            .compactNullableString(data.instanceId !== undefined ? data.instanceId : null)
            .compactNullableString(data.rackId !== undefined ? data.rackId : null)
            .Int32BE(data.rebalanceTimeoutMs !== undefined ? data.rebalanceTimeoutMs : -1)
            .compactNullableArray(data.subscribedTopicNames, this.compactString)
            .compactNullableString(data.subscribedTopicRegex !== undefined ? data.subscribedTopicRegex : null)
            .compactNullableString(data.serverAssignor !== undefined ? data.serverAssignor : null)
            .compactNullableArray(data.topicPartitions, this.ConsumerGroupHeartbeat_TopicPartitionsItem)
            .TaggedFields();
    }
});

// -- ConsumerGroupHeartbeatResponse v1 --
Protocol.define('ConsumerGroupHeartbeatResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .compactNullableString('errorMessage')
            .compactNullableString('memberId')
            .Int32BE('memberEpoch')
            .Int32BE('heartbeatIntervalMs')
            .ConsumerGroupHeartbeat_Assignment('assignment')
            .TaggedFields();
    }
});
