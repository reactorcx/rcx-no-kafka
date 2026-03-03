'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// KIP-932: ShareGroupHeartbeat (apiKey 76)
// Flexible from v0+. v0 was early-access (removed in 4.1), v1 is the first stable version.

// -- Helper: TopicPartitions item (used in assignment) --
Protocol.define('ShareGroupHeartbeat_TopicPartitionsItem', {
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
Protocol.define('ShareGroupHeartbeat_Assignment', {
    read: function () {
        this.Int8('_present');
        if (this.context._present < 0) {
            return null;
        }
        this
            .compactArray('topicPartitions', this.ShareGroupHeartbeat_TopicPartitionsItem)
            .TaggedFields();
    },
    write: function (data) {
        if (data === null || data === undefined) {
            this.Int8(-1);
        } else {
            this
                .Int8(1)
                .compactArray(data.topicPartitions, this.ShareGroupHeartbeat_TopicPartitionsItem)
                .TaggedFields();
        }
    }
});

// -- ShareGroupHeartbeatRequest v1 --
Protocol.define('ShareGroupHeartbeatRequestV1', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ShareGroupHeartbeatRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.groupId)
            .compactString(data.memberId || '')
            .Int32BE(data.memberEpoch)
            .compactNullableString(data.rackId !== undefined ? data.rackId : null)
            .compactNullableArray(data.subscribedTopicNames, this.compactString)
            .TaggedFields();
    }
});

// -- ShareGroupHeartbeatResponse v1 --
Protocol.define('ShareGroupHeartbeatResponseV1', {
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
            .ShareGroupHeartbeat_Assignment('assignment')
            .TaggedFields();
    }
});
