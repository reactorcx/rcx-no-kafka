'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// KIP-932: ShareAcknowledge (apiKey 79)
// Flexible from v0+. v1 is the first stable version in Kafka 4.2.

// ========================
//  REQUEST helpers
// ========================

// Reuse the same AcknowledgementBatch shape from ShareFetch
Protocol.define('ShareAck_AcknowledgementBatchItem', {
    write: function (data) {
        this
            .Int64BE(data.firstOffset)
            .Int64BE(data.lastOffset)
            .compactArray(data.acknowledgeTypes, this.Int8)
            .TaggedFields();
    }
});

Protocol.define('ShareAck_PartitionItem', {
    write: function (data) {
        this
            .Int32BE(data.partitionIndex)
            .compactArray(data.acknowledgementBatches, this.ShareAck_AcknowledgementBatchItem)
            .TaggedFields();
    }
});

Protocol.define('ShareAck_TopicItem', {
    write: function (data) {
        this
            .uuid(data.topicId)
            .compactArray(data.partitions, this.ShareAck_PartitionItem)
            .TaggedFields();
    }
});

// -- ShareAcknowledgeRequest v1 --
Protocol.define('ShareAcknowledgeRequestV1', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ShareAcknowledgeRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactNullableString(data.groupId)
            .compactString(data.memberId)
            .Int32BE(data.shareSessionEpoch)
            .compactArray(data.topics || [], this.ShareAck_TopicItem)
            .TaggedFields();
    }
});

// ========================
//  RESPONSE helpers
// ========================

Protocol.define('ShareAck_LeaderIdAndEpoch', {
    read: function () {
        this
            .Int32BE('leaderId')
            .Int32BE('leaderEpoch')
            .TaggedFields();
    }
});

Protocol.define('ShareAck_PartitionResponse', {
    read: function () {
        this
            .Int32BE('partitionIndex')
            .ErrorCode('error')
            .compactNullableString('errorMessage')
            .ShareAck_LeaderIdAndEpoch('currentLeader')
            .TaggedFields();
    }
});

Protocol.define('ShareAck_TopicResponse', {
    read: function () {
        this
            .uuid('topicId')
            .compactArray('partitions', this.ShareAck_PartitionResponse)
            .TaggedFields();
    }
});

Protocol.define('ShareAck_NodeEndpoint', {
    read: function () {
        this
            .Int32BE('nodeId')
            .compactString('host')
            .Int32BE('port')
            .compactNullableString('rack')
            .TaggedFields();
    }
});

// -- ShareAcknowledgeResponse v1 --
Protocol.define('ShareAcknowledgeResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .compactNullableString('errorMessage')
            .compactArray('responses', this.ShareAck_TopicResponse)
            .compactArray('nodeEndpoints', this.ShareAck_NodeEndpoint)
            .TaggedFields();
    }
});
