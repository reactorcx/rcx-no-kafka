'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// KIP-932: ShareFetch (apiKey 78)
// Flexible from v0+. v1 is the first stable version in Kafka 4.2.

// ========================
//  REQUEST helpers
// ========================

Protocol.define('ShareFetch_AcknowledgementBatchItem', {
    write: function (data) {
        this
            .Int64BE(data.firstOffset)
            .Int64BE(data.lastOffset)
            .compactArray(data.acknowledgeTypes, this.Int8)
            .TaggedFields();
    }
});

Protocol.define('ShareFetch_FetchPartitionItem', {
    write: function (data) {
        this
            .Int32BE(data.partitionIndex)
            .compactArray(data.acknowledgementBatches || [], this.ShareFetch_AcknowledgementBatchItem)
            .TaggedFields();
    }
});

Protocol.define('ShareFetch_FetchTopicItem', {
    write: function (data) {
        this
            .uuid(data.topicId)
            .compactArray(data.partitions, this.ShareFetch_FetchPartitionItem)
            .TaggedFields();
    }
});

Protocol.define('ShareFetch_ForgottenTopicItem', {
    write: function (data) {
        this
            .uuid(data.topicId)
            .compactArray(data.partitions, this.Int32BE)
            .TaggedFields();
    }
});

// -- ShareFetchRequest v1 --
Protocol.define('ShareFetchRequestV1', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ShareFetchRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactNullableString(data.groupId)
            .compactNullableString(data.memberId)
            .Int32BE(data.shareSessionEpoch)
            .Int32BE(data.maxWaitMs)
            .Int32BE(data.minBytes)
            .Int32BE(data.maxBytes || 0x7fffffff)
            .Int32BE(data.maxRecords !== undefined ? data.maxRecords : 5000)
            .Int32BE(data.batchSize !== undefined ? data.batchSize : 500)
            .compactArray(data.topics || [], this.ShareFetch_FetchTopicItem)
            .compactArray(data.forgottenTopicsData || [], this.ShareFetch_ForgottenTopicItem)
            .TaggedFields();
    }
});

// ========================
//  RESPONSE helpers
// ========================

Protocol.define('ShareFetch_LeaderIdAndEpoch', {
    read: function () {
        this
            .Int32BE('leaderId')
            .Int32BE('leaderEpoch')
            .TaggedFields();
    }
});

Protocol.define('ShareFetch_AcquiredRecordsItem', {
    read: function () {
        this
            .KafkaOffset('firstOffset')
            .KafkaOffset('lastOffset')
            .Int16BE('deliveryCount')
            .TaggedFields();
    }
});

Protocol.define('ShareFetch_PartitionData', {
    read: function () {
        var peekOffset;
        this
            .Int32BE('partitionIndex')
            .ErrorCode('error')
            .compactNullableString('errorMessage')
            .ErrorCode('acknowledgeError')
            .compactNullableString('acknowledgeErrorMessage')
            .ShareFetch_LeaderIdAndEpoch('currentLeader');

        // Records: compact nullable bytes containing RecordBatch data
        this.UVarint('messageSetSize');
        if (this.context.messageSetSize > 0) {
            this.context.messageSetSize = this.context.messageSetSize - 1; // compact encoding: N+1
        }
        if (this.context.messageSetSize > 0) {
            peekOffset = this.offset + 16;
            if (peekOffset < this.offset + this.context.messageSetSize && this.buffer[peekOffset] === 2) {
                this.RecordBatch('messageSet', this.context.messageSetSize);
            } else {
                this.MessageSet('messageSet', this.context.messageSetSize);
                if (this.context.messageSet.length === 1 && this.context.messageSet[0]._partial === true) {
                    this.context.messageSet = [];
                }
            }
        } else {
            this.context.messageSet = [];
        }

        this
            .compactArray('acquiredRecords', this.ShareFetch_AcquiredRecordsItem)
            .TaggedFields();
    }
});

Protocol.define('ShareFetch_TopicResponse', {
    read: function () {
        this
            .uuid('topicId')
            .compactArray('partitions', this.ShareFetch_PartitionData)
            .TaggedFields();
    }
});

Protocol.define('ShareFetch_NodeEndpoint', {
    read: function () {
        this
            .Int32BE('nodeId')
            .compactString('host')
            .Int32BE('port')
            .compactNullableString('rack')
            .TaggedFields();
    }
});

// -- ShareFetchResponse v1 --
Protocol.define('ShareFetchResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .compactNullableString('errorMessage')
            .Int32BE('acquisitionLockTimeoutMs')
            .compactArray('responses', this.ShareFetch_TopicResponse)
            .compactArray('nodeEndpoints', this.ShareFetch_NodeEndpoint)
            .TaggedFields();
    }
});
