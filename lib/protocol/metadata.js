'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol


//////////////////
// METADATA API //
//////////////////

Protocol.define('MetadataRequest', {
    write: function (data) { // data: { correlationId, clientId, [topicNames] }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.topicNames, this.string);
    }
});

Protocol.define('Broker', {
    read: function () {
        this
            .Int32BE('nodeId')
            .string('host')
            .Int32BE('port');
    }
});

Protocol.define('PartitionMetadata', {
    read: function () {
        this
            .ErrorCode('error')
            .Int32BE('partitionId')
            .Int32BE('leader')
            .array('replicas', this.Int32BE) // The set of alive nodes that currently acts as slaves for the leader for this partition
            .array('isr', this.Int32BE); // The set subset of the replicas that are "caught up" to the leader
    }
});

Protocol.define('TopicMetadata', {
    read: function () {
        this
            .ErrorCode('error')
            .string('topicName')
            .array('partitionMetadata', this.PartitionMetadata);
    }
});

Protocol.define('MetadataResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('broker', this.Broker)
            .array('topicMetadata', this.TopicMetadata);
    }
});

////////////////////
// METADATA v1    //
////////////////////

Protocol.define('MetadataRequestV1', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.topicNames, this.string);
    }
});

Protocol.define('BrokerV1', {
    read: function () {
        this
            .Int32BE('nodeId')
            .string('host')
            .Int32BE('port')
            .string('rack');
    }
});

Protocol.define('TopicMetadataV1', {
    read: function () {
        this
            .ErrorCode('error')
            .string('topicName')
            .Int8('isInternal')
            .array('partitionMetadata', this.PartitionMetadata);
    }
});

Protocol.define('MetadataResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('broker', this.BrokerV1)
            .Int32BE('controllerId')
            .array('topicMetadata', this.TopicMetadataV1);
    }
});

////////////////////
// METADATA v5    //
////////////////////

Protocol.define('MetadataRequestV5', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 5,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.topicNames, this.string)
            .Int8(data.allowAutoTopicCreation !== undefined ? data.allowAutoTopicCreation : 1);
    }
});

Protocol.define('PartitionMetadataV5', {
    read: function () {
        this
            .ErrorCode('error')
            .Int32BE('partitionId')
            .Int32BE('leader')
            .array('replicas', this.Int32BE)
            .array('isr', this.Int32BE)
            .array('offlineReplicas', this.Int32BE);
    }
});

Protocol.define('TopicMetadataV5', {
    read: function () {
        this
            .ErrorCode('error')
            .string('topicName')
            .Int8('isInternal')
            .array('partitionMetadata', this.PartitionMetadataV5);
    }
});

Protocol.define('MetadataResponseV5', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('broker', this.BrokerV1)
            .string('clusterId')
            .Int32BE('controllerId')
            .array('topicMetadata', this.TopicMetadataV5);
    }
});

////////////////////
// METADATA v7    //
////////////////////

Protocol.define('MetadataRequestV7', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 7,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.topicNames, this.string)
            .Int8(data.allowAutoTopicCreation !== undefined ? data.allowAutoTopicCreation : 1);
    }
});

Protocol.define('PartitionMetadataV7', {
    read: function () {
        this
            .ErrorCode('error')
            .Int32BE('partitionId')
            .Int32BE('leader')
            .Int32BE('leaderEpoch')
            .array('replicas', this.Int32BE)
            .array('isr', this.Int32BE)
            .array('offlineReplicas', this.Int32BE);
    }
});

Protocol.define('TopicMetadataV7', {
    read: function () {
        this
            .ErrorCode('error')
            .string('topicName')
            .Int8('isInternal')
            .array('partitionMetadata', this.PartitionMetadataV7);
    }
});

Protocol.define('MetadataResponseV7', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('broker', this.BrokerV1)
            .string('clusterId')
            .Int32BE('controllerId')
            .array('topicMetadata', this.TopicMetadataV7);
    }
});

////////////////////
// METADATA v8    //
////////////////////

// v8 (KIP-430) — adds includeCluster/TopicAuthorizedOperations to request, authorizedOperations to response
Protocol.define('MetadataRequestV8', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 8,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.topicNames, this.string)
            .Int8(data.allowAutoTopicCreation !== undefined ? data.allowAutoTopicCreation : 1)
            .Int8(data.includeClusterAuthorizedOperations || 0)
            .Int8(data.includeTopicAuthorizedOperations || 0);
    }
});

Protocol.define('TopicMetadataV8', {
    read: function () {
        this
            .ErrorCode('error')
            .string('topicName')
            .Int8('isInternal')
            .array('partitionMetadata', this.PartitionMetadataV7)
            .Int32BE('topicAuthorizedOperations');
    }
});

Protocol.define('MetadataResponseV8', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('broker', this.BrokerV1)
            .string('clusterId')
            .Int32BE('controllerId')
            .array('topicMetadata', this.TopicMetadataV8)
            .Int32BE('clusterAuthorizedOperations');
    }
});

////////////////////
// METADATA v9    //
////////////////////

// v9 — first flexible version (KIP-482)
Protocol.define('MetadataRequestV9_TopicItem', {
    write: function (data) {
        this
            .compactString(data)
            .TaggedFields();
    }
});

Protocol.define('MetadataRequestV9', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 9,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactArray(data.topicNames, this.MetadataRequestV9_TopicItem)
            .Int8(data.allowAutoTopicCreation !== undefined ? data.allowAutoTopicCreation : 1)
            .Int8(data.includeClusterAuthorizedOperations || 0)
            .Int8(data.includeTopicAuthorizedOperations || 0)
            .TaggedFields();
    }
});

Protocol.define('BrokerV9', {
    read: function () {
        this
            .Int32BE('nodeId')
            .compactString('host')
            .Int32BE('port')
            .compactNullableString('rack')
            .TaggedFields();
    }
});

Protocol.define('PartitionMetadataV9', {
    read: function () {
        this
            .ErrorCode('error')
            .Int32BE('partitionId')
            .Int32BE('leader')
            .Int32BE('leaderEpoch')
            .compactArray('replicas', this.Int32BE)
            .compactArray('isr', this.Int32BE)
            .compactArray('offlineReplicas', this.Int32BE)
            .TaggedFields();
    }
});

Protocol.define('TopicMetadataV9', {
    read: function () {
        this
            .ErrorCode('error')
            .compactString('topicName')
            .Int8('isInternal')
            .compactArray('partitionMetadata', this.PartitionMetadataV9)
            .Int32BE('topicAuthorizedOperations')
            .TaggedFields();
    }
});

Protocol.define('MetadataResponseV9', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('broker', this.BrokerV9)
            .compactString('clusterId')
            .Int32BE('controllerId')
            .compactArray('topicMetadata', this.TopicMetadataV9)
            .Int32BE('clusterAuthorizedOperations')
            .TaggedFields();
    }
});

////////////////////
// METADATA v10   //
////////////////////

// v10 (KIP-516) — topic items in request have {topicId, name}, response adds topicId per topic
Protocol.define('MetadataRequestV10_TopicItem', {
    write: function (data) {
        this
            .uuid(data.topicId || null)
            .compactNullableString(data.name !== undefined ? data.name : data)
            .TaggedFields();
    }
});

Protocol.define('MetadataRequestV10', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 10,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactArray(data.topicNames, this.MetadataRequestV10_TopicItem)
            .Int8(data.allowAutoTopicCreation !== undefined ? data.allowAutoTopicCreation : 1)
            .Int8(data.includeClusterAuthorizedOperations || 0)
            .Int8(data.includeTopicAuthorizedOperations || 0)
            .TaggedFields();
    }
});

Protocol.define('TopicMetadataV10', {
    read: function () {
        this
            .ErrorCode('error')
            .compactString('topicName')
            .uuid('topicId')
            .Int8('isInternal')
            .compactArray('partitionMetadata', this.PartitionMetadataV9)
            .Int32BE('topicAuthorizedOperations')
            .TaggedFields();
    }
});

Protocol.define('MetadataResponseV10', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('broker', this.BrokerV9)
            .compactString('clusterId')
            .Int32BE('controllerId')
            .compactArray('topicMetadata', this.TopicMetadataV10)
            .Int32BE('clusterAuthorizedOperations')
            .TaggedFields();
    }
});

////////////////////
// METADATA v11   //
////////////////////

// v11 — drops includeClusterAuthorizedOperations from request and clusterAuthorizedOperations from response (KIP-700)
Protocol.define('MetadataRequestV11', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 11,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactArray(data.topicNames, this.MetadataRequestV10_TopicItem)
            .Int8(data.allowAutoTopicCreation !== undefined ? data.allowAutoTopicCreation : 1)
            .Int8(data.includeTopicAuthorizedOperations || 0)
            .TaggedFields();
    }
});

Protocol.define('MetadataResponseV11', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('broker', this.BrokerV9)
            .compactString('clusterId')
            .Int32BE('controllerId')
            .compactArray('topicMetadata', this.TopicMetadataV10)
            .TaggedFields();
    }
});

////////////////////
// METADATA v12   //
////////////////////

// v12 — topicName becomes nullable in response (KIP-516 topic ID support fully enabled)
Protocol.define('MetadataRequestV12', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 12,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactArray(data.topicNames, this.MetadataRequestV10_TopicItem)
            .Int8(data.allowAutoTopicCreation !== undefined ? data.allowAutoTopicCreation : 1)
            .Int8(data.includeTopicAuthorizedOperations || 0)
            .TaggedFields();
    }
});

Protocol.define('TopicMetadataV12', {
    read: function () {
        this
            .ErrorCode('error')
            .compactNullableString('topicName')
            .uuid('topicId')
            .Int8('isInternal')
            .compactArray('partitionMetadata', this.PartitionMetadataV9)
            .Int32BE('topicAuthorizedOperations')
            .TaggedFields();
    }
});

Protocol.define('MetadataResponseV12', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('broker', this.BrokerV9)
            .compactString('clusterId')
            .Int32BE('controllerId')
            .compactArray('topicMetadata', this.TopicMetadataV12)
            .TaggedFields();
    }
});

////////////////////
// METADATA v13   //
////////////////////

// v13 — response adds top-level ErrorCode before broker array; request same as v12
Protocol.define('MetadataRequestV13', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.MetadataRequest,
                apiVersion: 13,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactArray(data.topicNames, this.MetadataRequestV10_TopicItem)
            .Int8(data.allowAutoTopicCreation !== undefined ? data.allowAutoTopicCreation : 1)
            .Int8(data.includeTopicAuthorizedOperations || 0)
            .TaggedFields();
    }
});

// v13 — wire-identical response to v12 (no top-level ErrorCode; plan was incorrect)
Protocol.define('MetadataResponseV13', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('broker', this.BrokerV9)
            .compactString('clusterId')
            .Int32BE('controllerId')
            .compactArray('topicMetadata', this.TopicMetadataV12)
            .TaggedFields();
    }
});
