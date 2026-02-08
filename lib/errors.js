'use strict';

var _ = require('lodash');

/* eslint max-len: [2, 350, 4] */

var errors = [
    ['Unknown', -1, 'An unexpected server error'],
    ['OffsetOutOfRange', 1, 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition.'],
    ['InvalidMessage', 2, 'This indicates that a message contents does not match its CRC'],
    ['UnknownTopicOrPartition', 3, 'This request is for a topic or partition that does not exist on this broker.'],
    ['InvalidMessageSize', 4, 'The message has a negative size'],
    ['LeaderNotAvailable', 5, 'This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.'],
    ['NotLeaderForPartition', 6, 'This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.'],
    ['RequestTimedOut', 7, 'This error is thrown if the request exceeds the user-specified time limit in the request.'],
    ['BrokerNotAvailable', 8, 'This is not a client facing error and is used mostly by tools when a broker is not alive.'],
    ['ReplicaNotAvailable', 9, 'If replica is expected on a broker, but is not (this can be safely ignored).'],
    ['MessageSizeTooLarge', 10, 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.'],
    ['StaleControllerEpoch', 11, 'Internal error code for broker-to-broker communication.'],
    ['OffsetMetadataTooLarge', 12, 'If you specify a string larger than configured maximum for offset metadata'],
    ['StaleControllerEpochCode', 13, 'The controller moved to another broker.'],
    ['GroupLoadInProgress', 14, 'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition), or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator.'],
    ['GroupCoordinatorNotAvailable', 15, 'The broker returns this error code for group coordinator requests, offset commits, and most group management requests if the offsets topic has not yet been created, or if the group coordinator is not active.'],
    ['NotCoordinatorForGroup', 16, 'The broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for.'],
    ['InvalidTopic', 17, 'For a request which attempts to access an invalid topic (e.g. one which has an illegal name), or if an attempt is made to write to an internal topic (such as the consumer offsets topic).'],
    ['RecordListTooLarge', 18, 'If a message batch in a produce request exceeds the maximum configured segment size.'],
    ['NotEnoughReplicas', 19, 'Returned from a produce request when the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1.'],
    ['NotEnoughReplicasAfterAppend', 20, 'Returned from a produce request when the message was written to the log, but with fewer in-sync replicas than required.'],
    ['InvalidRequiredAcks', 21, 'Returned from a produce request if the requested requiredAcks is invalid (anything other than -1, 1, or 0).'],
    ['IllegalGeneration', 22, 'Returned from group membership requests (such as heartbeats) when the generation id provided in the request is not the current generation.'],
    ['InconsistentGroupProtocol', 23, 'Returned in join group when the member provides a protocol type or set of protocols which is not compatible with the current group.'],
    ['InvalidGroupId', 24, 'Returned in join group when the groupId is empty or null.'],
    ['UnknownMemberId', 25, 'Returned from group requests (offset commits/fetches, heartbeats, etc) when the memberId is not in the current generation.'],
    ['InvalidSessionTimeout', 26, 'Returned in join group when the requested session timeout is outside of the allowed range on the broker'],
    ['RebalanceInProgress', 27, 'Returned in heartbeat requests when the coordinator has begun rebalancing the group. This indicates to the client that it should rejoin the group.'],
    ['InvalidCommitOffsetSize', 28, 'This error indicates that an offset commit was rejected because of oversize metadata.'],
    ['TopicAuthorizationFailed', 29, 'Returned by the broker when the client is not authorized to access the requested topic.'],
    ['GroupAuthorizationFailed', 30, 'Returned by the broker when the client is not authorized to access a particular groupId.'],
    ['ClusterAuthorizationFailed', 31, 'Returned by the broker when the client is not authorized to use an inter-broker or administrative API.'],
    ['UnsupportedSaslMechanism', 33, 'The broker does not support the requested SASL mechanism.'],
    ['IllegalSaslState', 34, 'Request is not valid given the current SASL state.'],
    ['UnsupportedVersion', 35, 'The version of API is not supported.'],
    ['TopicAlreadyExists', 36, 'Topic with this name already exists.'],
    ['InvalidPartitions', 37, 'Number of partitions is invalid.'],
    ['InvalidReplicationFactor', 38, 'Replication-factor is invalid.'],
    ['InvalidReplicaAssignment', 39, 'Replica assignment is invalid.'],
    ['InvalidConfig', 40, 'Configuration is invalid.'],
    ['NotController', 41, 'This is not the correct controller for this cluster.'],
    ['InvalidRequest', 42, 'This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker.'],
    ['UnsupportedForMessageFormat', 43, 'The message format version on the broker does not support the request.'],
    ['PolicyViolation', 44, 'Request parameters do not satisfy the configured policy.'],
    ['OutOfOrderSequenceNumber', 45, 'The broker received an out of order sequence number.'],
    ['DuplicateSequenceNumber', 46, 'The broker received a duplicate sequence number.'],
    ['InvalidProducerEpoch', 47, 'Producer attempted an operation with an old epoch.'],
    ['InvalidTxnState', 48, 'The producer attempted a transactional operation in an invalid state.'],
    ['InvalidProducerIdMapping', 49, 'The producer attempted to use a producer id which is not currently assigned to its transactional id.'],
    ['InvalidTransactionTimeout', 50, 'The transaction timeout is larger than the maximum value allowed by the broker.'],
    ['ConcurrentTransactions', 51, 'The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.'],
    ['TransactionalIdAuthorizationFailed', 53, 'Transactional Id authorization failed.'],
    ['ProducerFenced', 67, 'The producer has been rejected because it tried to use a producer id that is currently assigned to another transactional producer.']
];

function KafkaError(code, message) {
    // Error.captureStackTrace(this, this.constructor);

    this.name = this.constructor.name;
    this.code = code;
    this.message = message || 'Error';
}


exports.KafkaError = KafkaError;

KafkaError.prototype = Object.create(Error.prototype);
KafkaError.prototype.constructor = KafkaError;

KafkaError.prototype.toJSON = function () {
    return {
        name: this.name,
        code: this.code,
        message: this.message
    };
};

KafkaError.prototype.toString = function () {
    return this.name + ': ' + this.code + ': ' + this.message;
};


exports.byCode = function (code) {
    var error;

    if (code === 0) {
        return null; // no error
    }

    error = _.find(errors, function (e) {
        return e[1] === code;
    });

    if (error === undefined) {
        return new Error('Unknown error code: ' + code);
    }

    return new KafkaError(error[0], error[2]);
};

exports.byName = function (name) {
    var error = _.find(errors, function (e) {
        return e[0] === name;
    });

    if (error === undefined) {
        return null; // no error
    }

    return new KafkaError(error[0], error[2]);
};

function NoKafkaConnectionError(server, message) {
    // Error.captureStackTrace(this, this.constructor);

    this.name = this.constructor.name;
    this.server = server || 'none';
    this.message = message || 'Error';
}


NoKafkaConnectionError.prototype = Object.create(Error.prototype);
NoKafkaConnectionError.prototype.constructor = NoKafkaConnectionError;

NoKafkaConnectionError.prototype.toJSON = function () {
    return {
        name: this.name,
        server: this.server,
        message: this.message
    };
};

NoKafkaConnectionError.prototype.toString = function () {
    return this.name + ' [' + this.server + ']: ' + this.message;
};

exports.NoKafkaConnectionError = NoKafkaConnectionError;
