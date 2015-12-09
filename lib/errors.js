"use strict";

var _ = require('lodash');

/* jshint maxlen: 350 */

var KafkaError = function (code, message) {

    Error.call(this);
    Error.captureStackTrace(this, this.constructor);

    this.name = 'KafkaError';
    this.code = code;
    this.message = message || 'Unknown Error';
};

KafkaError.prototype = Object.create(Error.prototype);
KafkaError.prototype.constructor = KafkaError;

KafkaError.prototype.toJSON = function () {
    return {
        name: this.code,
        message: this.message
    };
};

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
    ['StaleControllerEpochCode', 11, 'Internal error code for broker-to-broker communication.'],
    ['OffsetMetadataTooLargeCode', 12, 'If you specify a string larger than configured maximum for offset metadata'],
    ['GroupLoadInProgressCode', 14,  'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition), or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator.'],
    ['GroupCoordinatorNotAvailableCode',    15,  'The broker returns this error code for group coordinator requests, offset commits, and most group management requests if the offsets topic has not yet been created, or if the group coordinator is not active.'],
    ['NotCoordinatorForGroupCode',  16,  'The broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for.'],
    ['InvalidTopicCode',    17,  'For a request which attempts to access an invalid topic (e.g. one which has an illegal name), or if an attempt is made to write to an internal topic (such as the consumer offsets topic).'],
    ['RecordListTooLargeCode',  18,  'If a message batch in a produce request exceeds the maximum configured segment size.'],
    ['NotEnoughReplicasCode',   19,  'Returned from a produce request when the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1.'],
    ['NotEnoughReplicasAfterAppendCode',    20,  'Returned from a produce request when the message was written to the log, but with fewer in-sync replicas than required.'],
    ['InvalidRequiredAcksCode', 21,  'Returned from a produce request if the requested requiredAcks is invalid (anything other than -1, 1, or 0).'],
    ['IllegalGenerationCode',   22,  'Returned from group membership requests (such as heartbeats) when the generation id provided in the request is not the current generation.'],
    ['InconsistentGroupProtocolCode',   23,  'Returned in join group when the member provides a protocol type or set of protocols which is not compatible with the current group.'],
    ['InvalidGroupIdCode',  24,  'Returned in join group when the groupId is empty or null.'],
    ['UnknownMemberIdCode', 25,  'Returned from group requests (offset commits/fetches, heartbeats, etc) when the memberId is not in the current generation.'],
    ['InvalidSessionTimeoutCode',   26,  'Return in join group when the requested session timeout is outside of the allowed range on the broker'],
    ['RebalanceInProgressCode', 27,  'Returned in heartbeat requests when the coordinator has begun rebalancing the group. This indicates to the client that it should rejoin the group.'],
    ['InvalidCommitOffsetSizeCode', 28,  'This error indicates that an offset commit was rejected because of oversize metadata.'],
    ['TopicAuthorizationFailedCode',    29,  'Returned by the broker when the client is not authorized to access the requested topic.'],
    ['GroupAuthorizationFailedCode',    30,  'Returned by the broker when the client is not authorized to access a particular groupId.'],
    ['ClusterAuthorizationFailedCode',  31,  'Returned by the broker when the client is not authorized to use an inter-broker or administrative API.']
];

exports.byCode = function (code) {
    if(code === 0){
        return null; // no error
    }

    var error = _.find(errors, function (e) {
        return e[1] === code;
    });

    return new KafkaError(error[0], error[2]);
};