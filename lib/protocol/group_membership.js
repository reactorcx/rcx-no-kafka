'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol


//////////////////////////
// GROUP MEMBERSHIP API //
//////////////////////////

Protocol.define('GroupCoordinatorRequest', {
    write: function (data) { // { groupId }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.GroupCoordinatorRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId);
    }
});

Protocol.define('GroupCoordinatorResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .Int32BE('coordinatorId')
            .string('coordinatorHost')
            .Int32BE('coordinatorPort');
    }
});

// FindCoordinator v1 (supports coordinatorType: 0=group, 1=transaction)
Protocol.define('FindCoordinatorRequest', {
    write: function (data) { // { key, coordinatorType }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.GroupCoordinatorRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.key)
            .Int8(data.coordinatorType || 0);
    }
});

Protocol.define('FindCoordinatorResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .string('errorMessage')
            .Int32BE('coordinatorId')
            .string('coordinatorHost')
            .Int32BE('coordinatorPort');
    }
});

// FindCoordinator v2 — same wire format as v1
Protocol.define('FindCoordinatorRequestV2', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.GroupCoordinatorRequest,
                apiVersion: 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.key)
            .Int8(data.coordinatorType || 0);
    }
});

// FindCoordinator v3 — first flexible version (KIP-482)
Protocol.define('FindCoordinatorRequestV3', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.GroupCoordinatorRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.key)
            .Int8(data.coordinatorType || 0)
            .TaggedFields();
    }
});

Protocol.define('FindCoordinatorResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .compactNullableString('errorMessage')
            .Int32BE('coordinatorId')
            .compactString('coordinatorHost')
            .Int32BE('coordinatorPort')
            .TaggedFields();
    }
});

// FindCoordinator v4 (KIP-699) — batch coordinator lookup
// v5 (KIP-890) and v6 (KIP-932) have same wire format, reuse via variable apiVersion
Protocol.define('FindCoordinatorRequestV4', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.GroupCoordinatorRequest,
                apiVersion: data.apiVersion || 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .Int8(data.keyType || 0)
            .compactArray(data.coordinatorKeys, this.compactString)
            .TaggedFields();
    }
});

Protocol.define('FindCoordinatorResponseV4_Coordinator', {
    read: function () {
        this
            .compactString('key')
            .Int32BE('nodeId')
            .compactString('host')
            .Int32BE('port')
            .ErrorCode('error')
            .compactNullableString('errorMessage')
            .TaggedFields();
    }
});

Protocol.define('FindCoordinatorResponseV4', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('coordinators', this.FindCoordinatorResponseV4_Coordinator)
            .TaggedFields();
    }
});

/* istanbul ignore next */
Protocol.define('JoinGroupRequest_GroupProtocolItem', {
    write: function (data) { // { name, metadata }
        this
            .string(data.name)
            .bytes(data.metadata);
    }
});

/* istanbul ignore next */
Protocol.define('JoinGroupRequest', {
    write: function (data) { // { groupId sessionTimeout memberId protocolType groupProtocols }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.JoinGroupRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.sessionTimeout)
            .string(data.memberId)
            .string(data.protocolType)
            .array(data.groupProtocols, this.JoinGroupRequest_GroupProtocolItem);
    }
});

// consumer protocol
Protocol.define('JoinConsumerGroupRequest_GroupProtocolItem', {
    write: function (data) { // { name, version, subscriptions, metadata, ownedPartitions }
        var _o1, _o2;
        this
            .string(data.name);
        _o1 = this.offset;
        this
            .skip(4) // following bytes length
            .Int16BE(data.version)
            .array(data.subscriptions, this.string)
            .bytes(data.metadata);
        if (data.version >= 1) {
            this.array(data.ownedPartitions || [], this.SyncConsumerGroupRequest_PartitionAssignment);
        }
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

Protocol.define('JoinConsumerGroupRequest', {
    write: function (data) { // { groupId sessionTimeout memberId groupProtocols }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.JoinGroupRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.sessionTimeout)
            .string(data.memberId)
            .string('consumer')
            .array(data.groupProtocols, this.JoinConsumerGroupRequest_GroupProtocolItem);
    }
});

// v1 adds rebalanceTimeout after sessionTimeout
Protocol.define('JoinConsumerGroupRequestV1', {
    write: function (data) { // { groupId sessionTimeout rebalanceTimeout memberId groupProtocols }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.JoinGroupRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.sessionTimeout)
            .Int32BE(data.rebalanceTimeout !== undefined ? data.rebalanceTimeout : data.sessionTimeout)
            .string(data.memberId)
            .string('consumer')
            .array(data.groupProtocols, this.JoinConsumerGroupRequest_GroupProtocolItem);
    }
});

/* istanbul ignore next */
Protocol.define('JoinGroupResponse_Member', {
    read: function () {
        this
            .string('id')
            .bytes('metadata');
    }
});

/* istanbul ignore next */
Protocol.define('JoinGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .Int32BE('generationId')
            .string('groupProtocol')
            .string('leaderId')
            .string('memberId')
            .array('members', this.JoinGroupResponse_Member);
    }
});

Protocol.define('JoinConsumerGroupResponse_Member', {
    read: function () {
        var _startOffset, _subLength;
        this.string('id');
        this.Int32BE('_subLength');
        _subLength = this.context._subLength;
        _startOffset = this.offset;
        this
            .Int16BE('version')
            .array('subscriptions', this.string)
            .bytes('metadata');
        if (this.context.version >= 1 && (this.offset - _startOffset) < _subLength) {
            this.array('ownedPartitions', this.SyncConsumerGroupRequest_PartitionAssignment);
        } else {
            this.context.ownedPartitions = [];
        }
    }
});

Protocol.define('JoinConsumerGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .Int32BE('generationId')
            .string('groupProtocol')
            .string('leaderId')
            .string('memberId')
            .array('members', this.JoinConsumerGroupResponse_Member);
    }
});

/* istanbul ignore next */
Protocol.define('SyncGroupRequest_GroupAssignment', {
    write: function (data) { // { memberId memberAssignment }
        this
            .string(data.memberId)
            .bytes(data.memberAssignment);
    }
});

/* istanbul ignore next */
Protocol.define('SyncGroupRequest', {
    write: function (data) { // { groupId generationId memberId groupAssignment }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.SyncGroupRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .array(data.groupAssignment, this.SyncGroupRequest_GroupAssignment);
    }
});

// consumer protocol
Protocol.define('SyncConsumerGroupRequest_PartitionAssignment', {
    write: function (data) { // { topic, partitions }
        this
            .string(data.topic)
            .array(data.partitions, this.Int32BE);
    },
    read: function () {
        this
            .string('topic')
            .array('partitions', this.Int32BE);
    }
});

Protocol.define('SyncConsumerGroupRequest_MemberAssignment', {
    write: function (data) { // { version partitionAssignment metadata }
        this
            .skip(4)
            .Int16BE(data.version)
            .array(data.partitionAssignment, this.SyncConsumerGroupRequest_PartitionAssignment)
            .bytes(data.metadata);
    },
    read: function () {
        this.Int32BE('_blength');
        if (this.context._blength <= 0) {
            return null;
        }
        this.Int16BE('version')
            .array('partitionAssignment', this.SyncConsumerGroupRequest_PartitionAssignment)
            .bytes('metadata');
        return undefined;
    }
});

Protocol.define('SyncConsumerGroupRequest_GroupAssignment', {
    write: function (data) { // { memberId memberAssignment}
        var _o1, _o2;
        this.string(data.memberId);
        _o1 = this.offset;
        this.SyncConsumerGroupRequest_MemberAssignment(data.memberAssignment);
        _o2 = this.offset;
        this.offset = _o1;
        this.Int32BE(_o2 - _o1 - 4);
        this.offset = _o2;
    }
});

Protocol.define('SyncConsumerGroupRequest', {
    write: function (data) { // { groupId generationId memberId groupAssignment }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.SyncGroupRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .array(data.groupAssignment, this.SyncConsumerGroupRequest_GroupAssignment);
    }
});

/* istanbul ignore next */
Protocol.define('SyncGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .bytes('memberAssignment');
    }
});

// consumer protocol
Protocol.define('SyncConsumerGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .SyncConsumerGroupRequest_MemberAssignment('memberAssignment');
    }
});

// v1 — same request wire format, response adds throttleTime
Protocol.define('SyncConsumerGroupRequestV1', {
    write: function (data) { // { groupId generationId memberId groupAssignment }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.SyncGroupRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .array(data.groupAssignment, this.SyncConsumerGroupRequest_GroupAssignment);
    }
});

Protocol.define('SyncConsumerGroupResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .SyncConsumerGroupRequest_MemberAssignment('memberAssignment');
    }
});

Protocol.define('HeartbeatRequest', {
    write: function (data) { // { groupId generationId memberId }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.HeartbeatRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId);
    }
});

Protocol.define('HeartbeatResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error');
    }
});

// v1 — same request wire format, response adds throttleTime
Protocol.define('HeartbeatRequestV1', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.HeartbeatRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId);
    }
});

Protocol.define('HeartbeatResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error');
    }
});

Protocol.define('LeaveGroupRequest', {
    write: function (data) { // { groupId memberId }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.LeaveGroupRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .string(data.memberId);
    }
});

Protocol.define('LeaveGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error');
    }
});

// v1 — same request wire format, response adds throttleTime
Protocol.define('LeaveGroupRequestV1', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.LeaveGroupRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .string(data.memberId);
    }
});

Protocol.define('LeaveGroupResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error');
    }
});

// v2 — same wire format as v1 for Heartbeat, LeaveGroup, SyncGroup
Protocol.define('HeartbeatRequestV2', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.HeartbeatRequest,
                apiVersion: 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId);
    }
});

Protocol.define('LeaveGroupRequestV2', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.LeaveGroupRequest,
                apiVersion: 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .string(data.memberId);
    }
});

// JoinGroup v3 — same request wire format as v1, response adds throttleTime
Protocol.define('JoinConsumerGroupRequestV3', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.JoinGroupRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.sessionTimeout)
            .Int32BE(data.rebalanceTimeout !== undefined ? data.rebalanceTimeout : data.sessionTimeout)
            .string(data.memberId)
            .string('consumer')
            .array(data.groupProtocols, this.JoinConsumerGroupRequest_GroupProtocolItem);
    }
});

Protocol.define('JoinConsumerGroupResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .Int32BE('generationId')
            .string('groupProtocol')
            .string('leaderId')
            .string('memberId')
            .array('members', this.JoinConsumerGroupResponse_Member);
    }
});

Protocol.define('SyncConsumerGroupRequestV2', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.SyncGroupRequest,
                apiVersion: 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .array(data.groupAssignment, this.SyncConsumerGroupRequest_GroupAssignment);
    }
});

// JoinGroup v5 (KIP-345: static membership) — adds groupInstanceId after memberId
Protocol.define('JoinConsumerGroupRequestV5', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.JoinGroupRequest,
                apiVersion: 5,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.sessionTimeout)
            .Int32BE(data.rebalanceTimeout !== undefined ? data.rebalanceTimeout : data.sessionTimeout)
            .string(data.memberId)
            .string(data.groupInstanceId || null)
            .string('consumer')
            .array(data.groupProtocols, this.JoinConsumerGroupRequest_GroupProtocolItem);
    }
});

Protocol.define('JoinConsumerGroupResponse_MemberV5', {
    read: function () {
        var _startOffset, _subLength;
        this
            .string('id')
            .string('groupInstanceId');
        this.Int32BE('_subLength');
        _subLength = this.context._subLength;
        _startOffset = this.offset;
        this
            .Int16BE('version')
            .array('subscriptions', this.string)
            .bytes('metadata');
        if (this.context.version >= 1 && (this.offset - _startOffset) < _subLength) {
            this.array('ownedPartitions', this.SyncConsumerGroupRequest_PartitionAssignment);
        } else {
            this.context.ownedPartitions = [];
        }
    }
});

Protocol.define('JoinConsumerGroupResponseV5', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .Int32BE('generationId')
            .string('groupProtocol')
            .string('leaderId')
            .string('memberId')
            .array('members', this.JoinConsumerGroupResponse_MemberV5);
    }
});

// JoinGroup v6 — first flexible version (KIP-482)
Protocol.define('JoinConsumerGroupRequestV6_GroupProtocolItem', {
    write: function (data) {
        var _bodyStart, _bodyEnd, bodyBuf, bodyLen;
        this.compactString(data.name);
        // serialize inner subscription metadata using old encoding to temp area
        _bodyStart = this.offset;
        this
            .Int16BE(data.version)
            .array(data.subscriptions, this.string)
            .bytes(data.metadata);
        if (data.version >= 1) {
            this.array(data.ownedPartitions || [], this.SyncConsumerGroupRequest_PartitionAssignment);
        }
        _bodyEnd = this.offset;
        bodyLen = _bodyEnd - _bodyStart;
        bodyBuf = new Buffer(bodyLen);
        this.buffer.copy(bodyBuf, 0, _bodyStart, _bodyEnd);
        // rewind and write as compactBytes
        this.offset = _bodyStart;
        this.compactBytes(bodyBuf);
        this.TaggedFields();
    }
});

Protocol.define('JoinConsumerGroupRequestV6', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.JoinGroupRequest,
                apiVersion: 6,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.groupId)
            .Int32BE(data.sessionTimeout)
            .Int32BE(data.rebalanceTimeout !== undefined ? data.rebalanceTimeout : data.sessionTimeout)
            .compactString(data.memberId)
            .compactNullableString(data.groupInstanceId || null)
            .compactString('consumer')
            .compactArray(data.groupProtocols, this.JoinConsumerGroupRequestV6_GroupProtocolItem)
            .TaggedFields();
    }
});

Protocol.define('JoinConsumerGroupResponse_MemberV6', {
    read: function () {
        var _startOffset, n;
        this
            .compactString('id')
            .compactNullableString('groupInstanceId');
        // metadata: compactBytes framing, inner uses old consumer protocol
        this.UVarint('_subLength');
        if (this.context._subLength > 0) {
            n = this.context._subLength - 1;
            _startOffset = this.offset;
            this
                .Int16BE('version')
                .array('subscriptions', this.string)
                .bytes('metadata');
            if (this.context.version >= 1 && (this.offset - _startOffset) < n) {
                this.array('ownedPartitions', this.SyncConsumerGroupRequest_PartitionAssignment);
            } else {
                this.context.ownedPartitions = [];
            }
        }
        this.TaggedFields();
    }
});

Protocol.define('JoinConsumerGroupResponseV6', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .Int32BE('generationId')
            .compactString('groupProtocol')
            .compactString('leaderId')
            .compactString('memberId')
            .compactArray('members', this.JoinConsumerGroupResponse_MemberV6)
            .TaggedFields();
    }
});

// Heartbeat v3 (KIP-345) — adds groupInstanceId after memberId
Protocol.define('HeartbeatRequestV3', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.HeartbeatRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .string(data.groupInstanceId || null);
    }
});

// SyncGroup v4 — first flexible version (KIP-482)
Protocol.define('SyncConsumerGroupRequestV4_GroupAssignment', {
    write: function (data) {
        var _bodyStart, _bodyEnd, bodyBuf, bodyLen;
        this.compactString(data.memberId);
        if (!data.memberAssignment ||
            (data.memberAssignment.version === undefined && !data.memberAssignment.partitionAssignment)) {
            this.compactBytes(null);
        } else {
            // serialize inner assignment body using old encoding to temp area
            _bodyStart = this.offset;
            this
                .Int16BE(data.memberAssignment.version)
                .array(data.memberAssignment.partitionAssignment, this.SyncConsumerGroupRequest_PartitionAssignment)
                .bytes(data.memberAssignment.metadata);
            _bodyEnd = this.offset;
            bodyLen = _bodyEnd - _bodyStart;
            bodyBuf = new Buffer(bodyLen);
            this.buffer.copy(bodyBuf, 0, _bodyStart, _bodyEnd);
            // rewind and write as compactBytes
            this.offset = _bodyStart;
            this.compactBytes(bodyBuf);
        }
        this.TaggedFields();
    }
});

Protocol.define('SyncConsumerGroupRequestV4', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.SyncGroupRequest,
                apiVersion: 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.groupId)
            .Int32BE(data.generationId)
            .compactString(data.memberId)
            .compactNullableString(data.groupInstanceId || null)
            .compactArray(data.groupAssignment, this.SyncConsumerGroupRequestV4_GroupAssignment)
            .TaggedFields();
    }
});

Protocol.define('SyncConsumerGroupResponseV4_MemberAssignment', {
    read: function () {
        var n;
        this.UVarint('_blength');
        if (this.context._blength <= 0) {
            return null;
        }
        n = this.context._blength - 1;
        if (n <= 0) {
            return null;
        }
        this.Int16BE('version')
            .array('partitionAssignment', this.SyncConsumerGroupRequest_PartitionAssignment)
            .bytes('metadata');
        return undefined;
    }
});

Protocol.define('SyncConsumerGroupResponseV4', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .SyncConsumerGroupResponseV4_MemberAssignment('memberAssignment')
            .TaggedFields();
    }
});

// Heartbeat v4 — first flexible version (KIP-482)
Protocol.define('HeartbeatRequestV4', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.HeartbeatRequest,
                apiVersion: 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.groupId)
            .Int32BE(data.generationId)
            .compactString(data.memberId)
            .compactNullableString(data.groupInstanceId || null)
            .TaggedFields();
    }
});

Protocol.define('HeartbeatResponseV4', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .TaggedFields();
    }
});

// SyncGroup v3 (KIP-345) — adds groupInstanceId after memberId
Protocol.define('SyncConsumerGroupRequestV3', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.SyncGroupRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .Int32BE(data.generationId)
            .string(data.memberId)
            .string(data.groupInstanceId || null)
            .array(data.groupAssignment, this.SyncConsumerGroupRequest_GroupAssignment);
    }
});

// LeaveGroup v3 (KIP-345) — replaces single memberId with members array
Protocol.define('LeaveGroupRequestV3_MemberItem', {
    write: function (data) { // { memberId, groupInstanceId }
        this
            .string(data.memberId || '') // member_id is non-nullable STRING
            .string(data.groupInstanceId || null);
    }
});

Protocol.define('LeaveGroupRequestV3', {
    write: function (data) { // { groupId, members }
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.LeaveGroupRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .string(data.groupId)
            .array(data.members, this.LeaveGroupRequestV3_MemberItem);
    }
});

// LeaveGroup v4 — first flexible version (KIP-482)
Protocol.define('LeaveGroupRequestV4_MemberItem', {
    write: function (data) {
        this
            .compactString(data.memberId || '')
            .compactNullableString(data.groupInstanceId || null)
            .TaggedFields();
    }
});

Protocol.define('LeaveGroupRequestV4', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.LeaveGroupRequest,
                apiVersion: 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactString(data.groupId)
            .compactArray(data.members, this.LeaveGroupRequestV4_MemberItem)
            .TaggedFields();
    }
});

Protocol.define('LeaveGroupResponseV4_MemberItem', {
    read: function () {
        this
            .compactString('memberId')
            .compactNullableString('groupInstanceId')
            .ErrorCode('error')
            .TaggedFields();
    }
});

Protocol.define('LeaveGroupResponseV4', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .compactArray('members', this.LeaveGroupResponseV4_MemberItem)
            .TaggedFields();
    }
});

Protocol.define('LeaveGroupResponseV3_MemberItem', {
    read: function () {
        this
            .string('memberId')
            .string('groupInstanceId')
            .ErrorCode('error');
    }
});

Protocol.define('LeaveGroupResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .array('members', this.LeaveGroupResponseV3_MemberItem);
    }
});
