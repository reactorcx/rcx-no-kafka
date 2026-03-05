'use strict';

var Protocol = require('./index');
var globals  = require('./globals');

Protocol.define('ListGroupsRequest', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ListGroupsRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            });
    }
});

Protocol.define('ListGroupResponse_GroupItem', {
    read: function () {
        this
            .string('groupId')
            .string('protocolType');
    }
});

Protocol.define('ListGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .ErrorCode('error')
            .array('groups', this.ListGroupResponse_GroupItem);
    }
});

// v1 — same request wire format, response adds throttleTime
Protocol.define('ListGroupsRequestV1', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ListGroupsRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            });
    }
});

Protocol.define('ListGroupResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .array('groups', this.ListGroupResponse_GroupItem);
    }
});

// ListGroups v3 — first flexible version (KIP-482)
Protocol.define('ListGroupsRequestV3', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ListGroupsRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .TaggedFields();
    }
});

Protocol.define('ListGroupResponseV3_GroupItem', {
    read: function () {
        this
            .compactString('groupId')
            .compactString('protocolType')
            .TaggedFields();
    }
});

Protocol.define('ListGroupResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .compactArray('groups', this.ListGroupResponseV3_GroupItem)
            .TaggedFields();
    }
});

// ListGroups v4 (KIP-518) — adds StatesFilter to request, groupState to response
Protocol.define('ListGroupsRequestV4', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ListGroupsRequest,
                apiVersion: 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactArray(data.statesFilter || [], this.compactString)
            .TaggedFields();
    }
});

Protocol.define('ListGroupResponseV4_GroupItem', {
    read: function () {
        this
            .compactString('groupId')
            .compactString('protocolType')
            .compactString('groupState')
            .TaggedFields();
    }
});

Protocol.define('ListGroupResponseV4', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .compactArray('groups', this.ListGroupResponseV4_GroupItem)
            .TaggedFields();
    }
});

// ListGroups v5 (KIP-848) — adds TypesFilter to request, groupType to response
Protocol.define('ListGroupsRequestV5', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.ListGroupsRequest,
                apiVersion: 5,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactArray(data.statesFilter || [], this.compactString)
            .compactArray(data.typesFilter || [], this.compactString)
            .TaggedFields();
    }
});

Protocol.define('ListGroupResponseV5_GroupItem', {
    read: function () {
        this
            .compactString('groupId')
            .compactString('protocolType')
            .compactString('groupState')
            .compactString('groupType')
            .TaggedFields();
    }
});

Protocol.define('ListGroupResponseV5', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .ErrorCode('error')
            .compactArray('groups', this.ListGroupResponseV5_GroupItem)
            .TaggedFields();
    }
});

// DescribeGroups v6 (KIP-1043) — wire-identical to v5
Protocol.define('DescribeGroupRequestV6', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.DescribeGroupsRequest,
                apiVersion: 6,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactArray(data.groups, this.compactString)
            .Int8(data.includeAuthorizedOperations || 0)
            .TaggedFields();
    }
});

Protocol.define('DescribeGroupRequest', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.DescribeGroupsRequest,
                apiVersion: 0,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.groups, this.string);
    }
});

Protocol.define('DescribeGroupResponse_ConsumerGroupMemberItem', {
    read: function () {
        this
            .string('memberId')
            .string('clientId')
            .string('clientHost')
            .skip(4) // metadata bytes length
            .Int16BE('version')
            .array('subscriptions', this.string)
            .bytes('metadata')
            .SyncConsumerGroupRequest_MemberAssignment('memberAssignment');
    }
});

/* istanbul ignore next */
Protocol.define('DescribeGroupResponse_GroupMemberItem', {
    read: function () {
        this
            .string('memberId')
            .string('clientId')
            .string('clientHost')
            .bytes('memberMetadata')
            .bytes('memberAssignment');
    }
});

Protocol.define('DescribeGroupResponse_GroupItem', {
    read: function () {
        this
            .ErrorCode('error')
            .string('groupId')
            .string('state')
            .string('protocolType')
            .string('protocol');
        if (this.context.protocolType === 'consumer') {
            this.array('members', this.DescribeGroupResponse_ConsumerGroupMemberItem);
        } else {
            /* istanbul ignore next */
            this.array('members', this.DescribeGroupResponse_GroupMemberItem);
        }
    }
});

Protocol.define('DescribeGroupResponse', {
    read: function () {
        this
            .Int32BE('correlationId')
            .array('groups', this.DescribeGroupResponse_GroupItem);
    }
});

// v1 — same request wire format, response adds throttleTime
Protocol.define('DescribeGroupRequestV1', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.DescribeGroupsRequest,
                apiVersion: 1,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.groups, this.string);
    }
});

Protocol.define('DescribeGroupResponseV1', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('groups', this.DescribeGroupResponse_GroupItem);
    }
});

// v2 — same wire format as v1 for DescribeGroups, ListGroups
Protocol.define('DescribeGroupRequestV2', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.DescribeGroupsRequest,
                apiVersion: 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.groups, this.string);
    }
});

Protocol.define('ListGroupsRequestV2', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.ListGroupsRequest,
                apiVersion: 2,
                correlationId: data.correlationId,
                clientId: data.clientId
            });
    }
});

// DescribeGroups v3 (KIP-430) — adds includeAuthorizedOperations to request, authorizedOperations to response
Protocol.define('DescribeGroupRequestV3', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.DescribeGroupsRequest,
                apiVersion: 3,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.groups, this.string)
            .Int8(data.includeAuthorizedOperations || 0);
    }
});

Protocol.define('DescribeGroupResponseV3_GroupItem', {
    read: function () {
        this
            .ErrorCode('error')
            .string('groupId')
            .string('state')
            .string('protocolType')
            .string('protocol');
        if (this.context.protocolType === 'consumer') {
            this.array('members', this.DescribeGroupResponse_ConsumerGroupMemberItem);
        } else {
            /* istanbul ignore next */
            this.array('members', this.DescribeGroupResponse_GroupMemberItem);
        }
        this.Int32BE('authorizedOperations');
    }
});

Protocol.define('DescribeGroupResponseV3', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('groups', this.DescribeGroupResponseV3_GroupItem);
    }
});

// DescribeGroups v4 (KIP-345) — adds groupInstanceId per member
Protocol.define('DescribeGroupRequestV4', {
    write: function (data) {
        this
            .RequestHeader({
                apiKey: globals.API_KEYS.DescribeGroupsRequest,
                apiVersion: 4,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .array(data.groups, this.string)
            .Int8(data.includeAuthorizedOperations || 0);
    }
});

Protocol.define('DescribeGroupResponse_ConsumerGroupMemberItemV4', {
    read: function () {
        this
            .string('memberId')
            .string('groupInstanceId')
            .string('clientId')
            .string('clientHost')
            .skip(4) // metadata bytes length
            .Int16BE('version')
            .array('subscriptions', this.string)
            .bytes('metadata')
            .SyncConsumerGroupRequest_MemberAssignment('memberAssignment');
    }
});

/* istanbul ignore next */
Protocol.define('DescribeGroupResponse_GroupMemberItemV4', {
    read: function () {
        this
            .string('memberId')
            .string('groupInstanceId')
            .string('clientId')
            .string('clientHost')
            .bytes('memberMetadata')
            .bytes('memberAssignment');
    }
});

Protocol.define('DescribeGroupResponseV4_GroupItem', {
    read: function () {
        this
            .ErrorCode('error')
            .string('groupId')
            .string('state')
            .string('protocolType')
            .string('protocol');
        if (this.context.protocolType === 'consumer') {
            this.array('members', this.DescribeGroupResponse_ConsumerGroupMemberItemV4);
        } else {
            /* istanbul ignore next */
            this.array('members', this.DescribeGroupResponse_GroupMemberItemV4);
        }
        this.Int32BE('authorizedOperations');
    }
});

Protocol.define('DescribeGroupResponseV4', {
    read: function () {
        this
            .Int32BE('correlationId')
            .Int32BE('throttleTime')
            .array('groups', this.DescribeGroupResponseV4_GroupItem);
    }
});

// DescribeGroups v5 — first flexible version (KIP-482)
Protocol.define('DescribeGroupRequestV5', {
    write: function (data) {
        this
            .FlexibleRequestHeader({
                apiKey: globals.API_KEYS.DescribeGroupsRequest,
                apiVersion: 5,
                correlationId: data.correlationId,
                clientId: data.clientId
            })
            .compactArray(data.groups, this.compactString)
            .Int8(data.includeAuthorizedOperations || 0)
            .TaggedFields();
    }
});

Protocol.define('DescribeGroupResponseV5_ConsumerGroupMemberItem', {
    read: function () {
        this
            .compactString('memberId')
            .compactNullableString('groupInstanceId')
            .compactString('clientId')
            .compactString('clientHost');
        // subscription metadata: compactBytes framing, inner uses old consumer protocol
        this.UVarint('_metaLen');
        if (this.context._metaLen > 0) {
            var n = this.context._metaLen - 1;
            var _startOffset = this.offset;
            this
                .Int16BE('version')
                .array('subscriptions', this.string)
                .bytes('metadata');
            // skip any remaining bytes in the compact bytes envelope
            var consumed = this.offset - _startOffset;
            if (consumed < n) {
                this.skip(n - consumed);
            }
        }
        // memberAssignment: compactBytes framing, inner uses old consumer protocol
        this.SyncConsumerGroupResponseV4_MemberAssignment('memberAssignment');
        this.TaggedFields();
    }
});

/* istanbul ignore next */
Protocol.define('DescribeGroupResponseV5_GroupMemberItem', {
    read: function () {
        this
            .compactString('memberId')
            .compactNullableString('groupInstanceId')
            .compactString('clientId')
            .compactString('clientHost')
            .compactBytes('memberMetadata')
            .compactBytes('memberAssignment')
            .TaggedFields();
    }
});

Protocol.define('DescribeGroupResponseV5_GroupItem', {
    read: function () {
        this
            .ErrorCode('error')
            .compactString('groupId')
            .compactString('state')
            .compactString('protocolType')
            .compactString('protocol');
        if (this.context.protocolType === 'consumer') {
            this.compactArray('members', this.DescribeGroupResponseV5_ConsumerGroupMemberItem);
        } else {
            /* istanbul ignore next */
            this.compactArray('members', this.DescribeGroupResponseV5_GroupMemberItem);
        }
        this.Int32BE('authorizedOperations')
            .TaggedFields();
    }
});

Protocol.define('DescribeGroupResponseV5', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('groups', this.DescribeGroupResponseV5_GroupItem)
            .TaggedFields();
    }
});

// DescribeGroups v6 (KIP-1043) — adds ErrorMessage per group between ErrorCode and GroupId
Protocol.define('DescribeGroupResponseV6_GroupItem', {
    read: function () {
        this
            .ErrorCode('error')
            .compactNullableString('errorMessage')
            .compactString('groupId')
            .compactString('state')
            .compactString('protocolType')
            .compactString('protocol');
        if (this.context.protocolType === 'consumer') {
            this.compactArray('members', this.DescribeGroupResponseV5_ConsumerGroupMemberItem);
        } else {
            /* istanbul ignore next */
            this.compactArray('members', this.DescribeGroupResponseV5_GroupMemberItem);
        }
        this.Int32BE('authorizedOperations')
            .TaggedFields();
    }
});

Protocol.define('DescribeGroupResponseV6', {
    read: function () {
        this
            .Int32BE('correlationId')
            .TaggedFields()
            .Int32BE('throttleTime')
            .compactArray('groups', this.DescribeGroupResponseV6_GroupItem)
            .TaggedFields();
    }
});
