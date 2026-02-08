'use strict';

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

module.exports = {
    API_KEYS: {
        ProduceRequest          : 0,
        FetchRequest            : 1,
        OffsetRequest           : 2,
        MetadataRequest         : 3,
        OffsetCommitRequest     : 8,
        OffsetFetchRequest      : 9,
        GroupCoordinatorRequest : 10,
        JoinGroupRequest        : 11,
        HeartbeatRequest        : 12,
        LeaveGroupRequest       : 13,
        SyncGroupRequest        : 14,
        DescribeGroupsRequest   : 15,
        ListGroupsRequest       : 16,
        ApiVersionsRequest      : 18,
        InitProducerIdRequest   : 22,
        AddPartitionsToTxnRequest : 24,
        AddOffsetsToTxnRequest  : 25,
        EndTxnRequest           : 26,
        TxnOffsetCommitRequest  : 28
    }
};

