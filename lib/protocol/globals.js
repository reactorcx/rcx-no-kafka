'use strict';

// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol

module.exports = {
    // Maps API key -> first flexible version (KIP-482), or null if not flexible in Kafka 2.6
    FLEXIBLE_VERSION_THRESHOLDS: {
        0:  null,   // Produce
        1:  null,   // Fetch
        2:  null,   // ListOffsets
        3:  9,      // Metadata
        8:  8,      // OffsetCommit
        9:  6,      // OffsetFetch
        10: 3,      // FindCoordinator
        11: 6,      // JoinGroup
        12: 4,      // Heartbeat
        13: 4,      // LeaveGroup
        14: 4,      // SyncGroup
        15: 5,      // DescribeGroups
        16: 3,      // ListGroups
        18: 3,      // ApiVersions
        22: 2       // InitProducerId
    },

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

