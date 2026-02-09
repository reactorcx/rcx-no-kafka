# Kafka no-kafka Client - Implementation Plan

(Previous completed phases omitted for brevity — see git history)

---

# KIP-429: Cooperative/Incremental Rebalancing

## Todo

- [x] **1** Write ownedPartitions in subscription metadata (group_membership.js)
- [x] **2** Read ownedPartitions from JoinGroup response members (group_membership.js)
- [x] **3** Add cooperative state tracking to GroupConsumer constructor/init (group_consumer.js)
- [x] **4** Pass ownedPartitions in JoinGroup request (group_consumer.js)
- [x] **5** Cooperative assignment filtering in _syncGroup leader logic (group_consumer.js)
- [x] **6** Incremental _updateSubscriptions for cooperative mode (group_consumer.js)
- [x] **7** Phase 2 rejoin trigger in _rejoin (group_consumer.js)
- [x] **8** Tests for cooperative rebalancing (test/03.group_consumer.js)
- [x] **9** Run full test suite to verify no regressions
- [x] **10** Review and cleanup

---

## Review

### Summary
Implemented KIP-429 cooperative/incremental rebalancing for the no-kafka consumer group client. This is a client-side-only change — no broker protocol version changes needed.

Key design decisions:
- Cooperative mode is **opt-in** via `cooperative: true` in strategy config (default remains EAGER)
- The cooperative filtering logic lives in `GroupConsumer._syncGroup()`, NOT in individual strategies — zero changes to assignment strategy files
- Subscription metadata v1 adds `ownedPartitions` field so the leader knows what each member currently owns
- `_updateSubscriptions()` becomes incremental in cooperative mode (add/remove specific partitions instead of clear-all)
- Two-phase rejoin: phase 1 revokes migrating partitions, phase 2 assigns them to new owners

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/group_membership.js` | Write ownedPartitions in subscription metadata v1; read ownedPartitions from JoinGroup response members (both v0 and v5 formats) |
| `lib/group_consumer.js` | Track owned partitions + cooperative mode flag in constructor; pass ownedPartitions in JoinGroup; cooperative assignment filtering in _syncGroup leader; incremental _updateSubscriptionsCooperative; phase 2 rejoin trigger; rebalance callbacks (onPartitionsRevoked/onPartitionsAssigned) |
| `test/03.group_consumer.js` | Added `GroupConsumer (cooperative)` describe block with tests for: single consumer cooperative init, two-consumer cooperative rebalance, ownedPartitions tracking |

### Test Results
- **317 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- All existing tests unaffected (backward compatible)

---

# KIP-482: Flexible Versions Infrastructure

## Todo

- [x] **1** Add compact type primitives (compactString, compactBytes, compactArray + nullable variants) to `lib/protocol/common.js`
- [x] **2** Add TaggedFields primitive to `lib/protocol/common.js`
- [x] **3** Add FlexibleRequestHeader to `lib/protocol/common.js`
- [x] **4** Add FLEXIBLE_VERSION_THRESHOLDS map to `lib/protocol/globals.js`
- [x] **5** Add unit tests in `test/17.kip482_flexible_versions.js`
- [x] **6** Run lint and tests to verify no regressions

---

## Review

### Summary
Added KIP-482 flexible versions infrastructure — the encoding primitives needed for Kafka 2.6+ flexible version APIs. No existing APIs are changed; this is the foundation layer only.

### What was added

**Compact type primitives** (`lib/protocol/common.js`):
- `compactString` / `compactNullableString` — UVarint(N+1)-prefixed UTF-8 strings
- `compactBytes` / `compactNullableBytes` — UVarint(N+1)-prefixed byte buffers
- `compactArray` / `compactNullableArray` — UVarint(N+1)-prefixed element loops

**TaggedFields** (`lib/protocol/common.js`):
- Read: skips unknown tagged fields (UVarint tag count, then tag/size/data triples)
- Write: always emits `0x00` (empty tag section)

**FlexibleRequestHeader** (`lib/protocol/common.js`):
- Request header v1: same as v0 but uses compactNullableString for clientId + trailing TaggedFields

**FLEXIBLE_VERSION_THRESHOLDS** (`lib/protocol/globals.js`):
- Maps API key → first flexible version number for all APIs through Kafka 2.6

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/common.js` | Added 8 new Protocol.define blocks (6 compact types + TaggedFields + FlexibleRequestHeader) |
| `lib/protocol/globals.js` | Added FLEXIBLE_VERSION_THRESHOLDS map |
| `test/17.kip482_flexible_versions.js` | New file: 24 round-trip encode/decode tests |

### Test Results
- **334 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- All existing tests unaffected (backward compatible)

---

# KIP-482: Flexible Version API Bumps

## Todo

- [x] **1** InitProducerId v2 (init_producer_id.js + client.js)
- [x] **2** Heartbeat v4 (group_membership.js + client.js)
- [x] **3** ListGroups v3 (admin.js + client.js)
- [x] **4** FindCoordinator v3 (group_membership.js + client.js)
- [x] **5** OffsetCommit v8 (offset_commit_fetch.js + client.js)
- [x] **6** OffsetFetch v6 (offset_commit_fetch.js + client.js)
- [x] **7** LeaveGroup v4 (group_membership.js + client.js)
- [x] **8** SyncGroup v4 (group_membership.js + client.js)
- [x] **9** DescribeGroups v5 (admin.js + client.js)
- [x] **10** JoinGroup v6 (group_membership.js + client.js)
- [x] **11** Metadata v9 (metadata.js + client.js)
- [x] **12** ApiVersions v3 (api_versions.js — protocol only)
- [x] **13** Unit tests (test/18.kip482_flexible_api_bumps.js)
- [x] **14** Lint + full test suite

---

## Review

### Summary
Bumped all 12 APIs to their first flexible version using the KIP-482 infrastructure. Each bump changes the wire encoding: `string`→`compactString`, `array`→`compactArray`, `bytes`→`compactBytes`, `RequestHeader`→`FlexibleRequestHeader`, and `TaggedFields()` appended after every struct-level boundary.

### Critical Bug Found & Fixed
The FlexibleRequestHeader was incorrectly encoding the `clientId` as a `compactNullableString` (UVarint prefix). By decompiling the Kafka 2.6 broker's `RequestHeaderData.read()` bytecode, we discovered that the Kafka `RequestHeader.json` spec has `flexibleVersions: "none"` for the ClientId field. This means even in header v2, the clientId uses **regular Int16-prefixed string encoding** — only TaggedFields are added at the end. Fixed in `lib/protocol/common.js`.

### Protocol Definitions Added

| File | APIs Added |
|------|-----------|
| `lib/protocol/init_producer_id.js` | InitProducerIdRequestV2, InitProducerIdResponseV2 |
| `lib/protocol/group_membership.js` | FindCoordinatorRequestV3/ResponseV3, JoinConsumerGroupRequestV6/ResponseV6, HeartbeatRequestV4/ResponseV4, SyncConsumerGroupRequestV4/ResponseV4, LeaveGroupRequestV4/ResponseV4 |
| `lib/protocol/admin.js` | ListGroupsRequestV3/ResponseV3, DescribeGroupRequestV5/ResponseV5 |
| `lib/protocol/offset_commit_fetch.js` | OffsetCommitRequestV8/ResponseV8, OffsetFetchRequestV6/ResponseV6 |
| `lib/protocol/metadata.js` | MetadataRequestV9/ResponseV9 (with BrokerV9, PartitionMetadataV9, TopicMetadataV9) |
| `lib/protocol/api_versions.js` | ApiVersionsRequestV3/ResponseV3 |

### Client Version Bumps (lib/client.js)

All 12 APIs bumped with version-branch logic:
- Metadata: 8→9, FindCoordinator: 2→3, Heartbeat: 3→4, JoinGroup: 5→6
- SyncGroup: 3→4, LeaveGroup: 3→4, OffsetCommit: 7→8, OffsetFetch: 5→6
- ListGroups: 2→3, DescribeGroups: 4→5, InitProducerId: 1→2
- ApiVersions v3: protocol only (client always sends v0 for bootstrap)

### Test Results
- **373 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- 32 new unit tests in `test/18.kip482_flexible_api_bumps.js`
- All existing integration tests pass (broker auto-negotiates to flexible versions)

---

# KIP-345: Static Group Membership Behavioral Layer

## Todo

- [x] **1** Suppress LeaveGroup on shutdown for static members (`lib/group_consumer.js` — `end()`)
- [x] **2** Handle FencedInstanceId as fatal in `_heartbeat()` (`lib/group_consumer.js`)
- [x] **3** Handle FencedInstanceId as fatal in `_fullRejoin()` (`lib/group_consumer.js`)
- [x] **4** Add integration tests (`test/19.static_membership.js`)
- [x] **5** Run lint + full test suite to verify no regressions

---

## Review

### Summary
Implemented KIP-345 static group membership behavioral layer. The wire format was already in place; this adds the GroupConsumer logic that makes static membership actually work.

Key behaviors added:
- **Static members skip LeaveGroup on shutdown** — the whole point of KIP-345. When `groupInstanceId` is set, `end()` goes directly to closing connections without calling `leaveGroupRequest`, so the broker preserves the member's assignment for session timeout duration.
- **FencedInstanceId is fatal** — if another consumer registers with the same `group.instance.id`, the broker returns error code 82. The consumer now treats this as unrecoverable in both `_heartbeat()` and `_fullRejoin()`, setting `_closed = true` and rethrowing instead of entering a retry loop.

### Files Modified

| File | Changes |
|------|---------|
| `lib/group_consumer.js` | 3 small changes: `end()` skips LeaveGroup when groupInstanceId is set; `_heartbeat()` catch-all checks for FencedInstanceId before retrying; `_fullRejoin()` catch checks for FencedInstanceId before backoff retry |
| `test/19.static_membership.js` | New file: 3 integration tests — static member joins successfully, `end()` skips LeaveGroup for static members, `end()` still calls LeaveGroup for dynamic members |

### Test Results
- **376 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- All existing tests unaffected (backward compatible)

---

# KIP-516: Topic IDs (UUIDs)

## Todo

- [x] **1** Add `uuid` primitive type to `lib/protocol/common.js`
- [x] **2** Add Metadata v10 request/response to `lib/protocol/metadata.js`
- [x] **3** Add ListOffsets v6 (first flexible version) to `lib/protocol/offset.js`
- [x] **4** Add Produce v9 (first flexible version) to `lib/protocol/produce.js`
- [x] **5** Add Fetch v12 (first flexible version) to `lib/protocol/fetch.js`
- [x] **6** Add Fetch v13 (topicId-based) to `lib/protocol/fetch.js`
- [x] **7** Update `FLEXIBLE_VERSION_THRESHOLDS` in `lib/protocol/globals.js`
- [x] **8** Client integration — topic ID cache + version bumps in `lib/client.js`
- [x] **9** Tests in `test/20.kip516_topic_ids.js`
- [x] **10** Lint + full test suite to verify no regressions

---

## Review

### Summary
Implemented KIP-516 Topic IDs (UUIDs) for the no-kafka client. This adds support for Kafka 2.8+ topic identification via 128-bit UUIDs, including the core Metadata v10 and Fetch v13 protocol changes, plus the prerequisite flexible version bumps for Produce (v9), Fetch (v12), and ListOffsets (v6).

### What was added

**UUID primitive** (`lib/protocol/common.js`):
- `uuid` type: reads/writes 16 raw bytes, converts to/from standard UUID string format (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`), all-zeros sentinel represents null

**Protocol definitions added**:

| File | APIs Added |
|------|-----------|
| `lib/protocol/metadata.js` | MetadataRequestV10 (with topicId+nullable name per topic), MetadataResponseV10 (with topicId per topic) |
| `lib/protocol/offset.js` | OffsetRequestV6, OffsetResponseV6 (first flexible version) |
| `lib/protocol/produce.js` | ProduceRequestV9, ProduceResponseV9 (first flexible version) |
| `lib/protocol/fetch.js` | FetchRequestV12/ResponseV12 (first flexible version), FetchRequestV13/ResponseV13 (topicId replaces topic name) |

**Client integration** (`lib/client.js`):
- Topic ID cache: `topicIds` (name→UUID) and `topicNames` (UUID→name) maps populated from Metadata v10+ responses
- Version bumps: Metadata 9→10, Fetch 11→13, Produce 8→9, ListOffsets 5→6
- Fetch v13 translates topic names to IDs on send, resolves IDs back to names on receive

**Globals** (`lib/protocol/globals.js`):
- FLEXIBLE_VERSION_THRESHOLDS updated: Produce=9, Fetch=12, ListOffsets=6

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/common.js` | Added `uuid` primitive type |
| `lib/protocol/metadata.js` | Added MetadataRequestV10, MetadataResponseV10, TopicMetadataV10, MetadataRequestV10_TopicItem |
| `lib/protocol/offset.js` | Added OffsetRequestV6, OffsetResponseV6 + partition/topic item types |
| `lib/protocol/produce.js` | Added ProduceRequestV9, ProduceResponseV9 + partition/topic/recordError item types |
| `lib/protocol/fetch.js` | Added FetchRequestV12/V13, FetchResponseV12/V13 + partition/topic/forgotten item types |
| `lib/protocol/globals.js` | Updated FLEXIBLE_VERSION_THRESHOLDS for Produce, Fetch, ListOffsets |
| `lib/client.js` | Added topicIds/topicNames cache, bumped 4 API version ceilings, added v10/v12/v13/v9/v6 branches |
| `test/17.kip482_flexible_versions.js` | Updated threshold assertions for Produce, Fetch, ListOffsets |
| `test/20.kip516_topic_ids.js` | New file: 15 round-trip tests |

### Test Results
- **391 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- 15 new unit tests in `test/20.kip516_topic_ids.js`
- All existing integration tests pass (version negotiation falls back gracefully)

---

# Transaction API Version Bumps (v1-v3) for API Keys 24, 25, 26, 28

## Todo

- [x] **1** Make v0 request definitions accept `apiVersion` from data (transaction.js)
- [x] **2** Add TxnOffsetCommit v2 definitions (committedLeaderEpoch)
- [x] **3** Add v3 flexible definitions for all 4 APIs (transaction.js)
- [x] **4** Update FLEXIBLE_VERSION_THRESHOLDS in globals.js
- [x] **5** Client version negotiation in client.js
- [x] **6** Add round-trip tests (test/21.transaction_api_versions.js)
- [x] **7** Lint + full test suite

---

## Review

### Summary
Bumped all 4 transaction APIs (AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit) from v0-only to v3 (first flexible version). This completes the KIP-482 flexible versions story for the entire client.

For APIs 24/25/26, v1 and v2 only change behavioral semantics (not wire format), so the existing v0 definitions accept a variable `apiVersion`. TxnOffsetCommit v2 adds `committedLeaderEpoch` per partition, requiring a separate definition. All v3 definitions use compact types and TaggedFields per KIP-482. TxnOffsetCommit v3 additionally adds `generationId`, `memberId`, and `groupInstanceId` fields.

### Protocol Definitions Added

| File | APIs Added |
|------|-----------|
| `lib/protocol/transaction.js` | TxnOffsetCommitRequestV2 (+ partition/topic items), AddPartitionsToTxnRequestV3/ResponseV3, AddOffsetsToTxnRequestV3/ResponseV3, EndTxnRequestV3/ResponseV3, TxnOffsetCommitRequestV3/ResponseV3 (+ partition/topic items) |

### Client Version Bumps (lib/client.js)

All 4 transaction APIs bumped with version-branch logic:
- AddPartitionsToTxn: 0→3, AddOffsetsToTxn: 0→3, EndTxn: 0→3
- TxnOffsetCommit: 0→3 (with v2 branch for committedLeaderEpoch)

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/transaction.js` | Made v0 defs accept apiVersion from data; added TxnOffsetCommitRequestV2 + items; added v3 flexible defs for all 4 APIs |
| `lib/protocol/globals.js` | Added FLEXIBLE_VERSION_THRESHOLDS for keys 24, 25, 26, 28 (all = 3) |
| `lib/client.js` | Added `_negotiateVersion` calls and version branches for all 4 transaction methods |
| `test/21.transaction_api_versions.js` | New file: 16 round-trip tests |

### Test Results
- **407 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- 16 new unit tests in `test/21.transaction_api_versions.js`
- All existing integration tests pass (version negotiation falls back gracefully)
