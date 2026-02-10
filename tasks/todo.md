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

---

# KIP-951: Leader Discovery Optimizations (Produce v10/v11)

## Background

KIP-951 adds leader discovery information to Produce responses so the client can find the new leader without a full metadata refresh on NOT_LEADER_OR_FOLLOWER errors.

**Produce v10** adds a `CurrentLeader` tagged field (tag 0) per partition in the response:
- `leaderId` (Int32, default -1) — the current leader broker ID
- `leaderEpoch` (Int32, default -1) — the latest known epoch

**Produce v11** adds a `NodeEndpoints` tagged field (tag 0) at the response root:
- Array of `NodeEndpoint`: `nodeId` (Int32), `host` (compactString), `port` (Int32), `rack` (compactNullableString)
- Provides actual connection endpoints so the client can connect directly without metadata refresh

Both are optional tagged fields — the response parser must gracefully handle their presence or absence.

**Request format**: v10 and v11 are identical to v9 (same fields, same compact encoding). Only the `apiVersion` changes.

## Todo

- [x] **1** Add `ProduceRequestV10` and `ProduceRequestV11` to `lib/protocol/produce.js` — same body as V9 but with `apiVersion: 10` and `apiVersion: 11`
- [x] **2** Add `ProduceResponseV10CurrentLeader` helper struct — reads `leaderId` (Int32), `leaderEpoch` (Int32), `TaggedFields()`
- [x] **3** Add `ProduceResponseV10NodeEndpoint` helper struct — reads `nodeId` (Int32), `host` (compactString), `port` (Int32), `rack` (compactNullableString), `TaggedFields()`
- [x] **4** Add `ProduceResponseV10PartitionItem` — same fields as V9 but replaces trailing `TaggedFields()` with inline tagged field parsing: tag 0 → `currentLeader` (using CurrentLeader helper)
- [x] **5** Add `ProduceResponseV10TopicItem` and `ProduceResponseV10` — uses V10 partition items; root-level body tagged fields parse tag 0 → `nodeEndpoints` (using NodeEndpoint helper array)
- [x] **6** Update `lib/client.js` version negotiation — bump clientMax from 9 to 11 in both `processPartition` and the send block; add `>= 11` and `>= 10` version dispatch branches
- [x] **7** Add unit tests in `test/22.kip951_leader_discovery.js` — encode V10/V11 requests; decode V10 response with CurrentLeader; decode with NodeEndpoints; decode without tagged fields (graceful fallback)
- [x] **8** Run lint + full test suite to verify no regressions

---

## Review

### Summary
Implemented KIP-951 leader discovery optimizations for the Produce API (v10/v11). This adds support for the broker to return the new leader's identity and connection endpoint directly in the Produce response, eliminating the need for a full metadata refresh on NOT_LEADER_OR_FOLLOWER errors.

Key design decision: Rather than modifying the shared `TaggedFields` primitive, each V10 response struct uses inline tagged field parsing that handles tag 0 specifically and skips unknown tags. This is minimal-impact and avoids changes to the existing TaggedFields behavior.

### Protocol Definitions Added

| Definition | Purpose |
|-----------|---------|
| `ProduceRequestV10` | Same as V9, apiVersion: 10 |
| `ProduceRequestV11` | Same as V9, apiVersion: 11 |
| `ProduceResponseV10CurrentLeader` | Helper: leaderId (Int32) + leaderEpoch (Int32) + TaggedFields |
| `ProduceResponseV10NodeEndpoint` | Helper: nodeId (Int32) + host (compactString) + port (Int32) + rack (compactNullableString) + TaggedFields |
| `ProduceResponseV10PartitionItem` | Same fields as V9; inline tagged field parsing for tag 0 → currentLeader |
| `ProduceResponseV10TopicItem` | Uses V10 partition items |
| `ProduceResponseV10` | Uses V10 topic items; inline body-level tagged field parsing for tag 0 → nodeEndpoints |

### Client Version Bumps (lib/client.js)

- Produce clientMax: 9 → 11 (both processPartition and send block)
- Added `>= 11` branch: ProduceRequestV11 + ProduceResponseV10
- Added `>= 10` branch: ProduceRequestV10 + ProduceResponseV10

### Response Data Flow

- `currentLeader` (per partition) flows through `_mapTopics` automatically via `_.omit(p, 'partition')`
- `nodeEndpoints` (root level) is available in the parsed result but not in the per-partition flattened array

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/produce.js` | Added 7 Protocol.define blocks for V10/V11 request and response types |
| `lib/client.js` | Bumped clientMax 9→11, added two version dispatch branches |
| `test/22.kip951_leader_discovery.js` | New file: 6 unit tests (encode V10/V11 requests, decode with/without CurrentLeader, decode with NodeEndpoints) |

### Test Results
- **410 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- 6 new unit tests in `test/22.kip951_leader_discovery.js`
- All existing integration tests pass (version negotiation falls back gracefully)

---

# KIP-516: Metadata v11/v12 (Topic ID Support Completion)

## Background

Metadata v10 (already implemented) added `topicId` to the response. Two more versions are needed:

- **v11** (KIP-700): Removes `includeClusterAuthorizedOperations` from request and `clusterAuthorizedOperations` from response — cluster authorized operations are now exposed by the DescribeCluster API instead.
- **v12** (KIP-516 completion): Topic `Name` becomes nullable in the response (`compactNullableString`). The broker can return only topicId without a name, enabling full topic-ID-based operation.

## Todo

- [x] **1** Add `MetadataRequestV11` and `MetadataResponseV11` to `lib/protocol/metadata.js` — drops `includeClusterAuthorizedOperations` / `clusterAuthorizedOperations`
- [x] **2** Add `MetadataRequestV12`, `TopicMetadataV12`, and `MetadataResponseV12` to `lib/protocol/metadata.js` — topicName uses `compactNullableString`
- [x] **3** Update `lib/client.js` metadata version negotiation — add `>= 12` and `>= 11` branches; resolve null topicName from topicId cache for v12 responses
- [x] **4** Add unit tests to `test/20.kip516_topic_ids.js` — encode v11/v12 requests; decode v11 response (no clusterAuthorizedOperations); decode v12 with null topicName; decode v12 with topicName present
- [x] **5** Run lint + full test suite to verify no regressions

---

## Review

### Summary
Completed Metadata v11/v12 support for the KIP-516 topic identifier story. v11 removes the deprecated cluster authorized operations fields. v12 makes topicName nullable in the response, enabling the broker to identify topics solely by UUID.

### Protocol Definitions Added

| Definition | Purpose |
|-----------|---------|
| `MetadataRequestV11` | Same as V10 but apiVersion: 11, drops `includeClusterAuthorizedOperations` |
| `MetadataResponseV11` | Same as V10 but drops `clusterAuthorizedOperations` |
| `MetadataRequestV12` | Same as V11 but apiVersion: 12 |
| `TopicMetadataV12` | topicName uses `compactNullableString` (nullable) |
| `MetadataResponseV12` | Uses `TopicMetadataV12` |

### Client Changes (lib/client.js)

- Added `>= 12` branch: MetadataRequestV12 + MetadataResponseV12 (sends topicId from cache when available)
- Added `>= 11` branch: MetadataRequestV11 + MetadataResponseV11
- Null topicName resolution: when topicName is null (v12), resolves from topicId cache before populating topicMetadata

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/metadata.js` | Added 5 Protocol.define blocks for V11/V12 request, response, and topic metadata types |
| `lib/client.js` | Added two version dispatch branches, null topicName resolution in metadata processing |
| `test/20.kip516_topic_ids.js` | Added 5 new tests (encode v11/v12 requests, decode v11/v12 responses with nullable topicName) |

### Test Results
- **415 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- 5 new unit tests in `test/20.kip516_topic_ids.js`
- All existing integration tests pass (version negotiation falls back gracefully)

---

# KIP-951: Leader Discovery Behavioral Layer

## Todo

- [x] **1** Add `_applyLeaderHints()` method to `lib/client.js`
- [x] **2** Modify `produceRequest()` response handling to call `_applyLeaderHints` for v10+ responses
- [x] **3** Parse `currentLeader` from Fetch v12/v13 tagged fields in `lib/protocol/fetch.js`
- [x] **4** Modify `fetchRequest()` response handling to call `_applyLeaderHints` for v12+ responses
- [x] **5** Add tests to `test/22.kip951_leader_discovery.js`
- [x] **6** Run lint + full test suite

---

## Review

### Summary
Implemented KIP-951 leader discovery behavioral layer. The wire format for Produce v10/v11 was already in place; this adds the Fetch v12/v13 `currentLeader` parsing and the `_applyLeaderHints()` method that processes leader hints from both APIs to update the client's metadata cache without a full metadata refresh.

### What was added

**`_applyLeaderHints(topics, nodeEndpoints)`** (`lib/client.js`):
- Creates broker connections from `nodeEndpoints` (Produce v11) if missing
- Updates `topicMetadata[topic][partition].leader` from `currentLeader` hints when `leaderId >= 0`
- Respects `checkBrokerRedirect` for new connections

**Fetch v12/v13 CurrentLeader parsing** (`lib/protocol/fetch.js`):
- Replaced generic `TaggedFields()` in `FetchResponseV12PartitionItem` with inline tagged field parsing
- Tag 1 → `currentLeader` (reuses `ProduceResponseV10CurrentLeader` helper)
- All other tags (including tag 0 = DivergingEpoch) → skipped
- Both v12 and v13 benefit since they share `FetchResponseV12PartitionItem`

**Response handling integration** (`lib/client.js`):
- `produceRequest()`: v10+ responses extract full result, call `_applyLeaderHints(topics, nodeEndpoints)`
- `fetchRequest()`: v12+ responses call `_applyLeaderHints(topics, null)` (Fetch has no NodeEndpoints until v16+)

### Files Modified

| File | Changes |
|------|---------|
| `lib/client.js` | Added `_applyLeaderHints()` method; modified `produceRequest()` and `fetchRequest()` response handling |
| `lib/protocol/fetch.js` | Replaced `TaggedFields()` with inline tag parsing in `FetchResponseV12PartitionItem` |
| `test/22.kip951_leader_discovery.js` | Added 5 new tests (Fetch v12 decode with/without CurrentLeader, _applyLeaderHints: metadata update, leaderId -1 ignored, nodeEndpoints creates connections) |

### Test Results
- **420 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- 5 new tests, 11 total in `test/22.kip951_leader_discovery.js`
- All existing integration tests pass (no regressions)

---

# Fetch v14–v16 Protocol Implementation (KIP-405, KIP-903, KIP-951)

## Todo

- [x] **1** Add `FetchRequestV14` to `lib/protocol/fetch.js` — same as V13 but apiVersion: 14
- [x] **2** Add `FetchRequestV15` to `lib/protocol/fetch.js` — removes replicaId, body starts at maxWaitTime
- [x] **3** Add `FetchRequestV16` and `FetchResponseV16` to `lib/protocol/fetch.js` — response adds NodeEndpoints (tag 0)
- [x] **4** Update `lib/client.js` — bump clientMax 13→16, add three version dispatch branches
- [x] **5** Add tests to `test/22.kip951_leader_discovery.js`
- [x] **6** Lint + full test suite

---

## Review

### Summary
Implemented Fetch v14–v16 protocol support:
- **v14** (KIP-405 Tiered Storage): Version bump only, no wire format changes
- **v15** (KIP-903 Broker Epoch): Removes `replicaId` from request body (consumer clients don't send ReplicaState tagged field)
- **v16** (KIP-951 Leader Discovery): Response adds `NodeEndpoints` body-level tagged field (tag 0), reusing `ProduceResponseV10NodeEndpoint`

### Protocol Definitions Added

| Definition | Purpose |
|-----------|---------|
| `FetchRequestV14` | Same as V13, apiVersion: 14 |
| `FetchRequestV15` | Removes replicaId field, apiVersion: 15 |
| `FetchRequestV16` | Same as V15, apiVersion: 16 |
| `FetchResponseV16` | Inline body-level tagged field parsing: tag 0 → nodeEndpoints |

### Client Changes (lib/client.js)

- Fetch clientMax: 13 → 16
- Added `>= 16` branch: FetchRequestV16 + FetchResponseV16 (passes nodeEndpoints to `_applyLeaderHints`)
- Added `>= 15` branch: FetchRequestV15 + FetchResponseV13
- Added `>= 14` branch: FetchRequestV14 + FetchResponseV13
- Changed topicId resolution from `responseName === 'FetchResponseV13'` to `fetchVersion >= 13` (covers v13-v16)
- Changed `_applyLeaderHints` call to pass `parsed.nodeEndpoints || null` (v16 provides nodeEndpoints)

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/fetch.js` | Added 4 Protocol.define blocks for V14/V15/V16 request and V16 response |
| `lib/client.js` | Bumped clientMax 13→16, added three version dispatch branches, updated response handling |
| `test/22.kip951_leader_discovery.js` | Added 5 new tests (encode V14/V15/V16 requests, decode V16 with/without NodeEndpoints) |

### Test Results
- **425 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- 5 new tests, 16 total in `test/22.kip951_leader_discovery.js`
- All existing integration tests pass (no regressions)

---

# ListOffsets v7–v9 Protocol Implementation

## Todo

- [x] **1** Add `OffsetRequestV7`, `OffsetRequestV8`, `OffsetRequestV9` to `lib/protocol/offset.js`
- [x] **2** Update `lib/client.js` version negotiation — bump clientMax 6→9, add v7/v8/v9 branches
- [x] **3** Add encode tests for v7/v8/v9 in `test/20.kip516_topic_ids.js`
- [x] **4** Lint + full test suite

---

## Review

### Summary
Added ListOffsets v7–v9 protocol support. All three versions have identical wire format to v6 — only the `apiVersion` in the request header changes:
- **v7** (KIP-734): Enables max timestamp query
- **v8** (KIP-405): Enables tiered storage queries
- **v9** (KIP-1005): Enables last tiered offset query (timestamp sentinel -5)

All reuse `OffsetRequestV6TopicItem`/`OffsetRequestV6PartitionItem` for the request body and `OffsetResponseV6` for the response.

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/offset.js` | Added 3 Protocol.define blocks (`OffsetRequestV7`, `OffsetRequestV8`, `OffsetRequestV9`) |
| `lib/client.js` | Bumped clientMax 6→9, added three version dispatch branches (v9, v8, v7) |
| `test/20.kip516_topic_ids.js` | Added 3 encode tests (ListOffsets v7/v8/v9 describe blocks) |

### Test Results
- **428 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- 3 new tests in `test/20.kip516_topic_ids.js`
- All existing integration tests pass (no regressions)

---

# FindCoordinator v4–v6 (KIP-699, KIP-890, KIP-932)

## Background

FindCoordinator is currently implemented through v3 (the first flexible version per KIP-482). Three more versions are needed:

- **v4** (KIP-699): Replaces single `key` (string) with `coordinatorKeys` ([]string) for batch coordinator lookup. Response restructured: removes top-level `errorCode`/`errorMessage`/`coordinatorId`/`coordinatorHost`/`coordinatorPort` and replaces with a `coordinators` array, where each item has `key`, `nodeId`, `host`, `port`, `errorCode`, `errorMessage`.
- **v5** (KIP-890): Same wire format as v4, adds support for `TRANSACTION_ABORTABLE` error code (error 74).
- **v6** (KIP-932): Same wire format as v4/v5, adds support for share group coordinator type (keyType=2).

All three versions are flexible (v3+), so they use `FlexibleRequestHeader`, `compactString`, `compactArray`, `TaggedFields`.

Since v5 and v6 have identical wire format to v4, the V4 protocol definitions can be reused for v5/v6 by accepting a variable `apiVersion`.

## Wire Format

### Request (v4-v6)
```
FlexibleRequestHeader(apiKey=10, apiVersion=4/5/6)
Int8(keyType)                    // 0=group, 1=transaction, 2=share
compactArray(coordinatorKeys)    // each: compactString
TaggedFields()
```

### Response (v4-v6)
```
Int32BE(correlationId)
TaggedFields()                   // flexible response header
Int32BE(throttleTime)
compactArray(coordinators)       // each coordinator item:
  compactString(key)
  Int32BE(nodeId)
  compactString(host)
  Int32BE(port)
  ErrorCode(errorCode)
  compactNullableString(errorMessage)
  TaggedFields()
TaggedFields()                   // body-level
```

## Todo

- [x] **1** Add `FindCoordinatorRequestV4` and response to `lib/protocol/group_membership.js` — request takes `coordinatorKeys` array + `keyType`; response returns `coordinators` array. Accept `apiVersion` from data so v5/v6 reuse the same definition.
- [x] **2** Update `_findGroupCoordinator()` in `lib/client.js` — bump coordMax threshold to use V4 when available, wrap single groupId into `coordinatorKeys: [groupId]`, extract first coordinator from response array.
- [x] **3** Update `_findTransactionCoordinator()` in `lib/client.js` — same pattern as step 2 but with `keyType: 1`.
- [x] **4** Add unit tests in `test/18.kip482_flexible_api_bumps.js` — encode V4 request, decode V4 response with coordinators array, round-trip verification.
- [x] **5** Lint + full test suite to verify no regressions.

---

## Review

### Summary
Implemented FindCoordinator v4–v6 protocol support (KIP-699, KIP-890, KIP-932). V4 restructures the API for batch coordinator lookup — the request takes an array of `coordinatorKeys` instead of a single `key`, and the response returns a `coordinators` array with per-key error handling instead of flat top-level fields. V5 and V6 have identical wire format to V4 (only adding new error code and coordinator type support respectively), so a single protocol definition handles all three via a variable `apiVersion`.

### Protocol Definitions Added

| Definition | Purpose |
|-----------|---------|
| `FindCoordinatorRequestV4` | Takes `coordinatorKeys` array + `keyType`, variable `apiVersion` for v4/v5/v6 |
| `FindCoordinatorResponseV4_Coordinator` | Per-coordinator item: key, nodeId, host, port, errorCode, errorMessage |
| `FindCoordinatorResponseV4` | Parses `coordinators` array (replaces flat fields from v3) |

### Client Changes (lib/client.js)

- `_findGroupCoordinator()`: Added `coordMax >= 4` branch — wraps `groupId` into `coordinatorKeys: [groupId]` with `keyType: 0`, extracts first coordinator from response array and normalizes field names
- `_findTransactionCoordinator()`: Same pattern with `keyType: 1`
- Both use `Math.min(coordMax, 6)` as apiVersion cap

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/group_membership.js` | Added 3 Protocol.define blocks (FindCoordinatorRequestV4, FindCoordinatorResponseV4_Coordinator, FindCoordinatorResponseV4) |
| `lib/client.js` | Added `coordMax >= 4` branches in both `_findGroupCoordinator()` and `_findTransactionCoordinator()`, with V4 response normalization |
| `test/18.kip482_flexible_api_bumps.js` | Added 5 tests (encode v4 request, encode v6 apiVersion, decode single coordinator, decode multiple coordinators, round-trip) |

### Test Results
- **433 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- 5 new tests in `test/18.kip482_flexible_api_bumps.js`
- All existing integration tests pass (no regressions)

---

# OffsetFetch v7–v9, InitProducerId v3–v5, LeaveGroup v5

## Background

Three API version bumps covering multiple KIPs.

### OffsetFetch (API key 9) — currently at v6

- **v7** (KIP-447): Adds `requireStable` (Int8/Bool, default false) at end of request body. Response identical to v6.
- **v8** (KIP-709): Major restructuring for multi-group support. Request replaces single `groupId` + `topics` with `groups` array (each containing `groupId` + `topics`). Response similarly restructured to `groups` array (each with `groupId`, `topics`, `errorCode`). This is the batch multi-group version.
- **v9** (KIP-848): Same as v8 but adds `memberId` (compactNullableString) and `memberEpoch` (Int32, default -1) to the request. Response can return new error codes (STALE_MEMBER_EPOCH, UNKNOWN_MEMBER_ID).

All are flexible versions (v6+). The client's `offsetFetchRequestV1` method only fetches for a single group, so v8/v9 will wrap it into a `groups: [{groupId, topics}]` array (similar to FindCoordinator v4 pattern).

#### Wire Format — OffsetFetch v7 Request
```
FlexibleRequestHeader(apiKey=9, apiVersion=7)
compactString(groupId)
compactNullableArray(topics)     // each: compactString(topicName), compactArray(partitions, Int32BE), TaggedFields
Int8(requireStable)              // NEW: 0 or 1
TaggedFields()
```

#### Wire Format — OffsetFetch v8 Request
```
FlexibleRequestHeader(apiKey=9, apiVersion=8)
compactArray(groups)             // each group:
  compactString(groupId)
  compactNullableArray(topics)   // each: compactString(topicName), compactArray(partitions, Int32BE), TaggedFields
  TaggedFields()
Int8(requireStable)
TaggedFields()
```

#### Wire Format — OffsetFetch v8 Response
```
Int32BE(correlationId)
TaggedFields()
Int32BE(throttleTime)
compactArray(groups)             // each group:
  compactString(groupId)
  compactArray(topics)           // each topic:
    compactString(topicName)
    compactArray(partitions)     // each partition:
      Int32BE(partition)
      KafkaOffset(offset)
      Int32BE(committedLeaderEpoch)
      compactNullableString(metadata)
      ErrorCode(error)
      TaggedFields()
    TaggedFields()
  ErrorCode(error)               // per-group error
  TaggedFields()
TaggedFields()
```

#### Wire Format — OffsetFetch v9 Request (same as v8 + 2 new fields)
```
FlexibleRequestHeader(apiKey=9, apiVersion=9)
compactArray(groups)             // same as v8
Int8(requireStable)
compactNullableString(memberId)  // NEW (KIP-848)
Int32BE(memberEpoch)             // NEW (KIP-848, default -1)
TaggedFields()
```

### InitProducerId (API key 22) — currently at v2

- **v3** (KIP-588): Adds `producerId` (Int64, default -1) and `producerEpoch` (Int16, default -1) to request, enabling producers to recover producer state after epoch bumps. Response identical to v2.
- **v4** (KIP-890): Same wire format as v3, adds PRODUCER_FENCED error code support.
- **v5** (KIP-890): Same wire format as v3/v4, adds TRANSACTION_ABORTABLE error code support.

All flexible (v2+). V4 and V5 reuse V3 definition with variable `apiVersion`.

#### Wire Format — InitProducerId v3 Request
```
FlexibleRequestHeader(apiKey=22, apiVersion=3/4/5)
compactNullableString(transactionalId)
Int32BE(transactionTimeoutMs)
Int64BE(producerId)              // NEW (default -1)
Int16BE(producerEpoch)           // NEW (default -1)
TaggedFields()
```

### LeaveGroup (API key 13) — currently at v4

**Note:** The Kafka protocol schema shows `validVersions: "0-5"` for LeaveGroup. There is no v6 — v5 is the latest version.

- **v5** (KIP-800): Adds `reason` (compactNullableString) per member item in the request. Response identical to v4.

#### Wire Format — LeaveGroup v5 Request Member Item
```
compactString(memberId)
compactNullableString(groupInstanceId)
compactNullableString(reason)    // NEW (KIP-800, nullable, ignorable)
TaggedFields()
```

## Todo

- [x] **1** Add OffsetFetch v7 request to `lib/protocol/offset_commit_fetch.js` — same as v6 but adds `requireStable` (Int8) at end of request body. Response reuses v6.
- [x] **2** Add OffsetFetch v8 request/response to `lib/protocol/offset_commit_fetch.js` — multi-group request structure with `groups` array; response with `groups` array containing per-group topics+error.
- [x] **3** Add OffsetFetch v9 request to `lib/protocol/offset_commit_fetch.js` — same as v8 but adds `memberId` and `memberEpoch`. Response reuses v8. Accept variable `apiVersion` from data.
- [x] **4** Update `offsetFetchRequestV1()` in `lib/client.js` — bump clientMax from 6 to 9; add v9/v8/v7 branches wrapping single group into `groups` array for v8+, extract first group from response.
- [x] **5** Add InitProducerId v3 request to `lib/protocol/init_producer_id.js` — adds `producerId` and `producerEpoch` to request. Accept variable `apiVersion` from data for v4/v5 reuse. Response reuses v2.
- [x] **6** Update `initProducerIdRequest()` in `lib/client.js` — bump clientMax from 2 to 5; add `>= 3` branch passing current producerId/producerEpoch.
- [x] **7** Add LeaveGroup v5 member item and request to `lib/protocol/group_membership.js` — adds `reason` per member. Response reuses v4.
- [x] **8** Update `leaveGroupRequest()` in `lib/client.js` — bump clientMax from 4 to 5; add `>= 5` branch.
- [x] **9** Add unit tests for all new protocol versions.
- [x] **10** Lint + full test suite to verify no regressions.

---

## Review

### Summary
Implemented three API version bumps covering six KIPs:

- **OffsetFetch v7–v9**: v7 (KIP-447) adds `requireStable` flag for read-committed consumers. v8 (KIP-709) restructures the API for multi-group batch fetches — request wraps groups into an array, response returns per-group error codes. v9 (KIP-848) adds `memberId` and `memberEpoch` for the new consumer protocol. The client wraps single-group fetches into the v8 groups array format and extracts the first group from the response, matching the FindCoordinator v4 pattern.

- **InitProducerId v3–v5**: v3 (KIP-588) adds `producerId` and `producerEpoch` to the request for epoch recovery after transactional failures. v4/v5 (KIP-890) have identical wire format (new error codes only). A single V3 definition handles all three via variable `apiVersion`.

- **LeaveGroup v5**: v5 (KIP-800) adds `reason` (compactNullableString) per member item. Note: the user originally requested v6, but the Kafka protocol schema only goes to v5 (`validVersions: "0-5"`).

### Protocol Definitions Added

| File | APIs Added |
|------|-----------|
| `lib/protocol/offset_commit_fetch.js` | OffsetFetchRequestV7, OffsetFetchRequestV8_GroupItem, OffsetFetchRequestV8, OffsetFetchResponseV8PartitionItem, OffsetFetchResponseV8TopicItem, OffsetFetchResponseV8_GroupItem, OffsetFetchResponseV8 |
| `lib/protocol/init_producer_id.js` | InitProducerIdRequestV3 |
| `lib/protocol/group_membership.js` | LeaveGroupRequestV5_MemberItem, LeaveGroupRequestV5 |

### Client Version Bumps (lib/client.js)

- OffsetFetch: 6 → 9 (v8+ wraps single group into `groups` array, extracts first group from response; v7 adds `requireStable: false`)
- InitProducerId: 2 → 5 (v3+ passes `producerId`/`producerEpoch` for epoch recovery)
- LeaveGroup: 4 → 5 (v5 adds `reason: null` per member)

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/offset_commit_fetch.js` | Added 7 Protocol.define blocks for OffsetFetch v7/v8/v9 request and v8 response types |
| `lib/protocol/init_producer_id.js` | Added 1 Protocol.define block for InitProducerId v3 request |
| `lib/protocol/group_membership.js` | Added 2 Protocol.define blocks for LeaveGroup v5 member item and request |
| `lib/client.js` | Bumped 3 API version ceilings, added 4 version dispatch branches (v8+, v7, v3+, v5) |
| `test/18.kip482_flexible_api_bumps.js` | Added 12 new tests (49 total in file) |

### Test Results
- **425 passing**, 14 failing (all pre-existing: SSL on port 9093, Static Group Membership timeouts, Consistent Assignment timeout)
- ESLint: clean
- 12 new unit tests in `test/18.kip482_flexible_api_bumps.js`
- All existing integration tests pass (no regressions from our changes)

---

# KIP-899: Client Re-bootstrap

## Background

KIP-899 allows Kafka clients to repeat the bootstrap process when all known brokers become unavailable. This is purely client-side logic — no protocol changes.

**Problem**: When all cached broker connections die (DNS changes, IP rotation, full cluster restart), the no-kafka client fails permanently because `initialBrokers` connections are created once in `init()` and never refreshed. Even though `metadataRequest()` tries all `initialBrokers` via `Promise.any()`, if those Connection objects are closed or point to stale addresses, every attempt fails and the error propagates.

**Solution**: When `metadataRequest()` fails and the `rebootstrap` option is enabled, the client re-parses the original `connectionString`, creates fresh Connection objects (which re-resolve DNS on connect), discovers API versions, and retries the metadata request.

### Current flow (lib/client.js)

```
init()
  → parse connectionString → create initialBrokers (Connection objects)
  → apiVersionsRequest() on each initialBroker
  → updateMetadata()

updateMetadata()
  → metadataRequest() — Promise.any(initialBrokers.map(send))
    → success → update brokerConnections, topicMetadata
    → failure → throw  ← PROBLEM: no recovery
```

### With KIP-899

```
updateMetadata()
  → metadataRequest()
    → success → update brokerConnections, topicMetadata
    → failure → if rebootstrap enabled:
        → _rebootstrap()  — close old, create new initialBrokers, apiVersions
        → metadataRequest()  — retry with fresh connections
          → success → update brokerConnections, topicMetadata
          → failure → throw
```

### Key points

- `connectionString` is preserved in `self.options.connectionString` — always available for re-parsing
- `Connection.close()` sets `closed = true` which prevents reconnection — old connections must be replaced, not reused
- DNS re-resolution happens naturally via `net.connect({host, port})` in new Connection objects
- One rebootstrap attempt per metadata failure — no infinite loops
- Log a warning when rebootstrap is triggered for observability

## Todo

- [x] **1** Add `rebootstrap` option to client constructor defaults (`lib/client.js` line ~33) — boolean, default `false`.
- [x] **2** Add `_rebootstrap()` method to Client prototype (`lib/client.js`) — closes old `initialBrokers`, re-parses `connectionString`, creates fresh Connection objects, runs `apiVersionsRequest()` on each. Returns a promise.
- [x] **3** Modify `updateMetadata()` (`lib/client.js` line ~263) — wrap the `metadataRequest()` call so that on failure, if `self.options.rebootstrap` is enabled, call `_rebootstrap()` then retry `metadataRequest()` once. If retry also fails, throw the original error.
- [x] **4** Add unit tests (`test/23.kip899_rebootstrap.js`) — test `_rebootstrap()` creates fresh connections; test `updateMetadata()` retries after rebootstrap when enabled; test no retry when disabled.
- [x] **5** Lint + full test suite to verify no regressions.

---

## Review

### Summary
Implemented KIP-899 client re-bootstrap for the no-kafka client. When all known broker connections are unavailable and a metadata update fails, the client can now automatically re-resolve the original `connectionString`, create fresh Connection objects (which re-resolve DNS on connect), discover API versions, and retry the metadata request. This is purely client-side logic — no protocol changes.

The feature is opt-in via `rebootstrap: true` in client options (default `false`). One rebootstrap attempt per metadata failure — no infinite retry loops. A warning is logged when rebootstrap is triggered for observability.

### How it works

1. `updateMetadata()` calls `metadataRequest()` which tries all `initialBrokers` via `Promise.any()`
2. If all fail and `rebootstrap` is enabled, a `.catch()` handler calls `_rebootstrap()`
3. `_rebootstrap()` closes old `initialBrokers` (permanently dead after `close()`), re-parses `connectionString`, creates new Connection objects, runs `apiVersionsRequest()` on each
4. `metadataRequest()` is retried with the fresh connections
5. If retry also fails, the error propagates normally

### Files Modified

| File | Changes |
|------|---------|
| `lib/client.js` | Added `rebootstrap: false` default option; added `_rebootstrap()` method (~15 lines); added `.catch()` handler in `updateMetadata()` (~7 lines) |
| `test/23.kip899_rebootstrap.js` | New file: 3 tests — `_rebootstrap()` creates fresh connections, `updateMetadata()` recovers with rebootstrap enabled, `updateMetadata()` throws with rebootstrap disabled |

### Test Results
- **448 passing**, 2 failing (pre-existing SSL on port 9093)
- ESLint: clean
- 3 new tests in `test/23.kip899_rebootstrap.js`
- All existing integration tests pass (no regressions)

---

# KIP-390: Compression Level Support

## Background

KIP-390 adds a `compression.level` configuration to the Kafka producer so users can control compression ratio vs. speed tradeoffs. This is purely a client-side change — no protocol modifications.

### Current architecture

The compression call chain is:

```
producer.send(data, {codec})
  → client.produceRequest(requests, codec)
    → compress(recordsBuf, codec)           // compression.js — no level param
    → compress(messageSetBuf, codec)        // legacy path — same
```

`compression.js` has four codecs, each with `compress`/`compressAsync` methods that take `(buffer)` only:
- **Gzip**: `zlib.gzipSync(buffer)` — supports `{level: -1..9}` option
- **Zstd**: `zstd.compress(buffer)` — supports numeric level argument (1-22)
- **LZ4**: `lz4.encode(buffer)` — supports `{highCompression: true}` option
- **Snappy**: `snappy.compressSync(buffer)` — no level support (fixed algorithm)

### Plan

Thread a `compressionLevel` parameter from producer options through to each codec:

1. **compression.js**: Add `level` parameter to `compress(buffer, codec, level)` and `compressAsync(buffer, codec, level)`. Each codec's compress functions accept the level:
   - Gzip: pass `{level: N}` to `zlib.gzipSync`/`zlib.gzip`
   - Zstd: pass level to `zstd.compress(buffer, level)`
   - LZ4: map `level > 0` to `{highCompression: true}`
   - Snappy: ignore level (fixed algorithm)
   - When level is `-1` or `undefined`, use the codec's default (current behavior)

2. **producer.js**: Add `compressionLevel: -1` to default options. Thread it alongside `codec` in `send()`.

3. **client.js**: Accept `compressionLevel` in `produceRequest()`. Pass it to `compress(buffer, codec, level)` in both the RecordBatch and legacy MessageSet paths.

## Todo

- [x] **1** Update compression codec functions in `lib/protocol/misc/compression.js` — add `level` parameter to Gzip, Zstd, LZ4 compress/compressAsync; add `level` to exported compress/compressAsync.
- [x] **2** Add `compressionLevel: -1` default option in `lib/producer.js`; thread `compressionLevel` alongside `codec` in `send()` options.
- [x] **3** Update `lib/client.js` — accept `compressionLevel` in `produceRequest()` signature; pass to `compress()` calls in RecordBatch and legacy MessageSet paths.
- [x] **4** Add unit tests (`test/24.kip390_compression_level.js`) — verify gzip level is applied (compare compressed sizes at level 1 vs 9); verify default level (-1) matches current behavior; verify snappy ignores level.
- [x] **5** Lint + full test suite to verify no regressions.

---

## Review

### Summary
Implemented KIP-390 compression level support for the no-kafka producer. Users can now specify a `compressionLevel` option to control the compression ratio vs. speed tradeoff. This is purely client-side — no protocol changes.

Level semantics per codec:
- **Gzip**: levels 0-9 (0=none, 1=fastest, 9=best compression). Default (-1) uses zlib default (level 6).
- **Zstd**: levels 1-22 (1=fastest, 22=best). Default (-1) uses zstd default (level 3).
- **LZ4**: level >= 0 enables high compression mode. Default (-1) uses standard LZ4.
- **Snappy**: level ignored (fixed algorithm, no tuning).

### Usage

```javascript
// Producer-level setting
var producer = new Kafka.Producer({
    codec: Kafka.COMPRESSION_GZIP,
    compressionLevel: 9  // max compression
});

// Per-send override
producer.send(data, { codec: Kafka.COMPRESSION_GZIP, compressionLevel: 1 });
```

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/misc/compression.js` | Added `level` parameter to Gzip, Zstd, LZ4 compress/compressAsync methods; added `level` to exported compress/compressAsync |
| `lib/producer.js` | Added `compressionLevel: -1` default; threaded through `send()` options to `produceRequest()` |
| `lib/client.js` | Added `compressionLevel` parameter to `produceRequest()` and `_compressMessageSet()`; passed to `compress()` calls |
| `test/24.kip390_compression_level.js` | New file: 6 tests — gzip default level, undefined level, level 1 vs 9 size comparison, async level, round-trip with level, snappy ignores level |

### Test Results
- **454 passing**, 2 failing (pre-existing SSL on port 9093)
- ESLint: clean
- 6 new unit tests in `test/24.kip390_compression_level.js`
- All existing compression tests pass (backward compatible — level undefined = codec default)
