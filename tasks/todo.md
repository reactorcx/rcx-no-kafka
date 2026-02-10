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
