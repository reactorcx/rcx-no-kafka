# Kafka v0.11 Protocol Upgrade - Implementation Plan

## Phase 1: Foundation (no behavioral changes)

- [x] **1.1** Add new error codes (13, 33-51) to `lib/errors.js`
- [x] **1.2** Add `ApiVersionsRequest: 18` to `lib/protocol/globals.js`
- [x] **1.3** Create CRC-32C module at `lib/protocol/misc/crc32c.js` (Castagnoli polynomial)
- [x] **1.4** Add LZ4 compression (codec 3) to `lib/protocol/misc/compression.js`
- [x] **1.5** Export `COMPRESSION_LZ4 = 3` from `lib/index.js`

## Phase 2: ApiVersions Protocol & Discovery

- [x] **2.1** Create `lib/protocol/api_versions.js` — ApiVersionsRequest/Response
- [x] **2.2** Register `api_versions` module in `lib/protocol/index.js`
- [x] **2.3** Add `this.apiVersions = null` to `lib/connection.js`
- [x] **2.4** Add version discovery to `lib/client.js` — `apiVersionsRequest()`, `_negotiateVersion()`

## Phase 3: Record Batch v2 Format

- [x] **3.1** Add RecordHeader, Record, RecordBatch protocol definitions to `lib/protocol/common.js`
- [x] **3.2** Update MessageAttributes codec mask from `0x3` to `0x7`
- [x] **3.3** Format detection (magic byte peek) implemented in FetchResponseV4PartitionItem

## Phase 4: Updated API Versions

- [x] **4.1** Add FetchRequest/Response v3 to `lib/protocol/fetch.js`
- [x] **4.2** Add FetchRequest/Response v4 to `lib/protocol/fetch.js`
- [x] **4.3** Add ProduceResponse v2 to `lib/protocol/produce.js`
- [x] **4.4** Add ProduceRequest v3 to `lib/protocol/produce.js`
- [x] **4.5** Add MetadataRequest/Response v1 to `lib/protocol/metadata.js`
- [x] **4.6** Add OffsetRequest/Response v1 to `lib/protocol/offset.js`

## Phase 5: Client Integration (version-aware request routing)

- [x] **5.1** Version-aware fetch in `lib/client.js` + `_decompressRecordBatches()`
- [x] **5.2** Version-aware produce in `lib/client.js` with RecordBatch for v3+
- [x] **5.3** Version-aware offset in `lib/client.js`
- [x] **5.4** Version-aware metadata in `lib/client.js`

## Phase 6: Public API Updates

- [x] **6.1** Producer naturally passes through `timestamp`/`headers` — no code change needed
- [x] **6.2** Update TypeScript types (`types/kafka.d.ts`) for timestamp, headers, COMPRESSION_LZ4

## Phase 7: Test Suite

- [x] **7.1** Create `test/10.protocol_v011.js` — 46 unit tests covering all v0.11 additions

## Phase 8: Integration Tests

- [x] **8.1** Add LZ4 compression tests to `test/07.compression.js` (sync + async sections)
- [x] **8.2** Create `test/11.v011_integration.js` — v0.11 integration tests

---

# Idempotent & Transactional Producer

## Phase 1: Idempotent Producer

- [x] **1.1** Fix `|| -1` bug in RecordBatch write (`lib/protocol/common.js:435-437`)
- [x] **1.2** Add InitProducerId protocol (`lib/protocol/init_producer_id.js`)
- [x] **1.3** Register InitProducerIdRequest in globals + index
- [x] **1.4** Add missing error codes 53, 67 to `lib/errors.js`
- [x] **1.5** Add `producerId`/`producerEpoch` fields + `initProducerIdRequest()` to `lib/client.js`
- [x] **1.6** Wire idempotent state into `processPartition` in `lib/client.js`
- [x] **1.7** Add idempotent mode to `lib/producer.js` (sequence tracking, init, error handling)
- [x] **1.8** TypeScript types for idempotent option (`types/producer.d.ts`)

## Phase 2: Transactional Producer

- [x] **2.1** Add FindCoordinator v1 protocol (`lib/protocol/group_membership.js`)
- [x] **2.2** Add transaction protocol definitions (`lib/protocol/transaction.js`)
- [x] **2.3** Register transaction API keys in globals + index
- [x] **2.4** Client transaction methods (`lib/client.js`)
- [x] **2.5** Producer transaction lifecycle (`lib/producer.js`)
- [x] **2.6** TypeScript types for transactions (`types/producer.d.ts`)

## Phase 3: Consumer Isolation Level

- [x] **3.1** Accept `isolationLevel` option in `lib/base_consumer.js` and pass through in `lib/client.js`
- [x] **3.2** TypeScript types for isolationLevel (`types/simple_consumer.d.ts`, `types/group_consumer.d.ts`)

## Review

### Summary of Changes

**New files (4):**
- `lib/protocol/init_producer_id.js` — InitProducerIdRequest/Response (apiKey=22)
- `lib/protocol/transaction.js` — AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit (apiKeys 24, 25, 26, 28)
- `test/12.idempotent_transactional.js` — 57 unit tests for all idempotent/transactional protocol, producer, and consumer additions
- `test/13.idempotent_txn_integration.js` — 17 integration tests for idempotent producer, transactional producer, and consumer isolation level

**Modified files (10):**
- `lib/protocol/common.js:435-437` — Fixed `|| -1` to `!== null && !== undefined ? x : -1` for producerId/producerEpoch/baseSequence (producerId=0 was treated as falsy)
- `lib/protocol/globals.js` — Added API keys: InitProducerIdRequest(22), AddPartitionsToTxnRequest(24), AddOffsetsToTxnRequest(25), EndTxnRequest(26), TxnOffsetCommitRequest(28)
- `lib/protocol/index.js` — Registered `init_producer_id` and `transaction` modules
- `lib/protocol/group_membership.js` — Added FindCoordinatorRequest/Response v1 (coordinatorType: 0=group, 1=transaction)
- `lib/errors.js` — Added error codes: TransactionalIdAuthorizationFailed(53), ProducerFenced(67)
- `lib/client.js` — Added producerId/producerEpoch/transactionCoordinators fields; initProducerIdRequest(); _findTransactionCoordinator(); addPartitionsToTxnRequest(); addOffsetsToTxnRequest(); endTxnRequest(); txnOffsetCommitRequest(); wire idempotent PID into processPartition; pass transactionalId in ProduceRequestV3; pass isolationLevel from options in FetchRequestV4
- `lib/producer.js` — Added idempotent option (forces acks=-1); transactionalId option (implies idempotent); sequence number tracking per topic:partition; initProducerIdRequest on init; DuplicateSequenceNumber/OutOfOrderSequenceNumber handling; auto-register partitions for transactions; beginTransaction/commitTransaction/abortTransaction/sendOffsets methods
- `lib/base_consumer.js` — Added `isolationLevel: 0` default option
- `types/producer.d.ts` — Added idempotent, transactionalId, beginTransaction, commitTransaction, abortTransaction, sendOffsets
- `types/simple_consumer.d.ts` — Added `isolationLevel?: 0 | 1`
- `types/group_consumer.d.ts` — Added `isolationLevel?: 0 | 1`

### Backward Compatibility
- Default producer behavior unchanged (idempotent=false, no transactionalId)
- Default consumer behavior unchanged (isolationLevel=0 = read_uncommitted)
- The `|| -1` bug fix only affects code that explicitly passes producerId=0, which doesn't happen without idempotent mode
- All existing 151 tests pass (2 pre-existing SSL failures)

### Verification Performed
- All protocol definitions serialize correctly (InitProducerId, FindCoordinator v1, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit)
- RecordBatch with producerId=0 correctly writes 0 (not -1), confirming bug fix
- RecordBatch with no producerId still writes -1 (default behavior preserved)
- Producer constructor correctly sets requiredAcks=-1 for idempotent mode
- Producer constructor correctly sets idempotent=true when transactionalId is provided
- Transaction state validation (beginTransaction, commitTransaction, abortTransaction) works correctly
- Unit tests: 57 passing — protocol serialize/deserialize, RecordBatch bug fix, error codes, API keys, Producer constructor, transaction state, Client fields, consumer isolationLevel
- Integration tests: 17 passing — idempotent producer obtains PID, sends messages, tracks sequences; transactional producer begin/commit/abort cycles, auto-partition registration; consumer isolation levels
- Full test suite: 225 passing, 2 failing (pre-existing SSL connection failures)

### Known Limitation
- `isolationLevel=1` (read_committed) correctly tells the broker to limit the fetch to the Last Stable Offset, but the client does not yet filter aborted transaction messages using the `abortedTransactions` list from FetchResponseV4. Full client-side abort filtering is a future enhancement.

---

# Kafka v1.0 Protocol Version Bumps

## Tasks

- [x] 1. Add `KafkaStorageException` (code 56) to `lib/errors.js`
- [x] 2. Phase 0: Coordinator version discovery — call `apiVersionsRequest` on coordinator connections
- [x] 3. Phase 1: Produce v5 — protocol definitions + client routing
- [x] 4. Phase 1: Fetch v6 — protocol definitions + client routing
- [x] 5. Phase 2: Metadata v5 — protocol definitions + client routing
- [x] 6. Phase 2: ListOffsets v2 — protocol definitions + client routing
- [x] 7. Phase 2: OffsetCommit v3 — protocol definitions + client routing
- [x] 8. Phase 2: OffsetFetch v2/v3 — protocol definitions + client routing
- [x] 9. Phase 3: JoinGroup v1 — protocol definitions + client routing
- [x] 10. Phase 4: Throttle-time bumps — Heartbeat/LeaveGroup/SyncGroup v1
- [x] 11. Phase 4: Throttle-time bumps — DescribeGroups/ListGroups v1
- [x] 12. Phase 4: ApiVersions v1 — protocol definitions + client routing
- [x] 13. Write unit tests in `test/14.protocol_v10.js`

## Review

### Summary of Changes

**New files (1):**
- `test/14.protocol_v10.js` — 32 unit tests covering all v1.0 protocol additions

**Modified files (9):**
- `lib/errors.js` — Added `KafkaStorageException` (code 56)
- `lib/protocol/produce.js` — Added `ProduceRequestV5`, `ProduceResponseV5` (with `logStartOffset`)
- `lib/protocol/fetch.js` — Added `FetchRequestV5PartitionItem` (with `logStartOffset`), `FetchRequestV5TopicItem`, `FetchRequestV6`, `FetchResponseV5` (with `logStartOffset`)
- `lib/protocol/metadata.js` — Added `MetadataRequestV5` (with `allowAutoTopicCreation`), `MetadataResponseV5` (with `throttleTime`, `clusterId`, `offlineReplicas`)
- `lib/protocol/offset.js` — Added `OffsetRequestV2` (with `isolationLevel`), `OffsetResponseV2` (with `throttleTime`)
- `lib/protocol/offset_commit_fetch.js` — Added `OffsetCommitRequestV3` (retentionTime still in wire format, broker ignores), `OffsetCommitResponseV3` (with `throttleTime`), `OffsetFetchResponseV2` (with top-level `ErrorCode`), `OffsetFetchResponseV3` (with `throttleTime` + `ErrorCode`), `OffsetFetchRequestV3` (supports null topics)
- `lib/protocol/group_membership.js` — Added `JoinConsumerGroupRequestV1` (with `rebalanceTimeout`), `HeartbeatRequestV1`/`ResponseV1`, `LeaveGroupRequestV1`/`ResponseV1`, `SyncConsumerGroupRequestV1`/`ResponseV1`
- `lib/protocol/admin.js` — Added `ListGroupsRequestV1`/`ListGroupResponseV1`, `DescribeGroupRequestV1`/`DescribeGroupResponseV1`
- `lib/protocol/api_versions.js` — Added `ApiVersionsRequestV1`/`ApiVersionsResponseV1` (with `throttleTime`)
- `lib/client.js` — Phase 0: coordinator version discovery (`apiVersionsRequest` on group/transaction coordinator connections); bumped `clientMax` for all APIs (Produce 3→5, Fetch 4→6, Metadata 1→5, ListOffsets 1→2, OffsetCommit 2→3, OffsetFetch 1→3, JoinGroup 0→2, Heartbeat 0→1, LeaveGroup 0→1, SyncGroup 0→1, ListGroups 0→1, DescribeGroups 0→1); stores `clusterId` from metadata v5; accepts optional `rebalanceTimeout` in `joinConsumerGroupRequest`

### Backward Compatibility
- All changes use version negotiation — clients connecting to older brokers automatically fall back to previously supported versions
- No behavioral changes for existing users; new fields (logStartOffset, clusterId, offlineReplicas, throttleTime) are only present when the broker supports them
- The `joinConsumerGroupRequest` signature adds an optional 5th parameter (`rebalanceTimeout`); existing callers are unaffected

### Bug Fixes Applied During Integration Testing
1. **FetchRequestV6 missing `logStartOffset` per partition** — Fetch v5+ adds `logStartOffset` (Int64) to the partition request item. Added `FetchRequestV5PartitionItem` and `FetchRequestV5TopicItem` with the correct format.
2. **OffsetResponseV1 used for v2 responses** — ListOffsets v2 response adds `throttleTime` before the topics array. Created `OffsetResponseV2` with throttleTime, updated client to use it.
3. **OffsetCommitRequestV3 incorrectly removed `retentionTime`** — The v3 wire format still includes `retentionTime` (the broker ignores it). Re-added the field with default -1.
4. **OffsetFetchResponseV2 used for v3 responses** — OffsetFetch v3 response adds `throttleTime` before the topics array. Created `OffsetFetchResponseV3` with throttleTime, updated client to use it.
5. **MetadataRequestV5 missing `allowAutoTopicCreation`** — Metadata v4+ adds this boolean field. Added it with default value 1 (true).

### Verification
- 257 passing, 2 failing (pre-existing SSL connection failures on unconfigured port 9093)
- All 32 v1.0 unit tests pass
- All 46 v0.11 unit tests pass
- All integration tests pass (SimpleConsumer, GroupConsumer, GroupAdmin, Compression, v0.11 Integration, Idempotent/Transactional)
