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
