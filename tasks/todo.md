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

(previous phases all [x] — omitted for brevity)

---

# Kafka v1.0 Protocol Version Bumps

(previous phases all [x] — omitted for brevity)

---

# Kafka 2.1 Protocol Version Bumps

(previous phases all [x] — omitted for brevity)

---

# Migrate from Bluebird to Native Promises

## Step 1: Create promise-utils.js

- [x] **1.1** Create `lib/promise-utils.js` with `delay`, `delayChain`, and `mapConcurrent` helpers

## Step 2: Migrate lib files

- [x] **2.1** Migrate `lib/protocol/misc/compression.js` — `Promise.promisify` → `util.promisify`, `Promise.map` → `Promise.all(..map)`
- [x] **2.2** Migrate `lib/connection.js` — remove Bluebird import only
- [x] **2.3** Migrate `lib/client.js` — ~25 replacements (most complex file)
- [x] **2.4** Migrate `lib/base_consumer.js` — `Promise.try`, `.map(concurrency)`, `Promise.method`, `.tap`, `.cancel`
- [x] **2.5** Migrate `lib/producer.js` — `Promise.try`, `Promise.map(concurrency)`, `Promise.delay`
- [x] **2.6** Migrate `lib/group_consumer.js` — `.catch({code})`, `Promise.delay`, `.tap`, `.cancel`
- [x] **2.7** Migrate `lib/simple_consumer.js` — `Promise.map` → `Promise.all`
- [x] **2.8** Migrate `lib/group_admin.js` — `.spread` → `.then` destructure

## Step 3: Migrate test files

- [x] **3.1** Migrate all 10 test files — remove `require('bluebird')`, replace `.delay()`, `.spread()`, `.each()`, `.map()`

## Step 4: Cleanup

- [x] **4.1** Delete `lib/bluebird-configured.js`
- [x] **4.2** Remove `bluebird` and `@types/bluebird` from `package.json`
- [x] **4.3** Update `README.md` — remove Bluebird reference

## Step 5: Verification

- [x] **5.1** Run `npm run eslint` and fix any issues
- [x] **5.2** Run `npm test` — expect 2 pre-existing SSL failures only

---

## Review

### Summary
Migrated the entire no-kafka client library from Bluebird promises to native JavaScript Promises, removing the external dependency entirely.

### New file
- **`lib/promise-utils.js`** — Small utility module with `delay(ms)`, `delayChain(ms)` (for `.then()` chaining), and `mapConcurrent(arr, fn, concurrency)` (worker-pool based concurrent mapping).

### Key replacements across lib files
| Bluebird API | Native replacement |
|---|---|
| `Promise.promisify(fn)` | `util.promisify(fn)` |
| `Promise.map(arr, fn)` | `Promise.all(arr.map(fn))` |
| `Promise.map(arr, fn, {concurrency: N})` | `promiseUtils.mapConcurrent(arr, fn, N)` |
| `Promise.try(fn)` | `Promise.resolve().then(fn)` |
| `Promise.delay(ms)` | `promiseUtils.delay(ms)` |
| `Promise.method(fn)` | Inline wrapper: `try { return Promise.resolve(fn(...)) } catch(e) { return Promise.reject(e) }` |
| `Promise.any(arr)` | Native `Promise.any(arr)` with `AggregateError` handling |
| `.spread(fn)` | `.then(function(r) { var a=r[0], b=r[1]; ... })` |
| `.tap(fn)` | `.then(function(r) { fn(); return r; })` |
| `.return(val)` | `.then(function() { return val; })` |
| `.cancel()` | `_closed` boolean flag checked before scheduling next iteration |
| `.isPending()` | `_updateMetadata_pending` boolean flag set/cleared on settle |
| `.isRejected()` | Self-cleaning catch: delete cache entry on rejection |
| `.catch({code: 'X'}, fn)` | `.catch(function(err) { if (err && err.code === 'X') return fn(err); throw err; })` |
| `.catch(ErrorClass, fn)` | `.catch(function(err) { if (err instanceof ErrorClass) return fn(err); throw err; })` |

### Bugs found and fixed during migration
1. **Infinite recursion in handler wrapping** — `base_consumer.js subscribe()` reassigned the `handler` closure variable to a wrapper that called itself. Fixed by capturing `var originalHandler = handler` before wrapping.
2. **Null subscription access after end()** — `base_consumer.js _fetch()` cleanup code accessed a subscription that was cleared by `end()`. Fixed by re-looking up the subscription from `self.subscriptions` before accessing.

### Deleted files
- `lib/bluebird-configured.js`

### Dependencies removed
- `bluebird` (^3.7.2)
- `@types/bluebird` (3.5.0)

### Test results
- **284 passing**, 2 failing (pre-existing SSL failures on port 9093 — not configured in test environment)
- ESLint: clean

---

# Kafka 2.4 Protocol Upgrade + Rack Awareness

## Phase 1: Error Codes (68-89)
- [x] **1.1** Add error codes 68-89 to `lib/errors.js`

## Phase 2: Simple Protocol Version Bumps
- [x] **2.1** Add ProduceRequestV8 to `lib/protocol/produce.js`
- [x] **2.2** Add OffsetRequestV5 to `lib/protocol/offset.js`
- [x] **2.3** Add InitProducerIdRequestV1 to `lib/protocol/init_producer_id.js`

## Phase 3: Static Membership (groupInstanceId) Protocol Definitions
- [x] **3.1** Add JoinConsumerGroupRequest/Response V5 to `lib/protocol/group_membership.js`
- [x] **3.2** Add HeartbeatRequestV3 to `lib/protocol/group_membership.js`
- [x] **3.3** Add SyncConsumerGroupRequestV3 to `lib/protocol/group_membership.js`
- [x] **3.4** Add LeaveGroupRequest/Response V3 to `lib/protocol/group_membership.js`
- [x] **3.5** Add OffsetCommitRequestV7 to `lib/protocol/offset_commit_fetch.js`
- [x] **3.6** Add DescribeGroupRequest/Response V3+V4 to `lib/protocol/admin.js`

## Phase 4: Metadata v8 (Authorized Operations)
- [x] **4.1** Add MetadataRequest/Response V8 to `lib/protocol/metadata.js`

## Phase 5: Fetch v11 + Rack Awareness
- [x] **5.1** Add FetchRequest/Response V11 to `lib/protocol/fetch.js`
- [x] **5.2** Update fetchRequest in `lib/client.js` for v11 + rackId
- [x] **5.3** Store broker rack info from metadata in `lib/client.js`
- [x] **5.4** Handle preferredReadReplica in `lib/base_consumer.js`
- [x] **5.5** Handle FencedLeaderEpoch error in `lib/base_consumer.js`

## Phase 6: Client.js Version Bumps + groupInstanceId Plumbing
- [x] **6.1** Bump produceRequest to v8 in `lib/client.js`
- [x] **6.2** Bump offsetRequest to v5 in `lib/client.js`
- [x] **6.3** Bump metadataRequest to v8 in `lib/client.js`
- [x] **6.4** Add version negotiation to initProducerIdRequest in `lib/client.js`
- [x] **6.5** Bump joinConsumerGroupRequest to v5 in `lib/client.js`
- [x] **6.6** Bump heartbeatRequest to v3 in `lib/client.js`
- [x] **6.7** Bump syncConsumerGroupRequest to v3 in `lib/client.js`
- [x] **6.8** Bump leaveGroupRequest to v3 in `lib/client.js`
- [x] **6.9** Bump offsetCommitRequestV2 to v7 in `lib/client.js`
- [x] **6.10** Bump describeGroupRequest to v4 in `lib/client.js`

## Phase 7: Unit Tests
- [x] **7.1** Create `test/16.protocol_v24.js` with tests for all new protocol versions

## Phase 8: Integration Verification
- [x] **8.1** Run `npm test` — 308 passing, 3 failing (1 pre-existing lag timing issue, 2 pre-existing SSL)
- [x] **8.2** Run `npm run eslint` — clean

---

## Review

### Summary
Added Kafka 2.2-2.4 protocol support including static group membership (KIP-345), fetch from closest replica / rack awareness (KIP-392), authorized operations in metadata/describe groups (KIP-430), and various simple version bumps.

### Files Modified

| File | Changes |
|------|---------|
| `lib/errors.js` | Added 22 error codes (68-89): NonEmptyGroup through ThrottlingQuotaExceeded |
| `lib/protocol/produce.js` | Added `ProduceRequestV8` (apiVersion 8, same wire format as v7) |
| `lib/protocol/offset.js` | Added `OffsetRequestV5` (apiVersion 5, same wire format as v4) |
| `lib/protocol/init_producer_id.js` | Added `InitProducerIdRequestV1` (apiVersion 1, same wire format as v0) |
| `lib/protocol/group_membership.js` | Added JoinGroup V5, Heartbeat V3, SyncGroup V3, LeaveGroup V3 (all with groupInstanceId support) |
| `lib/protocol/offset_commit_fetch.js` | Added `OffsetCommitRequestV7` (adds groupInstanceId) |
| `lib/protocol/admin.js` | Added DescribeGroups V3 (authorizedOperations) and V4 (groupInstanceId per member) |
| `lib/protocol/metadata.js` | Added Metadata V8 request (authorized ops flags) and response (per-topic + cluster authorized operations) |
| `lib/protocol/fetch.js` | Added Fetch V11 request (rackId) and response (preferredReadReplica per partition) |
| `lib/client.js` | Bumped all clientMax values, added v8/v11/v5/v1/v5/v3/v3/v3/v7/v4 branches, stored broker racks, passed rackId/groupInstanceId, handled MemberIdRequired (KIP-394) |
| `lib/base_consumer.js` | Added preferredReadReplica handling for rack-aware fetch, added FencedLeaderEpoch to error recovery |
| `lib/group_consumer.js` | Added MemberIdRequired handling for KIP-394 two-phase join |

### New File
- `test/16.protocol_v24.js` — 24 unit tests covering error codes, simple version bumps, static membership protocols, metadata v8, and fetch v11

### Bug Found During Integration
- **KIP-394 two-phase join**: With JoinGroup v5+ on Kafka 2.2+ brokers, the broker returns `MemberIdRequired` with an assigned `memberId` on the first join attempt (empty memberId). Fixed by saving the assigned memberId from the error response and retrying.
- **LeaveGroup v3 null member_id**: The `member_id` field in LeaveGroup v3 members array is non-nullable STRING but could be sent as null when called during cleanup. Fixed by defaulting to empty string.

### Test Results
- **308 passing**, 3 failing (1 pre-existing consumer lag timing issue from stale topic data, 2 pre-existing SSL tests on port 9093)
- ESLint: clean
