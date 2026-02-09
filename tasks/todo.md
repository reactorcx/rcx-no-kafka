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
