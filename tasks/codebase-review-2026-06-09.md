# Full Codebase Review — Bugs & Security (2026-06-09)

Five parallel review agents covered: (A) connection/client networking, (B) protocol core,
(C) group/admin/transaction protocol, (D) consumer classes, (E) producer/errors/utils.
Top Critical findings were independently re-verified against the source before inclusion.

Verdict: protocol wire formats are accurate across a large version matrix, but there are
10 Critical issues — 4 security (DoS / TLS), 6 correctness (data loss / process crash /
permanent wedge). The idempotent/transactional producer path and the new share-consumer
loop lifecycle are the weakest areas.

---

## CRITICAL

### Security

**S1. Unvalidated frame length → infinite loop or process crash** — `lib/connection.js:221-234`
`length = data.readInt32BE(dataOffset)` is never checked. `length = -4` makes both guards
pass and `dataOffset += 4 + (-4) = 0` → event loop hard-hangs. With exactly 4 bytes
buffered, `readInt32BE(dataOffset + 4)` throws a RangeError inside the socket `data`
handler → uncaught exception, process crash. Reachable by a malicious/buggy broker or a
MITM (plaintext is the default transport, see S2). *(verified)*
Fix: validate `length` (positive, below a max-frame cap) and force-disconnect on violation.

**S2. Silent TLS → plaintext downgrade** — `lib/connection.js:85`
TLS is used only when `(ssl.cert && ssl.key) || ssl.ca`. A broker with a publicly-trusted
cert (no client cert, no custom CA) cannot be reached over TLS; `ssl: true` or a typo'd ssl
object silently falls back to `net.connect` plaintext. Fails open instead of closed. *(verified)*
Fix: any truthy `ssl` option must use `tls.connect`; never fall back to plaintext.

**S3. Decompression bombs — no output limit on any codec** — `lib/protocol/misc/compression.js:50-104`
gzip (`zlib.gunzip` without `maxOutputLength`), snappy, lz4, zstd all decode whatever the
compressed frame declares. Compressed batch bytes are controllable by **any producer on a
topic this client consumes** — a ~1 MB bomb expands to multi-GB and OOM-kills the consumer.
Fix: pass `maxOutputLength` to zlib; enforce a configurable cap for the other codecs.

**S4. Unbounded response buffering** — `lib/connection.js:141-145, 205-212`
A broker can declare a frame length up to 2^31-1 and stream it; no `maxResponseSize` cap,
buffer grows to demand (O(n²) copying) and never shrinks. Memory DoS.
Fix together with S1 (max-frame cap), grow geometrically, reset when drained.

### Correctness

**C1. Six wrong error-code mappings** — `lib/errors.js:59, 74, 97-100` *(verified against Kafka Errors.java)*
- 52 is TRANSACTION_COORDINATOR_FENCED, not UnsupportedCompressionType (real one is 76, present as `UnsupportedCompressionType2`)
- 67 is INVALID_PRINCIPAL_TYPE; PRODUCER_FENCED is 90
- 90 is PRODUCER_FENCED; TRANSACTION_ABORTABLE is 120
- InvalidRecordState / ShareSessionNotFound / InvalidShareSessionEpoch are 121/122/123, not 91/92/93
Impact: zombie-fencing (code 90) is misreported as "TransactionAbortable"; real share-group
session errors (122/123) fall through `byCode` to a plain Error with no `.code`, so any
`err.code === 'InvalidShareSessionEpoch'` handling can never fire. Codes 94-119 and 124+
are also unmapped (Important E8).

**C2. Idempotent sequences re-stamped on every retry** — `lib/producer.js:179-191, 256` *(verified)*
`item._baseSequence = self.sequenceNumbers[key]` runs unconditionally inside `_try`, which
re-enters on retry. A retried batch gets new, higher sequences → fatal
OUT_OF_ORDER_SEQUENCE_NUMBER; or if the first attempt persisted but the response was lost,
the retry bypasses broker dedup → duplicates. The DuplicateSequenceNumber handling at
producer.js:228 can never fire as designed. Fix: stamp once, reuse on retry (Java client behavior).

**C3. Sequences consumed even when the attempt rejects → partition wedged** — `lib/producer.js:182-191, 221-222, 270-272`
Sequences increment before `produceRequest`; if it rejects wholesale they're lost and every
later idempotent send to that partition fails OutOfOrderSequenceNumber until restart. No
re-InitProducerId recovery exists. Related: no in-flight serialization per partition —
overlapping `_send` chains can reorder stamped batches on the wire (same fatal error).

**C4. Unhandled rejections in heartbeat loops crash the process** — `lib/group_consumer.js:330-355`, `lib/consumer_group.js:125-127+193-196`, `lib/share_consumer.js:130-132+207-212`
`self._heartbeatPromise = self._heartbeat()` inside setTimeout attaches no catch; terminal
errors (FencedInstanceId rethrow; join attempts exhausted) become unhandledRejection →
process exit on Node 18 defaults. Fix: route terminal loop failures to an error surface, never let them escape a timer.

**C5. ShareConsumer duplicates fetch loops on every rejoin** — `lib/share_consumer.js:122-123, 211, 286-289`
`_join()` on UnknownMemberId restarts BOTH loops, but the old fetch loop self-perpetuates
via setTimeout. Each rejoin adds a concurrent fetch loop forever: duplicate ShareFetch,
racing shareSessionEpoch increments (guaranteed INVALID_SHARE_SESSION_EPOCH), duplicate
handler invocations. Fix: start the fetch loop only once.

**C6. JoinGroup member metadata envelope bytes not skipped → leader misparse in mixed-client groups** — `lib/protocol/group_membership.js:271-276, 634-639, 716-721`
Modern Java/librdkafka members send ConsumerProtocolSubscription v2/v3 (generationId,
rackId); unconsumed bytes desync every subsequent member in the response → garbage
assignments or RangeError during rebalance when a no-kafka member is leader. The correct
skip pattern already exists at `admin.js:445-449`. Same class of bug in DescribeGroups
v0-v4 (`admin.js:204-208, 364-368`).

---

## IMPORTANT

### Networking (lib/connection.js, lib/client.js)
- **N1.** No per-request timeout; `_disconnect` race can strand queue entries that are never rejected → callers hang forever (connection.js:118-139, 167-196).
- **N2.** `close()` during in-flight connect leaks the socket; `onConnect` doesn't check `closed` (connection.js:79-83, 147-152).
- **N3.** `init()` overwrites the cert/key readFile promise when `ca` is also a file path — TLS may handshake with path strings; dropped promise is an unhandled rejection (client.js:127-144).
- **N4.** Transient `apiVersionsRequest` failure sets `apiVersions = {}` permanently → all APIs negotiate v0 forever; Kafka 4.x brokers close the connection on v0 (client.js:356-360, 2132-2139).
- **N5.** `produceRequest`/`offsetRequest`/v0-commit deref a possibly-undefined connection after concurrent metadata refresh → TypeError bypasses the retry path; `fetchRequest` has the guard, produce doesn't (client.js:805, 1204, 1226, 1250).
- **N6.** `_sendToAnyBroker` only ever uses `initialBrokers`; if bootstrap brokers are decommissioned, metadata/coordinator discovery is dead unless `rebootstrap: true` (client.js:385-419).
- **N7.** Decompression failure on the legacy MessageSet path returns `undefined` into the flattened messageSet → TypeError in base_consumer.js:92 (client.js:1072-1077). The RecordBatch path gets it right (returns []).
- **N8.** Coalesced `updateMetadata` ignores the new caller's topicNames → spurious UnknownTopicOrPartition under concurrency (client.js:291-293, 534-541).
- **N9.** `updateGroupCoordinator` stale-cache window: cache key deleted only after the promise resolves (client.js:1370-1400, 2608-2622).

### Protocol (lib/protocol/*)
- **P1.** CRC is read but never verified on fetched batches (RecordBatch v2 and legacy) — corrupted data delivered silently (common.js:574, 372-377).
- **P2.** Legacy magic-1 message format misparsed (timestamp bytes consumed as key length) — affects pre-0.11 format topics (common.js:368-377; reachable via fetch.js:165, 253).
- **P3.** Negative `messageSetSize` on fetch v0-v3 path rewinds the reader (`skip` has no bounds check) and desyncs the parse (fetch.js:55-62, common.js:397-398).
- **P4.** `OffsetCommitRequestV1` encodes generationId as string, spec says int32 — dead code today, guaranteed wire break if used (offset_commit_fetch.js:78).
- **P5.** Negotiated fetch v9 sends a hardcoded v10 request (`fetchVersion >= 9` → FetchRequestV10) — benign with real brokers but fragile (client.js:963-976, fetch.js:360-381).
- **P6.** `uuid.write` doesn't validate 16-byte decode — malformed topicId silently corrupts the whole request frame (common.js:299-307).

### Consumers (lib/base_consumer.js, group_consumer.js, consumer_group.js, share_consumer.js)
- **D1.** Handler rejection still advances the consume offset → silent message loss (at-most-once, not at-least-once) (base_consumer.js:84-93). ShareConsumer does this correctly via RELEASE.
- **D2.** KIP-848 reconciliation failures swallowed and never retried — broker only resends assignment on change (null = unchanged), so a transient subscribe/metadata error stalls the partition until the next group-wide change (consumer_group.js:166-168, 281-285). Java client re-reconciles every cycle against a stored target. Same class: group_consumer.js:481-483.
- **D3.** Classic group leader TypeError when another member subscribes to a topic absent from leader metadata → leader bricks in a `_fullRejoin` loop (group_consumer.js:148-166).
- **D4.** Drained share acks destroyed if the piggybacking ShareFetch fails — RELEASEs/REJECTs silently lost, records stuck until lock timeout (share_consumer.js:239-247, 281-283).
- **D5.** Share session epoch: reset races in-flight fetch; no handling of ShareSessionNotFound/InvalidShareSessionEpoch (compounded by C1 — those codes aren't even mapped); single global epoch shared across per-broker sessions (share_consumer.js:166, 249-254, 331-346).
- **D6.** ShareConsumer never recovers from FencedMemberEpoch — retries the same stale epoch forever; only UnknownMemberId triggers rejoin (share_consumer.js:204-221). consumer_group.js:174 handles it correctly.
- **D7.** `end()` doesn't cancel in-flight join/rejoin → ghost member squats in the group until session timeout (consumer_group.js:110-118; group_consumer.js:283-313; share_consumer.js:111-124).

### Producer (lib/producer.js)
- **E1.** commit/abortTransaction don't flush queued batches first → records produced after EndTxn leak into the next transaction or fail InvalidTxnState (producer.js:360-407).
- **E2.** Retriable set too narrow: RequestTimedOut, NetworkException, NotEnoughReplicas(±AfterAppend), KafkaStorageException, FencedLeaderEpoch all surface as terminal (producer.js:235). Only safe to widen after C2.
- **E3.** `new errors.NoKafkaConnectionError('Producer closed')` — argument order wrong, signature is (server, message) (producer.js:467).
- **E4.** Retry-exhausted messages pushed as `{error}` only — topic/partition identity lost (producer.js:259-261).

---

## MINOR (selected)
- `__proto__` is a legal Kafka topic name; plain-object accumulator maps (`topicMetadata`, `groupBy`, owned/assignment maps) malfunction on it. Use `Object.create(null)` (client.js:339-341; utils.js:57-65; base_consumer.js:49-51; consumer_group.js:324-326; share_consumer.js:312-316). No actual prototype pollution — DoS/malfunction only.
- `leader === -1` string-vs-number dead checks (client.js:842, 2755, 2854).
- `parseHostString` drops hosts without an explicit port instead of defaulting 9092 (client.js:238-243).
- EndTxn v5 `producerId` kept as a Long, not `.toNumber()`d like initProducerId (client.js:2455-2457).
- KIP-848/932 heartbeats hardcode v1 with no version negotiation → opaque failure on older brokers (client.js:2694, 2721).
- Empty compact-bytes member-metadata envelope reads past bounds (`_subLength > 0` should be `n > 0`) (group_membership.js:709-715; admin.js:437-444).
- SyncGroup v4/v5 writes null compactBytes for non-nullable Assignment (group_membership.js:866-869).
- OffsetCommitRequestV2 `Int64BE(retentionTime)` throws if undefined; v3 defaults to -1, v2 should too (offset_commit_fetch.js:97).
- Tagged-field sub-parses don't bound to declared `_tagSize` — future broker additions desync the reader (fetch.js:568-577, 763-774; produce.js:436-525).
- Sync decompress can throw instead of rejecting (compression.js:118-139).
- `misc/crc32.js:84` `Math.abs(crc ^ -1)` is not standard CRC-32 — fine for the partitioner it serves, must never be reused for integrity (add a comment; do NOT change, would reshuffle partition affinity).
- WRR strategy: undefined weight overwrites default (weighted_round_robin.js:19-24); unguarded JSON.parse of member metadata can wedge the leader (consistent.js:21, weighted_round_robin.js:21).
- group_admin lag never computed for offset 0 (`> 0` should be `>= 0`) (group_admin.js:73).
- ShareConsumer sends leave even if never joined (`if (self.memberId)` is always true; use `memberEpoch > 0`) (share_consumer.js:424).
- ConsumerGroup.commitOffset has no epoch guard during fenced rejoin (consumer_group.js:366-370); GroupConsumer has the analog.
- `unsubscribe()` can throw synchronously (base_consumer.js:294-296); `init()` not idempotent/close-aware (base_consumer.js:34-39).
- mapConcurrent with undefined concurrency → NaN → silently resolves array of undefined (promise-utils.js:30-60).
- KafkaError/NoKafkaConnectionError carry no stack trace (`Error.captureStackTrace` commented out) (errors.js:108, 165).
- Default logLevel 5 (trace) in production (logger.js:10).
- Batch hash omits compressionLevel → different-level sends merge (producer.js:296-303); `dataSize` ignores keys/headers and uses string length not byte length; no queue growth bound (producer.js:321-323).
- `defaultsDeep`/send mutate caller-supplied option and message objects; `_baseSequence` leaks into results (producer.js:11, 289; utils.js:7-23).
- `_transactionV2` decided from any initial broker's versions, not the txn coordinator's (producer.js:104-115).
- types/: producer init() return type, retries shape, missing compressionLevel; `const enum ACKNOWLEDGE_TYPE` unusable under isolatedModules; types/index.d.ts re-exports a Client never exposed at runtime.
- Mixed-version clusters: fan-out requests encoded once at the max version any initial broker supports (client.js:427-431, 1411-1415, 2147-2151).

---

## Suggested fix order
1. **S1+S4** frame-length validation + max-frame cap (one change in `_receive`) — crash/hang/DoS.
2. **S2** TLS fail-closed.
3. **C1** errors.js code corrections (+ fill 94-129) — small, unblocks D5/D6 handling.
4. **S3** decompression output caps.
5. **C2+C3** producer sequence lifecycle (stamp-once, rollback, recovery) — required before anyone trusts idempotent mode; then E1, E2.
6. **C4** loop rejection handling (all three consumers).
7. **C5, D4-D6** share consumer loop/session/ack lifecycle.
8. **C6** JoinGroup envelope skip — interop for mixed-client groups.
9. **D1** (decide policy: redeliver vs document), **D2**, **D3**.
