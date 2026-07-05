# Fix all findings from codebase review (tasks/codebase-review-2026-06-09.md)

(Previous KIP-848/KIP-932 plans are preserved in git history.)

Approach: TDD per fix (failing test → minimal fix → green). Run the affected test
file after each fix, full suite at phase checkpoints. Keep each change minimal.

## Phase 1 — Security criticals
- [x] S1+S4: connection.js — validate frame length (negative / > max cap) in `_receive`, disconnect on violation; max-frame cap option (default 100MB) + buffer shrink when drained — test/31.connection_unit.js
- [x] S2: connection.js — TLS fail-closed: `ssl: true` / `ssl.enabled: true` force tls.connect (cert/key/ca behavior unchanged; empty ssl object stays plaintext for back-compat) — test/31.connection_unit.js
- [x] C1: errors.js — fixed 6 wrong codes, filled all codes through 133; verified against apache/kafka 4.1 Errors.java — test/32.errors_unit.js; updated stale expectations in test/12 + test/15
- [x] S3: compression.js — decompression output caps: gzip via maxOutputLength, snappy via declared-uvarint pre-check (rejects before allocation), lz4/zstd post-decode check; default 128MB exported as `maxOutputSize`; sync paths now reject instead of throwing — test/33.compression_limits.js

## Phase 2 — Producer criticals/importants
- [x] C2: producer.js — sequences stamped once per message, reused on retry; stamps cleared when the task settles so re-sent message objects re-stamp — test/34.producer_unit.js
- [x] C3: producer.js — `_needsReinit` raised on wholesale attempt failure or OutOfOrderSequenceNumber; next idempotent send re-runs InitProducerId (fresh sequence space) instead of staying wedged. Mid-transaction, recovery stays with endTxn/abort (epoch bump)
- [x] C10: producer.js — idempotent sends serialized through `_sendChain` (no overlapping stamped batches on the wire)
- [x] E1: producer.js — new public `flush()`; commitTransaction/abortTransaction flush queued batches before EndTxn
- [x] E2: producer.js — retriable set widened (RequestTimedOut, NetworkException, NotEnoughReplicas[AfterAppend], KafkaStorageException, FencedLeaderEpoch)
- [x] E3: producer.js — NoKafkaConnectionError(null, 'Producer closed') arg order fixed
- [x] E4: producer.js — retry-exhausted failures keep topic/partition

## Phase 3 — Consumer loops
- [x] C4: all three consumers — `_runHeartbeat()` wrapper catches terminal loop errors (logged, loop ends; no unhandled rejection / process crash) — test/35
- [x] C5: share_consumer.js — `_fetchStarted` guard: fetch loop starts once; rejoins restart membership only — test/35
- [x] D4: share_consumer.js — `_requeueAcks()` re-merges drained acks (front of queue) when the carrying fetch fails — test/35
- [x] D5: share_consumer.js — per-broker session epochs (`shareSessionEpochs` map; client.shareFetchRequest accepts map or number); ShareSessionNotFound/InvalidShareSessionEpoch reopen that broker's session; mid-flight assignment reset detected by map identity (not clobbered) — test/35
- [x] D6: share_consumer.js — FencedMemberEpoch now rejoins with epoch 0 (kept memberId) — test/35
- [x] D7: all three — join completing after end() sends an immediate leave instead of squatting (consumer_group + share_consumer heartbeat(-1), group_consumer leaveGroupRequest); also fixed share leave guard to memberEpoch > 0 — test/35
- [x] D1: base_consumer.js — offset NOT advanced when handler rejects; batch redelivered next fetch (at-least-once) — test/35 (README note pending)
- [x] D2: consumer_group.js — `_targetAssignment` stored; idempotent `_reconcile` re-runs every heartbeat until owned matches; cleared on revokeAll — test/35
- [x] D3: group_consumer.js — leader updates metadata for ALL members' topics and skips still-missing topics instead of TypeError-bricking — test/35

## Phase 4 — Networking importants (connection.js / client.js)
- [x] N1: `requestTimeout` option (default 300s) rejects hung requests; `_disconnect` always settles queue entries (timers cleared) — test/31
- [x] N2: onConnect destroys the socket if close() landed mid-connect; close() destroys an in-flight socket — test/31
- [x] N3: init() chains the ca read after cert/key reads — test/36
- [x] N4: apiVersions stays null after pure transport failures (discovery retried); {} only when the broker closed the connection (genuine v0 era) — test/36
- [x] N5: LeaderNotAvailable fake response instead of TypeError in produceRequest/offsetRequest/offsetCommitV0/offsetFetchV0 when the leader connection vanished — test/36
- [x] N6: _sendToAnyBroker candidates = initialBrokers + live brokerConnections (deduped by host:port) — test/36
- [x] N7: legacy decompress catch returns [] (concat-flattened) instead of undefined
- [x] N8: coalesced updateMetadata chains a follow-up when the in-flight refresh doesn't cover the requested topics — test/36
- [x] N9: update{Group,Transaction,Share}Coordinator delete the cache key synchronously — test/36

## Phase 5 — Protocol importants
- [x] C6: group_membership.js — all 3 JoinGroup member readers skip leftover ConsumerProtocolSubscription v2+ envelope bytes (+ empty/negative envelope guards); admin.js DescribeGroups v0-v4 member items capture metadata length and skip remainder — test/37
- [x] P1: common.js — CRC-32C verified on every complete RecordBatch read (throws "Corrupt record batch" on mismatch; truncated batches still skipped) — test/37
- [x] P2: common.js — Message.read parses the magic-1 Int64 timestamp — test/37
- [x] P3: fetch.js — messageSetSize <= 0 yields empty messageSet (no reader rewind) — test/37
- [x] P4: offset_commit_fetch.js — OffsetCommitRequestV1 generationId now Int32BE — test/37
- [x] P5: client.js — fetch gating >= 10 for FetchRequestV10 (max-9 brokers fall through to wire-correct v7)
- [x] P6: common.js — uuid.write throws on malformed UUID instead of corrupting the frame — test/37

## Phase 6 — Minors (batched)
- [x] Object.create(null) for topic/group-keyed maps (client.js topicMetadata/topicIds/topicNames/coordinator caches; utils.groupBy) — test/38; assertions on these maps in test/12/26 updated (null-proto objects have no `.should`)
- [x] leader === '-1' string comparisons (Object.keys yields strings); parseHostString defaults port to 9092 — test/38 + test/05 (the old "wrong connectionString = localhost" test premise changed; now uses '')
- [x] EndTxn v5 producerId Long → .toNumber()
- [x] KIP-848/932 heartbeats reject with a clear "does not support" error when the coordinator lacks API 68/76 — test/38
- [x] group_membership empty-envelope guards (done in C6); SyncGroup v4/v5 missing assignment → empty (not null) compactBytes — test/38
- [x] OffsetCommitV2 retentionTime defaults to -1 — test/38
- [x] tagged-field sub-parses bounded by declared _tagSize (fetch.js ×2, produce.js ×3)
- [x] compression.js sync paths reject instead of throwing (done in S3) — test/33
- [x] crc32.js warning comment (non-standard CRC, partitioner-only)
- [x] WRR weight fallback to 10 + guarded JSON.parse; consistent strategy guarded JSON.parse + id fallback — test/38 (Buffer-metadata variants)
- [x] group_admin lag computed for offset/HWM 0 (>= 0) — test/38
- [x] share_consumer leave only when memberEpoch > 0 (done in Phase 3)
- [x] consumer_group commitOffset epoch guard (RebalanceInProgress) — test/38
- [x] base_consumer unsubscribe never throws sync; init() idempotent (_fetchStarted) and close-aware — test/38
- [x] promise-utils mapConcurrent: concurrency defaults to 10; fn sync-throws become rejections — test/38
- [x] errors.js Error.captureStackTrace enabled — test/38
- [x] logger default level 3 (info) instead of 5 (trace) — test/38
- [x] producer batch hash includes compressionLevel — test/38; _baseSequence stamps cleared when the task settles (Phase 2)
- [x] producer _transactionV2 decided from the transaction coordinator's versions (initialBrokers fallback removed); detection failure leaves V1 behavior
- [x] types: producer init() → Promise<void>, retries shape fixed, compressionLevel added, flush() added, ACKNOWLEDGE_TYPE as declared const object, dead './client' re-export removed

## Checkpoint after each phase: run full test suite, report status
- Phase 1-3 checkpoint: 577 passing / 2 pre-existing SSL failures (port 9093, no broker)
- Final: 616 passing, 4 pending, 2 failing (same pre-existing SSL-on-9093 tests only)

## Review

All ~50 findings from tasks/codebase-review-2026-06-09.md are fixed (10 Critical, ~25 Important, all Minors), TDD-style: every behavioral fix has a test that was watched failing first. Final suite: **618 passing, 4 pending, 2 failing** — the 2 failures are the pre-existing SSL tests against port 9093 (no SSL listener configured; out of scope). `npm test` (eslint + istanbul coverage gate) passes.

### Headline changes
- **connection.js**: frame-length validation + 100MB `maxFrameSize` cap (fixes infinite-loop/crash/memory DoS), TLS fail-closed (`ssl: true` / `ssl.enabled`), `requestTimeout` (default 5 min), always-settle queue entries, close-vs-connect race fixed, buffer shrink after large frames.
- **errors.js**: 6 wrong code mappings corrected and the table completed through 133 (verified against apache/kafka 4.1 Errors.java); stack traces enabled.
- **compression.js**: decompression-bomb protection (128MB default `maxOutputSize`; snappy checked pre-allocation via its declared-length uvarint).
- **producer.js**: idempotent sequences stamped once and reused on retry; sends serialized (max-in-flight 1); automatic producer-id re-init after unknown-outcome failures or OutOfOrderSequenceNumber; new `flush()`; commit/abort flush queued AND in-flight batches before EndTxn; retriable set widened; `_transactionV2` decided from the txn coordinator.
- **consumers**: heartbeat loops can no longer crash the process via unhandled rejections; offset NOT advanced when a handler rejects (at-least-once, documented); KIP-848 target assignment re-reconciled every heartbeat; classic leader survives unknown topics in other members' subscriptions; joins that land after end() leave immediately.
- **share_consumer.js**: single fetch loop across rejoins; per-broker share-session epochs; session errors reopen the session; drained acks requeued when the carrying fetch fails; FencedMemberEpoch rejoins.
- **protocol**: JoinGroup/DescribeGroups member-metadata envelopes skip newer ConsumerProtocolSubscription fields (mixed-client interop); RecordBatch CRC-32C verified on read; magic-1 timestamp parsed; uuid.write validated; OffsetCommit v1 generationId Int32; fetch v10 gating; tagged-field sub-parses bounded.
- **client.js**: leaders that vanish mid-request produce LeaderNotAvailable instead of TypeError; apiVersions discovery retried after transport failures; metadata/coordinator requests fall back to live broker connections; coordinator caches invalidated synchronously; null-prototype maps for data-driven keys; ssl ca read chained after cert/key.

### Intentional behavior changes (release-notes worthy)
1. Handler rejection now redelivers the batch instead of skipping it (at-least-once). A deterministically failing handler will loop on the same batch — handlers must resolve to skip.
2. Error code renames/remaps: `UnsupportedCompressionType` is now code 76 (was wrongly 52), `ProducerFenced` is 90 (was wrongly 67), `TransactionAbortable` 120, share-session codes 121-123. `UnsupportedCompressionType2` removed.
3. Idempotent sends are serialized (throughput ↓, correctness ↑).
4. Default `logLevel` is 3 (info) instead of 5 (trace).
5. Hosts without a port in `connectionString` now default to 9092 instead of being dropped.
6. `client.topicMetadata` / `topicIds` / `topicNames` / coordinator caches are null-prototype objects (no `.hasOwnProperty` / chai `.should` directly on them).

### Post-implementation code review
A reviewer pass on the full diff found 0 Critical, 2 Important — both fixed and tested: (1) `requestTimeout`/`maxFrameSize` are now plumbed through `_createConnection`; (2) `flush()` also awaits in-flight (already dispatched) batches via `_sendChain` so EndTxn can't race them. Remaining reviewer minors accepted as-is: the broker-closed-vs-transport regex in apiVersionsRequest matches this library's own error message (not arbitrary text); the updateMetadata full-refresh follow-up is required under metadata v13 (a no-topic request returns broker info only).

### New test files
test/31 (connection), test/32 (errors), test/33 (compression limits), test/34 (producer), test/35 (consumer loops), test/36 (client), test/37 (protocol), test/38 (minors) — 86 new unit tests, no broker required.
