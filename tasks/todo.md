# Kafka no-kafka Client - Implementation Plan

(Previous completed phases omitted for brevity — see git history)

---

# Codebase audit 2026-11-14: bugs, security, performance

Audited `bug-jira-CORE-4239` @ c525fec. Six parallel reviewers (client/connection, protocol,
consumers, producer/utilities, produce-path performance, consume-path performance); the findings
marked **[R]** below were re-run or re-measured by the lead reviewer, **[read]** were confirmed by
reading the code, unmarked items rest on the sub-reviewer's own reproduction.

No Kafka broker was running, so nothing here was tested end to end. Claims about what a broker does
with a malformed or unexpected request come from reading apache/kafka 4.1/4.2 source.

## Headline

The June 9 audit's fixes are NOT on this branch or on `master`. They exist only on
`feature-KIP-848` (PR #3, open), in 8 commits bundled with the KIP-848 feature. Outside
`base_consumer.js` / `group_consumer.js`, `lib/` here is byte-identical to the tree that audit
reviewed, so all of its findings are still live, including the 4 critical security ones.
None of the new findings below are fixed on `feature-KIP-848` either.

The June findings are in `git show feature-KIP-848:tasks/codebase-review-2026-06-09.md`
and are referenced below by their IDs (S1-S4, C1-C6, N1-N9, P1-P6, D1-D7, E1-E4).

## Decision (2026-11-15: option A chosen)

How should the June fixes reach `master`?

- **A (recommended): port them to a new branch off `master`, separate from KIP-848.** Four of the
  eight commits cherry-pick cleanly (8e5c785 connection, 0a84a89 decompression, 4bc186f producer
  sequences, 09773ab protocol). Four conflict (3c6d8db errors.js, 656ec3b client.js, 798a353
  consumers, c0284d8 misc) and need hand-merging; 798a353 overlaps this branch's drain work.
- B: merge PR #3 as is. Ships the security fixes together with an unreviewed feature.
- C: leave it and only fix the new findings. Not recommended: S1/S2 are exploitable today.

## Phase 0 — June fixes (after the decision above)

- [x] 0.1 Cherry-pick the 4 clean commits: 8e5c785 (S1, S2, S4, N1, N2), 0a84a89 (S3),
      4bc186f (C2, C3, E1-E4), 09773ab (C6, P1-P6)
- [x] 0.2 Hand-port 3c6d8db (C1 error codes) and 656ec3b (N3-N9 client resilience)
- [x] 0.3 Hand-port 798a353 onto the reworked consumers: C4 (detached heartbeat promise exits the
      process on FencedInstanceId), C5, D1 (handler rejection advances the offset), D3-D7.
      All re-confirmed still present at HEAD.
- [x] 0.4 Hand-port c0284d8 (minor hardening, types)
- [x] 0.5 `npm audit fix` — lodash (high) and protocol-buffers-schema (moderate) reach production
      through `bin-protocol`. The other 25 advisories are dev-only (mocha 3, eslint 2, istanbul 0.4).
- [ ] 0.6 Follow-up: the June S2 fix still connects in plaintext for `ssl: { rejectUnauthorized: true }`
      or `{ servername }` alone (only `ssl: true`, `ssl.enabled`, cert+key or ca select TLS).

Done on branch `fix-audit-2026-06-port` (off `master`): all 8 commits cherry-picked in order with `-x`.
Against `master` only errors.js (took the corrected table), client.js (dropped the KIP-848
`consumerGroupHeartbeatRequest` hunk), consumer_group.js (KIP-848 only, removed) and todo.md
conflicted; base_consumer/group_consumer changes applied identically to the original commit.
KIP-848 ConsumerGroup tests removed from test 35/38. `lib/` + `types/` now equal
`feature-KIP-848` minus exactly the KIP-848 feature. Lockfile resynced; prod deps 0 advisories.
PR #2 (CORE-4239) conflicts with this branch in base_consumer.js and group_consumer.js (verified with
`git merge-tree`); resolve when one of the two lands.

Still-live June criticals re-run at HEAD **[R]**: S1 frame length -4 hangs the event loop, length 0
throws inside the socket handler (`lib/connection.js:221`); S2 `ssl: true`, `ssl: {}`,
`{rejectUnauthorized: true}` and `{servername}` all connect in plaintext (`lib/connection.js:85`);
C1 code 90 maps to TransactionAbortable and 120-123 are unmapped.

## Phase 1 — New data-loss and ordering bugs

Consume path (one function, `_decompressRecordBatches`, `lib/client.js:2041-2077`):
- [ ] 1.1 **[R]** A batch that fails to decode is replaced by `[]`; later batches are delivered and
      the offset moves past it. Missing optional codec (snappy is only a devDependency) is enough.
      Fix: surface it as the partition's `error`, deliver nothing after the failed batch.
      (Contradicts June N7, which called this path correct.)
- [ ] 1.2 **[R]** Records below the requested fetch offset are delivered (brokers return whole
      batches). Fix: pass the requested offset in, drop `record.offset < fetchOffset`.
- [ ] 1.3 **[R]** Transaction control markers are delivered as messages (`attributes & 0x20` never
      checked). Fix: skip control batches.
- [ ] 1.4 **[R]** `isolationLevel: 1` does not filter aborted records; `abortedTransactions` is
      parsed and never read. Fix: implement the Java client's filter, or stop documenting it.
- [ ] 1.5 **[R, single step]** Position advances from the last record, not the batch's
      `lastOffsetDelta`, so a compacted batch is refetched and redelivered. Fix: return
      `nextOffset` from the decode and use it in `base_consumer.js:96`. Needed by 1.3.
- [ ] 1.6 **[R]** `[].concat.apply([], r)` overflows the stack above ~110k batches (8.6 MB of
      single-record batches), rejecting the fetch forever. Unreachable at 1 MB `maxBytes`;
      reachable at PR #4's 25 MB default. Fix: `.flat()` at `lib/client.js:2076` and `:1077`.

Consumers:
- [ ] 1.7 **[R]** A partition with no leader when a rebalance completes is never consumed:
      `findLeader` reports `UnknownTopicOrPartition`, the subscribe failure is swallowed
      (`lib/group_consumer.js:666-668`), and in cooperative mode it already counts as owned.
      Fix: store the same retry placeholder `_subscribePartition` uses for LeaderNotAvailable.
- [ ] 1.8 **[R]** A fetch response is applied to whichever entry holds the key when it arrives
      (`lib/base_consumer.js:70`). After a rebalance re-subscribes at the committed offset, an
      in-flight fetch moves the new entry past the uncommitted batch. Defeats CORE-4239's
      fallback. Fix: drop results whose entry is no longer the one the request was built from.

Producer:
- [ ] 1.9 **[R]** Leader lookup is per message; a partial failure splits one partition's batch,
      reordering it, and a retriable error then resends messages already queued for retry
      (15 sent, 25 produced). `lib/producer.js:174-175, 237`. Fix: retry the whole partition.
- [ ] 1.10 **[R]** No metadata refresh on retry, so a dead leader is retried forever
      (`lib/producer.js:253-257`; removed upstream in 2018). Fix: `updateMetadata(topics)` before
      the retry.
- [ ] 1.11 **[read]** Commit/abort resets `sequenceNumbers` even when EndTxn < v5 did not bump the
      epoch (`lib/producer.js:378, 405`) — brokers before 4.0. Fix: reset only on a new epoch.
- [ ] 1.12 Transaction state: partitions marked registered before AddPartitionsToTxn completes and
      never unmarked on failure (`:199-218`); `send()` outside a transaction is still flagged
      transactional (`lib/client.js:624`); a rejected EndTxn leaves `_inTransaction` stuck.
- [ ] 1.13 **[read]** All `send()` calls in one linger window share one promise and one result
      array (`lib/producer.js:305-336`): callers see each other's results and errors.
- [ ] 1.14 Idempotent/transactional producer silently downgrades to legacy produce with no
      producerId when a leader's API versions are unknown (`lib/client.js:596, 665-675`).

## Phase 2 — New security and robustness findings

- [ ] 2.1 **[R]** One lookup of a topic named `constructor` throws inside `updateMetadata` after
      the pending latch is set (`lib/client.js:295-296`); every later refresh sends nothing.
      A topic named `hasOwnProperty` breaks `getTopicPartitions` for all topics (`:528`).
      Fix: exception-safe latch plus null-prototype maps (wider than June's `__proto__` item).
- [ ] 2.2 **[R]** `ssl: {cert: <path>, key: <PEM text>}` puts the private key in the `init()`
      error message and `err.path` (`lib/client.js:127-137`). Fix: test cert and key separately.
- [ ] 2.3 **[R, partly]** Numeric message keys: `Buffer(key)` allocates N zero bytes, so the
      partition is computed from the wrong bytes, memory scales with the key's value, and negative
      keys throw (`lib/assignment/partitioners/default.js:34`). Fix: `Buffer.from(String(key))`.
      Changes key-to-partition mapping for anyone already sending numeric keys.
- [ ] 2.4 Sync throws inside the linger timer crash the process: a null element passed to
      `send()`, or >120k queued sends (`lib/producer.js:164, 320-323`).
- [ ] 2.5 **[read]** A failed metadata refresh rejects unrelated in-flight requests
      (`lib/client.js:375-383`). Fix: `.catch(noop)` in `_waitMetadata`.
- [ ] 2.6 Topic errors in metadata are cached as "zero partitions": partitioner returns NaN and is
      never recomputed; `subscribe()` resolves with nothing (`lib/client.js:334-342`).
- [ ] 2.7 A deleted and recreated topic stays broken until restart: the stale topic ID is re-sent
      and re-cached (`lib/client.js:436-447`). Broker half read from source.
- [ ] 2.8 Group-coordinator cache never invalidated for `sendOffsets` and GroupAdmin; transaction
      coordinator sockets dropped without `close()` (`lib/client.js:2340, 2398, 2462, 2471`).
- [ ] 2.9 ShareConsumer: **[read]** top-level ShareFetch error discarded (`lib/client.js:2754`);
      delivers records outside `acquiredRecords` (`lib/share_consumer.js:261`); drops queued acks
      on `end()` and for partitions absent from the next fetch; never fetches an assigned
      partition missing from cached metadata.
- [ ] 2.10 `GroupConsumer.end()` skips `client.end()` when LeaveGroup fails
      (`lib/group_consumer.js:583-586`); `onPartitionsAssigned` is called unguarded (`:715, 803,
      816`); `revokeTimeout` above 2^31-1 becomes a 1 ms drain.
- [ ] 2.11 Minor protocol: MetadataResponse v13 omits the top-level ErrorCode (**[R]** against the
      4.2 spec); InitProducerId v6 request paired with the v2 response reader; negotiated Fetch v5
      sends v6; LogAppendTime ignored; async lz4/zstd/snappy decode can throw synchronously;
      legacy MessageSet path hands a shared writer buffer to async compression.
- [ ] 2.12 Types and docs: `Result` has no `error` field although failures resolve as `{error}`;
      `SendOptions` lists options `send()` ignores; `asyncCompression` documented false, default
      true; `sendOffsets` signature and next-offset semantics; GroupAdmin and partitioner typings.

Design question for CORE-4239, not a plain bug: the drain budget opens before JoinGroup
(`lib/group_consumer.js:326-328`), so a slow JoinGroup or the 1 s retry backoffs can spend it and
the revoke callback is then not awaited. The comment at `:349` records this as deliberate
(commit 6590f49). Alternative: charge only time spent inside drains.

## Phase 3 — Performance (never audited before)

Consume:
- [ ] 3.1 **[read]** The fetch loop sleeps `idleTimeout` (1000 ms) after every round, including a
      full one (`lib/base_consumer.js:107-113`). Reviewer measured 1.0 MB/s at defaults against
      36.5 MB/s without the sleep. Fix: no delay after a round that delivered messages.
- [ ] 3.2 **[R]** Record decode is ~2 us/record, 74% of it in bin-protocol/lodash: 21 ms per
      10,000 records, one synchronous stretch. Reviewer's direct reader (~35 lines in
      `lib/protocol/common.js`) gave ~4 ms with identical output.
- [ ] 3.3 **[read]** Request-level fetch `maxBytes` equals the per-partition value and partition
      order never rotates (`lib/client.js:856` + 11 siblings): one backlogged partition can starve
      the rest on that broker.
- [ ] 3.4 RecordBatch header parse 8.8 us per batch (`lib/protocol/common.js:567-635`).
- [ ] 3.5 A leadership change triggers N/10 sequential metadata refreshes per fetch round
      (100 requests at 1,000 partitions). Fix: memoise per topic per round.

Produce:
- [ ] 3.6 **[R]** CRC-32C is a byte-at-a-time loop at ~300 MB/s (`lib/protocol/misc/crc32c.js`):
      33 of 47 ms for a 10 MB request. Reviewer's slicing-by-8 version was ~5x faster.
- [ ] 3.7 **[R]** `Record.write` costs ~1.3 us/record regardless of size. Reviewer's direct write
      reached 0.18 us with byte-identical output.
- [ ] 3.8 **[read]** `batch.size` (16 KB) counts bytes across the whole producer, not per
      partition (`lib/producer.js:321-325`), so multi-partition sends fragment into 1-2 record
      batches. Reviewer measured 2.3x less CPU at 256 KB with 12 partitions.
- [ ] 3.9 11-19 promises per message in leader lookup (`lib/producer.js:120-157`).
- [ ] 3.10 Every protocol read/write goes through lodash-bound wrappers (bin-protocol `define`).

Not recommended now: one fetch loop per leader (structural, touches the drain invariants);
producer in-flight backpressure (behaviour change); fetch sessions (negligible below ~1,000
partitions).

Reviewer's combined produce prototypes: 2.7x-5.8x faster, 379 broker-free tests passing. Not re-run.

## Interaction with open PRs

- PR #4 (25 MB `maxBytes`): makes 1.6 reachable by default, and with 3.2 unfixed a full response of
  small records blocks the event loop for roughly half a second (extrapolated). Land 1.6 first.
- PR #2 (this branch): 1.7 and 1.8 are in the code it touches; 1.8 undermines its guarantee.

## Verification plan

- Per fix: a broker-free unit test in the style of `test/31`-`test/33`, written first and seen to
  fail. The reviewers' repro recipes are the starting point.
- Per phase: `npm run eslint`, then the broker-free files with `_mocha`.
- Before merge: full `npm test` against the Docker broker (`./test/setup-topics.sh` first). The
  broker was not running during this audit.
- Performance items: re-run the before/after micro-benchmarks and compare encoder output bytes.

## Review

(to be filled in as phases complete)
