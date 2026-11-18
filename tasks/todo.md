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

## PR #5 review (2026-11-16)

Adversarial review of 0209719, all four findings confirmed against the code before fixing:

- [x] R1 [P1] A message/batch that failed to decompress or decode was dropped and later records
      delivered, moving the offset past it (legacy `return []` added in the port; RecordBatch path
      too, now reachable via the 128 MB cap). Fetch now stops at the first failure and reports it as
      the partition error when it comes first (`_untilFailure`); README documents the stall. Share
      fetch still skips bad batches, and now also removes them from `acquiredRecords` so a handler
      acking by range can't accept records it never got.
- [x] R2 [P1] `commitTransaction()` committed after a send in the transaction had failed (flush
      discards outcomes; transport failures resolve as `result.error`). Now rejects; abort still works.
- [x] R3 [P2] `ACKNOWLEDGE_TYPE` lost its type in the .d.ts (TS2749). Added the value-union type.
- [x] R4 [P2] Top-level ShareFetch errors (ShareSessionNotFound, InvalidShareSessionEpoch) were
      discarded. Now reported on that broker's partitions only (`_fakeTopicsErrorResponse`, as for
      connection errors), so `_partitionError` reopens just that session, other brokers' records
      are delivered, and that partition's acks are resent. Throwing instead (the reviewer's
      suggestion) would discard every broker's records for the round. Found while fixing: the
      broker rejects acks on an epoch-0 request (INVALID_REQUEST, `SharePartitionManager.newContext`),
      so resent acks would loop forever; acks for a broker whose session is opening now wait one
      fetch (Java's `ShareConsumeRequestManager` never sends acks on a new session either). Also
      fixed: fake share errors lost their partition number in the flatten.

Found by the code reviews of these fixes (security, maintainability, style):
- [x] R5 [high] A decode failure is now retried by re-subscribing, and each re-subscribe re-wrapped
      the handler: stack overflow after ~6,000 retries (~1.5 h). Ported Phase 1's `_promiseWrapped`
      guard and its test.
- [x] R6 A CRC-32C mismatch threw during response parsing, failing the fetch for every broker (the
      check came with the June port). The batch is now marked and stops only its partition.
- [x] R7 Acks resent once per partition, before handling (a partition listed twice doubled them,
      and a rejecting `_partitionError` skipped the rest), and only where the broker sent no answer.
- [x] R8 `_withoutRanges` was quadratic for a hostile broker (40k ranges x 4k batches: 14.8 s);
      now one sorted sweep (18 ms), checked against a brute-force model.
- Not fixed here: per-record objects aren't bounded by `maxOutputSize` (~4 GB heap from one
  ~190 KB batch). Pre-existing; it is 2.14.

Follow-up review of 384cdbd (2026-11-16):
- [x] R9 [P1] Share fetch cut a failed batch's range out of `acquiredRecords` using that batch's
      header, but a CRC failure means the header (e.g. `lastOffsetDelta`) may be what is corrupt:
      a shrunk delta left undelivered offsets acknowledgeable. `acquiredRecords` now keeps only
      offsets inside batches that decoded (CRC-verified headers) via `_withinRanges`, which
      replaces `_withoutRanges`. Regression corrupts `lastOffsetDelta`, not just the payload.
      Our review of this found the same hole for a truncated trailing batch (dropped by the
      reader, its offsets still acquired) when no batch failed; the trim now always runs.
      Its security review then found: (a) trimming cut compaction gaps between good batches,
      which a range-acking handler then can't ack (locked until the 30 s timeout); spans are now
      runs of consecutive decoded batches, gaps included, the first run open at the start; and
      (b) overlapping `acquiredRecords` made the output ranges x batches (6000x6000: 10.8 s,
      3.4 GB); overlaps are now clipped (1 ms). Both checked against a brute-force model.
- [x] R10 (merge review) The R2 commit guard missed a failed `sendOffsets()`: commit then made the
      records visible without the consumer offsets, so the input is processed twice. It now sets
      the same flag (Java's producer also refuses the commit). The rejected-send branch of the
      guard had no test; one test now covers both.
- [x] R11 (merge review) R6's CRC marking made a corrupt-batch flood expensive: one Error (with
      stack) and one error-level log line per batch. 200,000 corrupt empty batches in one share
      partition: 4.7 s, +799 MB heap, 226 MB of log output. Now one Error per read, no per-batch
      log; the fetch path reports the failure as the partition error (already logged by the
      consumer) and the share path logs once per partition: 940 ms, +190 MB, 1 line, about the
      same as that payload with valid CRCs (943 ms, +177 MB).

Follow-up review of c3f7d59 (2026-11-17):
- [x] R12 [P2] Commit and abort don't wait for a pending `sendOffsets()`, so transaction A's late
      offset-write failure set the failed flag during transaction B and B's commit was refused
      (R10 introduced the flag write). A `sendOffsets()` failure now counts only against the
      transaction it was called in (`_txnSeq`, bumped by `beginTransaction()`); a failed offset
      write leaves nothing in B. Sends deliberately keep the original rule (a send failing while
      a transaction is open fails it): attributing them too was tried, and our reviews showed a
      send dispatched before `beginTransaction()`, retried after an abort, or still on its way
      can write into the next transaction, which the client can't detect. A wrong refusal costs
      an abort and retry; a wrong commit exposes a partial transaction. Regressions: late offset
      failure (V1, V2), a send registering into B, a send failing before registration, and a
      send in flight across B's start (refused). Verification: the offset regressions fail on
      c3f7d59; unit tests 31-38: 105 pass; full suite on a fresh 4.2.0 broker: 616 pass, same 10
      failures as `master`. (`_txnSeq` was replaced by R13.)

Follow-up review of b2d4b3a (2026-11-17):
- [x] R13 [P1] R12's guard let a V1 `sendOffsets()` from aborted transaction A still send its
      TxnOffsetCommit inside B (its AddOffsetsToTxn answered late), and ignored that write's
      partial failure: B committed A's offset 100 instead of its own 50 (reviewer, live on 3.9.2).
      Root cause: commit and abort didn't wait for pending offset writes. They now do
      (`_offsetWrites`, a chain that never rejects), so A's write lands in A and is aborted; `_txnSeq`
      is gone since no offset write can outlive its transaction. While a commit or abort runs
      (`_endingTxn`), `send()`, `sendOffsets()` and a second commit/abort reject: they would miss
      the wait, and overlapping ends would clear each other's flag (our maintainability review).
      Our security review found that a synchronous throw from `flush()` would leave that flag set:
      `concat.apply` in `_send` overflows the stack at ~125k empty-value sends in one batch (also a
      process crash from the `maxWait` timer). Took Phase 1's `task.data.flat()`.
      Regressions: AddOffsetsToTxn itself held (V1) or TxnOffsetCommit held (V2), with calls during
      the abort rejected and offsets usable after a commit; commit waits for an earlier, slower
      write; a send racing an abort (taken from Phase 1). Verification: live 3.9.2 repro commits
      100 on b2d4b3a and 50 with the fix; removing any of the 7 pieces fails a test; unit tests
      31-38: 105 pass; eslint 0 errors; tsc passes; full suite on a fresh 4.2.0 broker: 616 pass,
      same 10 failures as `master`; 130k empty sends then commit: `RangeError` on b2d4b3a, commits
      with the fix.
- Noted, not fixed (pre-existing, outside this PR): under V1 a timed-out offset write's outcome
  is unknown (the broker may still apply it after the client gave up; V2's epoch bump fences it);
  the share handler also gets records outside
  `acquiredRecords` (the Java client filters them); a legacy magic 0/1 share response skips the
  trim (Kafka 4.x share brokers never send one).

## Phase 1 — New data-loss and ordering bugs

Consume path (one function, `_decompressRecordBatches`, `lib/client.js:2041-2077`):
- [x] 1.1 **[R]** A batch that fails to decode is replaced by `[]`; later batches are delivered and
      the offset moves past it. Missing optional codec (snappy is only a devDependency) is enough.
      Fix: surface it as the partition's `error`, deliver nothing after the failed batch.
      (Contradicts June N7, which called this path correct.)
- [x] 1.2 **[R]** Records below the requested fetch offset are delivered (brokers return whole
      batches). Fix: pass the requested offset in, drop `record.offset < fetchOffset`.
- [x] 1.3 **[R]** Transaction control markers are delivered as messages (`attributes & 0x20` never
      checked). Fix: skip control batches.
- [x] 1.4 **[R]** `isolationLevel: 1` does not filter aborted records; `abortedTransactions` is
      parsed and never read. Fix: implement the Java client's filter, or stop documenting it.
- [x] 1.5 **[R, single step]** Position advances from the last record, not the batch's
      `lastOffsetDelta`, so a compacted batch is refetched and redelivered. Fix: return
      `nextOffset` from the decode and use it in `base_consumer.js:96`. Needed by 1.3.
- [x] 1.6 **[R]** `[].concat.apply([], r)` overflows the stack above ~110k batches (8.6 MB of
      single-record batches), rejecting the fetch forever. Unreachable at 1 MB `maxBytes`;
      reachable at PR #4's 25 MB default. Fix: `.flat()` at `lib/client.js:2076` and `:1077`.

Consumers:
- [x] 1.7 **[R]** A partition with no leader when a rebalance completes is never consumed:
      `findLeader` reports `UnknownTopicOrPartition`, the subscribe failure is swallowed
      (`lib/group_consumer.js:666-668`), and in cooperative mode it already counts as owned.
      Fix: store the same retry placeholder `_subscribePartition` uses for LeaderNotAvailable.
- [x] 1.8 **[R]** A fetch response is applied to whichever entry holds the key when it arrives
      (`lib/base_consumer.js:70`). After a rebalance re-subscribes at the committed offset, an
      in-flight fetch moves the new entry past the uncommitted batch. Defeats CORE-4239's
      fallback. Fix: drop results whose entry is no longer the one the request was built from.

Producer:
- [x] 1.9 **[R]** Leader lookup is per message; a partial failure splits one partition's batch,
      reordering it, and a retriable error then resends messages already queued for retry
      (15 sent, 25 produced). `lib/producer.js:174-175, 237`. Fix: retry the whole partition.
- [x] 1.10 **[R]** No metadata refresh on retry, so a dead leader is retried forever
      (`lib/producer.js:253-257`; removed upstream in 2018). Fix: `updateMetadata(topics)` before
      the retry.
- [x] 1.11 **[read]** Commit/abort resets `sequenceNumbers` even when EndTxn < v5 did not bump the
      epoch (`lib/producer.js:378, 405`) — brokers before 4.0. Fix: reset only on a new epoch.
- [x] 1.12 Transaction state: partitions marked registered before AddPartitionsToTxn completes and
      never unmarked on failure (`:199-218`); `send()` outside a transaction is still flagged
      transactional (`lib/client.js:624`); a rejected EndTxn leaves `_inTransaction` stuck.
- [x] 1.13 **[read]** All `send()` calls in one linger window share one promise and one result
      array (`lib/producer.js:305-336`): callers see each other's results and errors.
- [x] 1.14 Idempotent/transactional producer silently downgrades to legacy produce with no
      producerId when a leader's API versions are unknown (`lib/client.js:596, 665-675`).

PR #6 review of 586a588 (2026-11-17):
- [x] P1 Abort after a commit with an unknown outcome reported success. 1.12's abort recovery
      took `InvalidTxnState` to mean "never opened", but the broker also answers it once the
      commit went through, so an app retrying after the "abort" duplicated its output. Now a
      failed EndTxn(commit) sets `_commitFailed`; an abort that then gets `InvalidTxnState`
      rejects and says to retry the commit (Java refuses that abort too). Live, with the commit
      response dropped on 3.9.2 (V1) and 4.2.0 (V2): 586a588's abort resolves while
      read_committed sees the record; the fix refuses, the retried commit succeeds, and the record
      is seen once.
- [x] P2 A failed send left a sequence gap in the next V1 transaction. 1.11 keeps sequences across
      transactions on the same epoch, but a stamped batch that ran out of retries or was rejected
      reserved sequences the broker never appended, and nothing set `_needsReinit`, so the next
      transaction got OutOfOrderSequenceNumber. Such a batch now sets it; abort re-initializes.
- [x] P2 read_committed decompressed aborted batches before dropping them, so an aborted batch
      over `maxOutputSize` or with a missing codec stalled the partition. Control batches are
      decoded first, the aborted ones found from the headers (as in `CompletedFetch`), and those
      are dropped without decompressing; their CRC is still checked.
- Our review of these fixes:
  - [x] Re-initializing right after a V1 EndTxn can meet ConcurrentTransactions (the coordinator
        answers before it writes the markers), and InitProducerId wasn't retried, so the abort
        rejected after ending and the gap stayed. `_initProducerId` now retries it (with the two
        coordinator-loading errors `init()` already retried), as Java's InitProducerIdHandler does.
  - [x] After a failed commit, a `send()` or `sendOffsets()` failing because the commit did go
        through left the producer unable to commit or abort. Both now reject until the commit is
        retried (Java refuses sends while committing too).
  - [x] Test that `_commitFailed` is cleared once the transaction ends.
  - [x] If that re-init still failed (connection lost), the commit or abort rejected though it had
        gone through: a retried commit duplicates the output, and the next V1 transaction's sends
        reused the gapped sequences. Commit and abort now resolve and log it; `_needsReinit`
        stays set and a transaction's first send re-initializes while nothing is registered yet.
- Codex review (gpt-6.1-sol, xhigh), round 1:
  - [x] P2 A `send()` rejected for a bad topic (validated per call since 4a1ac1d) didn't fail its
        transaction, so the commit published the rest; the base refused it. It sets `_txnFailed`
        now, as Java's `doSend` does.
  - [x] P2 A transaction starting with `sendOffsets()` registered the offsets before the owed
        re-init, so its sends reused the gapped sequences. With a re-init owed, `sendOffsets()`
        queues it on `_sendChain` and registers only after it ran.
- Codex round 2 and the PR #6 re-review of 9711afd (2026-11-17): no new findings.
- Verification: each new test fails on 586a588 (4); removing any of the 10 new pieces fails a
  test; the CRC check moved after the drop fails the new decode test; live on 3.9.2 and 4.2.0
  with the commit response dropped: 586a588's abort resolves after the commit went through, the
  fix refuses it and the retried commit succeeds; our differential fuzz of the decode rewrite
  against 586a588 (6,000 runs): identical except that undecodable aborted batches no longer stall.
- Noted, not fixed:
  - EndTxn doesn't retry ConcurrentTransactions (Java does; pre-existing). An abort or commit retry
    sent while the coordinator is still writing the commit's markers rejects with it (seen live on
    3.9.2 and 4.2.0); the app has to retry.
  - A non-transactional idempotent producer now takes a new producer id after each batch that
    failed with sequences stamped (it used to fail the next send with OutOfOrderSequenceNumber).
    Java rewinds sequences for batches the broker definitely rejected; add that if failures are
    frequent.

## Phase 2 — New security and robustness findings

Branch `fix-audit-phase2`, off `fix-audit-phase1` (PR #6) at aa086b0. Re-checked against this
branch on 2026-11-17 and reviewed by a Fable 5.2 agent (xhigh); its corrections are folded in and
the main ones spot-checked. Line numbers are this branch's.

Already fixed by Phase 0/1: the top-level ShareFetch error (`lib/client.js:2957`), the
`getTopicPartitions` own-property check, null-prototype client maps, `utils.groupBy`, producer
`newTopics`, and both 2.4 crashes (`send()` rejects a null element; `.flat()` replaced the
overflowing `apply`). Not on this branch: `revokeTimeout` (2.10) and the CORE-4239 drain-budget
question. Dropped: 2.11e (async snappy is already wrapped in `attempt()`, `compression.js:193`) and
2.8's ApiVersions leak (`apiVersionsRequest` never rejects).

Each item: a broker-free test written first and seen failing, then the fix, one commit per item.
Order: 2.1, 2.15, 2.2, 2.4, 2.3, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.11, 2.13, 2.12.

- [x] 2.1 **[R]** `updateMetadata` sets its pending latch, then calls `metadataRequest` synchronously
      (`lib/client.js:313-314`). A non-array `topicNames` throws there (`ShareConsumer.init({topics:
      'name'})` reaches it, `share_consumer.js:75` only checks `.length`); the latch stays set, later
      refreshes send nothing and `updateMetadata(['x'])` re-enters itself in microtasks, starving
      timers. Fix: start the chain with `Promise.resolve().then(_try)`. Test asserts a second
      `metadataRequest` call (the stuck path would hang mocha).
- [x] 2.15 Plain `{}` maps keyed by topic name (`__proto__`, `constructor` and `toString` are legal
      topic names) become `Object.create(null)`: `lib/base_consumer.js:54-55` (one subscription to
      `constructor` makes every fetch throw, so the consumer fetches nothing),
      `lib/group_consumer.js:147` (the group never stabilises), and `lib/share_consumer.js` fetch
      `retry`, `_buildFetchRequests` and `_drainPendingAcks` (writes `Object.prototype[partition]`).
- [x] 2.2 **[R]** If `ssl.cert` isn't PEM text, cert and key are both read as files
      (`lib/client.js:126-137`), so a PEM key lands in the ENOENT message and `err.path`.
      Fix: decide per value, with an unanchored PEM test (a template-literal PEM starts with `\n`).
- [x] 2.3 **[R]** `Buffer(key)` on a numeric key allocates that many zero bytes
      (`lib/assignment/partitioners/default.js:34`): key 5 and "5" go to different partitions though
      both are sent as "5"; negative, boolean and timestamp keys throw and reject the whole linger
      batch; key 314,572,800 took 327 ms and 360 MB. `HashCRC32Partitioner`
      (`hash_crc32.js:20`): numeric keys always give partition 0, Buffer keys throw.
      Fix: hash the bytes sent, `Buffer.isBuffer(key) ? key : Buffer.from(String(key))`, in both.
      **Decision (2026-11-17): fix it.** Numeric keys move partition (during a rolling deploy old
      and new producers split a key across two partitions; compacted topics keep stale values in
      the old one). README and release note: overriding `hashKey` restores the old mapping.
- [x] 2.4 Tests only: a null element and more than 120k queued sends (neither has one).
- [x] 2.5 **[read]** `_waitMetadata` returns the in-flight refresh (`lib/client.js:393-399`) and 11
      request paths start with it, so one failed refresh rejects unrelated requests.
      Fix: `.catch(function () { return null; })` there. Before 2.11a.
- [x] 2.6 Topic-level metadata errors are cached as zero partitions (`lib/client.js:352-364`): the
      partitioner returns NaN, which is never recomputed (`typeof NaN === 'number'`,
      `producer.js:179`; this also breaks the first send to an auto-created topic), and
      `subscribe()` resolves with nothing. Fix: don't cache an errored topic, log its error (it
      surfaces as UnknownTopicOrPartition, hiding e.g. TopicAuthorizationFailed).
- [x] 2.7 A deleted and recreated topic stays broken until restart:
  - [x] a. Metadata v12+ sends cached topic IDs (`lib/client.js:463-480`); any non-zero ID makes the
        broker ignore names (`KafkaApis.scala:889-903`), so the stale ID gets UNKNOWN_TOPIC_ID and
        named-only topics in the same request are dropped (a group leader refreshing `[cached A,
        new B]` never sees B). Fix: ask by name only, as Java does for explicit topics; remove the
        then-dead `:354-356`.
  - [x] b. The producer treats `UnknownTopicId` as fatal (`RETRIABLE_ERROR_CODES`,
        `producer.js:13`), so it never refreshes. Fix: add it and `InconsistentTopicId`, and check
        the retry refreshes metadata. (Consumers already refresh on both via
        `base_consumer.js:164`.)
  - [x] c. `ShareConsumer._partitionError` (`share_consumer.js:425`) doesn't refresh on either.
  Live check on 4.2.0: delete and recreate a topic under a running producer and consumer.
- [x] 2.8 Coordinators:
  - [x] a. Never invalidated: `txnOffsetCommitRequest` (`client.js:2658`), GroupAdmin, InitProducerId
        on the transaction coordinator (`:2391`), and `describeGroupRequest`, whose NotCoordinator
        is inside `groups[0].error` (`:2155`). Fix: one helper for "coordinator error or lost
        connection -> invalidate", used by these and the existing catch blocks.
  - [x] b. Transaction-coordinator errors `delete` the entry without closing its socket
        (`:2524, 2582, 2649`). Fix: the helper calls `updateTransactionCoordinator` (no callers yet).
  - [x] c. `_find*Coordinator`'s failure path deletes the cache entry unconditionally
        (`:1599, 2470, 2867`), possibly a newer one. Fix: delete only if it is still its own promise.
- [x] 2.9 ShareConsumer:
  - [x] a. Records outside `acquiredRecords` are delivered (`client.js:3039` trims the ranges to
        the decoded batches, not the records to the ranges; noted, not fixed, at the PR #5 review).
        Fix: filter each record to the acquired ranges, as Java's `ShareCompletedFetch` does.
  - [x] b. `end()` drops queued acks (`share_consumer.js:532`). Fix: send them with ShareAcknowledge
        at epoch -1 to each leader with an open session (the broker accepts it and closes the
        session; Java does the same on close), catch and warn so `client.end()` always runs.
        `shareAcknowledgeRequest` takes one epoch for all leaders today; give it a per-leader map
        like `shareFetchRequest`.
  - [x] c. Acks for a partition not in the next fetch are drained and never sent
        (`share_consumer.js:288`, `client.js:2937`). Fix: drain only acks the fetch will carry;
        acks for partitions no longer assigned go out with ShareAcknowledge (the 2.9b path).
  - [x] d. An assigned partition missing from cached metadata is never fetched
        (`share_consumer.js:383`). Fix: refresh metadata (by name, so after 2.7a) for assigned
        topics it lacks.
  Live check: acks queued before `end()` are applied.
- [x] 2.10 GroupConsumer:
  - [x] a. `end()` skips `BaseConsumer.end()` when LeaveGroup rejects
        (`group_consumer.js:403-406`), leaving sockets and timers. Fix: catch and warn as
        `ShareConsumer.end()` does; skip LeaveGroup when `memberId` is null (as `:320` does).
  - [x] b. `onPartitionsAssigned` / `onPartitionsRevoked` are called unguarded (`:475, 513, 561,
        571, 603`): a throw fails the rebalance and the consumer rejoins over and over (at `:561` it
        also skips `_needsRejoin` and `ownedPartitions`); a rejecting async callback is an unhandled
        rejection. Fix: one helper that logs both.
- [x] 2.11 Protocol:
  - [x] a. MetadataResponse v13's top-level ErrorCode (last field, after Topics) isn't read
        (`lib/protocol/metadata.js:506`; the comment at `:489` has its position wrong). Read it and
        fail the refresh before the broker list is replaced (`client.js:333`). Java acts only on
        REBOOTSTRAP_REQUIRED and only with rebootstrap enabled, so this is stricter (accepted
        2026-11-17); with 2.5 a
        failed refresh no longer breaks other requests. Update the `test/25` fixture.
  - [x] b. An InitProducerId v6 request is read with the v2 reader (`client.js:2379`) and fails
        ("Malformed varint"). v6 is unstable (brokers advertise it only with
        `unstable.api.versions.enable`). Fix: cap at v5, delete the v6 writer and its test.
  - [x] c. A negotiated Fetch v5 is sent as v6 (`client.js:1088-1089`); the wire format is the same
        but the version isn't (0.11.0 brokers). Fix: send v5.
  - [x] d. LogAppendTime batches (attributes bit 3) should give every record the batch
        `maxTimestamp`; ignored today. Fix at `client.js:2194`, where record timestamps are set.
  - [x] f. Legacy MessageSet compression hands a slice of the shared protocol writer to async
        compression (`client.js:2266`); the next request overwrites it first (reproduced).
        Fix: `Buffer.from(...)`, as `:718` does.
- [x] 2.12 Types and docs: `Result` has no `error` (`types/producer.d.ts:218`); `asyncCompression`
      is documented as defaulting to false (`README.md:274, 388, 510`; `types/producer.d.ts:186,
      339`; `group_consumer.d.ts:107`; `simple_consumer.d.ts:84`), the code says true; `SendOptions`
      fields `send()` ignores get a doc note (removing them breaks callers' compiles); `sendOffsets`
      typing; GroupAdmin methods typed as returning non-Promises (`group_admin.d.ts:9, 13, 17`);
      `Message.key` is `string` only (`kafka.d.ts:25`).
- [x] 2.13 **[read]** lz4 and zstd decode the whole batch before `checkOutput`
      (`lib/protocol/misc/compression.js:110-134`): a 32 KB zstd frame declaring 1 GiB took RSS
      48 MB -> 1,073 MB. Also a truncated lz4 frame crashes the process: the decoder emits `error`
      on the next tick with no listener.
      Fix: zstd streams through `zstd-napi/binding`'s `DCtx.decompressStream` with a context per
      call (one abandoned mid-frame corrupts the next decode), stops past `maxOutputSize`, and
      rejects a frame that doesn't end (`ret !== 0`; today a truncated one returns 0 bytes); lz4
      uses `lz4.createDecoderStream()` with a byte-counting listener and an `error` listener.
      Live check: RSS with the bomb, and the truncated frame.
- [x] 2.14 Moved to Phase 3 as 3.11 (decision 2026-11-17); documented in the README for now.
Verification: per item as above; then eslint, `tsc`, unit tests 31-39, full suite on a fresh 4.2.0
broker (same 10 failures as `master`), the live checks for 2.7, 2.9 and 2.13, review subagents
(security, maintainability, style), then Codex (gpt-6.1-sol, xhigh) until clean.

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

- [ ] 3.11 (from 2.14) The decompression cap is per batch with no total: 8 gzip batches in one
      1 MB partition fetch held ~1 GB. Budget `maxOutputSize` per partition response; needs
      sequential decode in `_decompressRecordBatches` (today `Promise.all` spends the memory before
      the truncation runs); later batches wait for the next fetch. Not a process bound (partitions x
      cap). Share fetch and the legacy MessageSet path stay out. Measure throughput before/after.

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

### Phase 0 (2026-11-15), branch `fix-audit-2026-06-port`

- 8 June commits plus bd04ed8 (test topic setup) cherry-picked off `master`; conflicts resolved as noted
  under Phase 0. Lockfile resynced (`npm audit` prod: 0).
- Review fixes: rewrote the end()-during-join test for GroupConsumer and ShareConsumer (it was
  ConsumerGroup-only); grew the receive buffer geometrically. The S1 shrink-after-drain made every
  large frame quadratic (50 MB: 2.9 s per frame -> 15 ms). Same change as PR #4.
- Verification: eslint 0 errors; unit tests 31-38: 85 pass; S1/S2/C1 repro re-run; full suite on a
  fresh 4.2.0 broker: same 10 failures as `master` on a fresh broker (SSL 9093 x2, group coordinator
  timeouts x7, producer unknown-topic x1), none new.
- Security review found 2.13-2.15 (not port regressions; added to Phase 2).

### PR #5 review fixes (2026-11-16), branch `fix-audit-2026-06-port`

- R1-R4 from the PR review and R5-R8 from our own review of the fixes (see "PR #5 review" above).
- Our review also caught a bug in the first R2 draft: successful produce results carry
  `error: null`, so an `!== undefined` check would have rejected every real commit. The broker
  run showed it (4 transactional integration tests); the unit test now uses the wire shape.
- Verification: each new test fails without its fix (9 in total); `tsc` 5.9.3 on `types/index.d.ts`
  passes (TS2749 before); eslint 0 errors; unit tests 31-38: 93 pass; full suite on a fresh 4.2.0
  broker: 604 pass, same 10 failures as `master`.
- Follow-up (R9 and its review fixes): each new test fails on 384cdbd (4); range helper matches a
  brute-force model on 20,000 random cases; unit tests 31-38: 97 pass; eslint 0 errors; full suite
  on a fresh 4.2.0 broker: 608 pass, same 10 failures as `master`.
- Merged into `fix-audit-phase1` (see below).

### Phase 1 (2026-11-16), branch `fix-audit-phase1` (stacked on PR #5)

- 1.1-1.8 (consume path): `_decompressRecordBatches` follows Java's CompletedFetch and returns
  `{messageSet, nextOffset, error?}`; `_fetch` uses `nextOffset` and drops responses for replaced
  subscription entries; `findLeader` reports leader -1 as LeaderNotAvailable. New broker test: a
  read_committed consumer gets only the committed record (old code delivered 4 values: the aborted
  record, 2 markers and the committed one).
- 1.9-1.14 (producer): partitions held back whole on partial leader failure; metadata refreshed
  before retries; per-send results; sequences reset only on an epoch bump; empty transactions skip
  EndTxn (broker answers InvalidTxnState, verified); AddPartitionsToTxn failure unmarks partitions;
  no legacy-format fallback for idempotent producers; transactional `send()` outside a transaction
  rejects (verified on 4.2: acknowledged then aborted).
- Review fixes: abort after failed AddPartitionsToTxn re-inits the producer id; aborting a txn the
  broker never opened completes; `send()` during commit rejects; per-call topic validation; position
  never moves backwards; aborted-txn match is linear; share fetch skips a bad batch; handler no
  longer re-wrapped per re-subscribe; prototype-free `newTopics`.
- Every fix has a unit test that fails without it. Verification: eslint 0 errors; unit tests 31-39:
  112 pass; full suite on a fresh 4.2.0 broker: 624 pass, same 10 failures as `master`.
- Still open (not done here): an undecodable batch halts its partition until fixed or skipped by
  hand (by design, matches Java, but the app only sees log lines) — consider an error callback.

### Merge of PR #5 fixes into `fix-audit-phase1` (2026-11-16)

- Phase 1's decoder kept (it already stops at the first failed batch, with nextOffset, marker and
  aborted-txn handling); the CRC marker is folded into its throw-safe decode. Legacy path takes
  PR #5's `_untilFailure`. Share path keeps Phase 1's per-batch decode and adds the
  `acquiredRecords` trim. Commit guard sits in front of Phase 1's `_endTransaction`; README keeps
  both transaction notes. Re-wrap guard and its test were identical on both sides (one copy kept).
- Merge review found R10 (`sendOffsets` failure not guarded) and R11 (corrupt-batch flood); both
  fixed on PR #5 (7b77519, c3f7d59) and merged in a second time. On this branch the share flood
  costs the same as valid data (200,000 batches: 1.1 s, ~390 MB either way); the extra heap over
  PR #5 is the per-batch share decode, a Phase 3 performance item, not corruption-specific.
- Also fixed in the merge: a pre-existing no-return-assign lint error in test/34 (Phase 1's
  "0 errors" note above was wrong).
- R12 (b2d4b3a) merged in a third time. Two of its send tests stage races this branch already
  prevents (`send()` rejects outside a transaction and while one is ending), so here: the
  send-before-`beginTransaction()` test is dropped (covered by "should reject a transactional
  send outside a transaction"), and the in-flight-across-start test checks that a send racing an
  abort is rejected and the next transaction commits.
- R13 (4b9bb3d) merged in a fourth time. Commit and abort keep `_endTransaction()` and now also
  wait for `_offsetWrites`. A send while one is ending is rejected by PR #5's guard ("Transaction
  is being committed or aborted"); the outside-a-transaction check no longer repeats it, and
  "should reject a send made while the transaction is being committed" expects the new message.
  `task.data.flat()` was already here.

### Phase 2 (2026-11-17), branch `fix-audit-phase2` (stacked on PR #6)

- One commit per item (2.1-2.13, 2.15), each with a broker-free test written first and seen
  failing; 2.14 moved to Phase 3 (3.11) and documented in the README. Decisions: numeric keys
  move partition (CHANGELOG, README `hashKey` override that keeps the old mapping, checked to
  give the old partitions); a Metadata v13 top-level error fails the refresh.
- Plan reviewed first by a Fable 5.2 agent (xhigh): one "already fixed" claim was wrong (share
  records outside `acquiredRecords`, now 2.9a), two problems were not real here (2.11e, 2.8's
  ApiVersions leak), and several missed sites were added (2.15 base_consumer, CRC32 partitioner,
  four coordinator paths, async rebalance callbacks, the lz4 truncated-frame crash).
- Found while fixing: the 150k-send test first ran out of heap because its stub returned one
  result per message (real produce returns one per partition); 40k sends take 96 ms, linear.
  zstd-napi's shared decoder, left mid-frame by a truncated batch, failed every later zstd decode
  in the process (now reset after each call). lz4 can't decode concatenated frames, before or
  after (Kafka writes one per batch).
- Review (security, maintainability, style subagents):
  - High: ShareGroupHeartbeat never dropped a moved coordinator (a 2.8 sibling). Fixed.
  - Medium: a late failure on an old coordinator dropped and closed its replacement; invalidation
    now names the connection the request used. `ShareConsumer.end()` didn't wait for running
    handlers, losing their acks. Rebalance callbacks lost `this`. All fixed with tests.
  - Low, fixed: an extra zstd allocation per sized frame (+37% on 1 KB batches); `_fetch`
    starting outside its promise chain; LogAppendTime on legacy compressed wrappers; CRC32
    partitioner docs; CHANGELOG notes for `subscribe()` now rejecting on an errored topic and
    by-name metadata re-creating a deleted topic when auto-creation is on.
  - Low, accepted: a topic's real metadata error (e.g. TopicAuthorizationFailed) still surfaces
    as UnknownTopicOrPartition, as before 2.6, and is logged on each refresh. Acks for a partition
    revoked before a fetch can carry them are only sent on `end()` (by then their locks have
    usually expired and the records are redelivered: at-least-once, as before but no longer
    silent). Sending them earlier needs a ShareAcknowledge racing the fetch's session epoch.
  - Style: two doc fixes (stacked JSDoc, CHANGELOG format).
  - Security, three high (all reachable from a fetched batch; reproduced here first):
    - lz4: the lz4 module's stream decoder loops forever on a 9-byte skippable frame, and
      zero-fills a block-size buffer (up to 4 MB) per block, so 64 KB of 1-byte blocks took
      5.8 s and 4.4 GB. Both pre-date Phase 2. lz4 frames are now parsed here and blocks decoded
      with the module's native `decodeBlock` (skippable frames rejected; checksums checked as
      before): 1 ms and 12 ms / 53 MB, and lz4 decode got 2-16x faster.
    - zstd: 9dc7708 kept a 128 KB buffer per frame (each frame ends a decode call), so 1 MB of
      10-byte frames took 2.8 s and 1.7 GB; one buffer at an offset now: 57 ms / 56 MB.
  - Security, low: `ShareConsumer.end()` could wait forever on a fetch stuck refreshing metadata
    (now 30 s at most, Java's default close timeout); a non-PEM SSL key (base64, DER) still landed
    in the read error (now replaced by one that names only `ssl.key` and the error code).
- Codex review (gpt-6.1-sol, xhigh), round 1:
  - [x] P2 The 30 s bound covered only the fetch wait: the final acks then waited on the same
        stuck metadata refresh, so `client.end()` was never reached. One deadline now covers the
        fetch wait, final acks and leave heartbeat.
  - [x] P2 Structured TLS values (`key: [{ pem, passphrase }]`, arrays of CAs) were read as file
        paths; worked before 2.2. Only a string or Buffer without PEM text is read as a file now.
- Codex review round 2 (switched to gpt-6.2-sol, xhigh, at the user's request):
  - [x] P1 A queued share ack was matched to a topic by name, so after a delete and recreate it
        could be applied to the new topic's records at the same offset. Acks now carry the ID of
        the topic their records came from; ShareFetch and ShareAcknowledge drop those for an ID
        that is no longer the topic's.
  - [x] P2 A handler calling `end()` waited the full 30 s for its own fetch, and the deadline cut
        off the final acks. `end()` no longer waits for running handlers (as Java's close): acks
        made so far are sent, a later `acknowledge()` is logged and not queued, and the epoch -1
        request releases the rest for redelivery.
  - [x] P2 File URLs (`ssl.ca: new URL(...)`) were passed to tls as is since round 1; read again.
  - [x] P3 `Result.error` is `null` on success: typed.
- Codex review round 3 (gpt-6.2-sol, xhigh):
  - [x] P2 `end()` drained the final acks while a ShareFetch carrying acks was still in flight;
        if it failed, the acks it requeued were neither sent nor reported. `end()` now settles
        the request (not its handlers) first.
  - [x] P2 `client.end()` waited for a coordinator lookup to resolve before closing its socket:
        one waiting on a silent broker's ApiVersions reply held `end()` for `requestTimeout`
        (pre-existing). A lookup records its connection as soon as it is created.
  - [x] P1 An ack made after the next delivery for its partition was tagged with that delivery's
        topic ID, so across a topic recreation it could accept a new record. Decision (2026-11-17):
        Java's rule, `acknowledge()` takes only records from the partition's latest delivery;
        others are logged and redelivered.
  - [x] P2 Acks kept for a revoked partition could overlap fresh acks if it was reacquired (the
        broker rejects overlapping batches). A delivery now drops queued acks for the offsets it
        delivers again; the rest are kept.
- Codex review round 4 (gpt-6.2-sol, xhigh, Fast mode from here on, at the user's request):
  - [x] P2 An idempotent batch with stamped sequences was retried into a recreated topic
        (UnknownTopicId, retriable since 2.7b), where it fails OutOfOrderSequenceNumber. Such a
        batch now fails, metadata is refreshed, and the next send starts a fresh sequence space.
  - [x] P2 `shareAcknowledgeRequest` rejected as a whole on one broker's failure, so `end()` moved
        on while other brokers' final acks were still in flight. Failures are per broker now.
  - [x] P2 Concurrent `end()` calls share one shutdown.
  - [x] P2 A redelivery dropped whole queued range acks; only the redelivered offsets are now.
  - [x] P2 A delivery's acquired ranges are copied (a handler may change its array), merged once,
        and binary-searched (5,000 ranges: ~280 ms per call before) (also P3).
  - [x] P2 A fetch waiting on a metadata refresh when `end()` ran still sent its request and
        handed out records; nothing is fetched or delivered after `end()` now.
  - [x] P2 `end()` closes every open share session (epoch -1), acks or not, so records a handler
        is still working on are released at once, not when their locks expire.
  - [x] P1 A deferred ack still couldn't be told from one for records delivered since from a
        recreated topic at the same offset (acks are `{topic, partition, offset}`). Decision
        (2026-11-17): a delivery's records can be acknowledged only while its handler runs (Java:
        until the next `poll()`); later acks are logged and the records redelivered.
- Codex review round 5 (gpt-6.2-sol, xhigh, Fast):
  - [x] P2 A stamped idempotent batch could be retried into a recreated topic after a plain
        timeout (the refresh learned the new ID). Checked against its stamped topic ID before
        every retry now; replaces round 4's response-code check.
  - [x] P2 Share fetch sent the old assignment's partitions under a recreated topic's ID. It keeps
        the assigned ID and skips the topic until the heartbeat updates the assignment.
  - [x] P2 A session reset by an assignment change was invisible to `end()`, dropping its acks.
  - [x] P2 A fetch waiting on a metadata refresh still went out after `end()`.
  - [x] P1 An ack scheduled by a handler that already returned can be accepted while a later
        delivery of the same offset is running. Decision (2026-11-17): app contract, documented
        in README and the typings (acks carry no identity); accepted, not a defect.
- Codex review round 6 (gpt-6.2-sol, xhigh, Fast; a first attempt with custom instructions was
  stopped by OpenAI's content filter, rerun as a plain `--base` review):
  - [x] P2 Produce v13 read the topic ID after async compression, so a refresh in between could
        retarget a stamped retry to a recreated topic. Fixed when requests are grouped.
  - [x] P2 InvalidShareSessionEpoch (session still on the broker) and a fetch completing after an
        assignment reset dropped the session from `end()`'s view, losing its acks.
  - [x] P3 Rejecting a large recreated-topic batch was quadratic (150,000 records ~6 s, now 0.3 s).
- Codex review round 7 (gpt-6.2-sol, xhigh, Fast):
  - [x] P2 ShareSessionNotFound for a partition without metadata reset every broker's session;
        results now carry the answering broker and only its session is forgotten.
  - [x] P2 Round 6 pinned unstamped Produce v13 topic IDs too; only stamped requests are now.
  - [x] P2 `_fetchRequest` kept the last fetch's records alive; it holds nothing now.
  - [x] P3 Exhausted retries gave one result per message; one per partition again.
  - [x] P3 `send()` typing accepts number keys (`SendMessage`).
- Codex review round 8 (gpt-6.2-sol, xhigh, Fast):
  - [x] P2 One silent broker's ShareFetch held up every broker's final acks past the deadline;
        fetches and final acks are per broker now.
  - [x] P2 A failed partition recovery ended the round under running handlers; logged instead.
  - [x] P2 A ShareAcknowledge decode failure rejected the whole call; per broker now.
- Codex review round 9 (gpt-6.2-sol, xhigh, Fast):
  - [x] P2 A broker's session and acks were settled only after every broker answered, so
        `end()` (per broker since round 8) missed newly opened sessions; per broker now.
  - [x] P2 A silent broker's final acks used the whole deadline, leaving no time to leave the
        group; the acks get 25 s of the 30.
  - [x] P2 A failed close-only ShareAcknowledge came back as an empty success; reported now.
- Codex review round 10 (gpt-6.2-sol, xhigh, Fast):
  - [x] P2 A fetch that failed in transit dropped its still-open session from `end()`'s view.
  - [x] P2 Final acks waited on a metadata refresh stuck on another broker; they don't now.
  - [x] P3 `SendMessage` keeps `Message`'s optional `offset`.
- Codex review round 11 (gpt-6.2-sol, xhigh, Fast):
  - [x] P2 A recreated topic's partition inherited the deleted log's sequence counter; the
        counter now records its topic ID and starts at 0 for a new one.
  - [x] P2 `client.end()` waited for a coordinator lookup still in progress; it doesn't now.
  - [x] P2 An unreadable epoch-0 fetch reply may have opened a session; `end()` tries to close it.
  - [x] P2 Final acks went to the partition's current leader, not the broker that delivered
        the records; acks remember their broker now.
  - [x] P2 Acquired offsets with no record were never acknowledged; GAP-acked now, as in Java.
  - [x] P3 Partitioner typings take the producer message type.
- Codex review round 12 (gpt-6.2-sol, xhigh, Fast):
  - [x] P1 Round 11's sequence reset also fired when a topic first got an ID (broker upgrade),
        risking a new batch taken as a duplicate; only a change between known IDs resets now.
  - [x] P2 A share fetch with only a truncated batch GAP-acked every acquired offset; none now.
  - [x] P2 A coordinator lookup answered as `end()` ran opened a connection afterwards.
- Codex review round 13 (gpt-6.2-sol, xhigh, Fast):
  - [x] P2 A stamped batch whose topic was recreated during compression was still sent by name.
  - [x] P2 Topic-ID errors kept retrying a stamped batch when the refresh learned no new ID.
  - [x] P2 After UnknownMemberId the old member's sessions and acks were sent as the new member.
- Codex review round 14 (gpt-6.2-sol, xhigh, Fast):
  - [x] P2 A broker that answered before a member change had its records delivered to the new one.
  - [x] P2 ShareFetch waited for metadata again before sending, opening a gap for `end()`.
  - [x] P3 A broker answering after the acks' deadline still started final acks during the leave.
- Codex review round 15 (gpt-6.2-sol, xhigh, Fast):
  - [x] P1 A topic ID flipping back (A -> B -> A) reused sequence 0 for A; per-ID sequences now.
  - [x] P2 Round 12 left legacy MessageSet share records unacknowledgeable; fixed.
  - [x] P2 ShareFetch went out a microtask after the `end()` check; same turn now.
- Codex review round 16 (gpt-6.2-sol, xhigh, Fast):
  - [x] P1 Per-ID counters (round 15) could still reuse a sequence by name on Produce < v13; a
        topic-ID change now fails the send and starts a fresh epoch instead.
  - [x] P1 Compressed legacy share records were delivered as their wrapper and the rest GAP-acked;
        expanded now. Also resolved magic-1 relative inner offsets (regular consumer too).
- Codex review round 17 (gpt-6.2-sol, xhigh, Fast):
  - [x] P2 A truncated inner legacy message set was rebased to wrong offsets; rejected now.
  - [x] P2 Metadata for the old topic ID stalled an assigned share partition; refreshed now.
  - [x] P3 `FetchOptions` fields required.
- Codex review round 18 (gpt-6.2-sol, xhigh, Fast):
  - [x] P2 A batch stamped before its topic had an ID escaped the recreated check; bound now.
  - [x] P2 A replaced share member kept fetching the old assignment; cleared now.
  - [x] P2 zstd's 128 MB window limit rejected valid frames under a raised cap; follows the cap.
- Verification: unit tests 31-39: 266 pass; eslint 0 errors (7 pre-existing warnings); tsc
  passes, and a TypeScript usage sample has 9 errors against the old typings, none now. Full
  suite on a fresh 4.2.0 broker: 701 pass, the same 10 failures as `master`. Live on 4.2.0:
  delete and recreate a topic under a running producer and consumer (Phase 1: 0/10 sends, nothing
  consumed; now 10/10 sent and consumed); share acks queued at `end()` (Phase 1: all 3 records
  redelivered to the next member; now none). 1 GiB bombs, peak RSS: zstd 1.1-2.2 GB -> 49-188 MB,
  lz4 2.2 GB -> 194 MB; zstd and lz4 decode speed unchanged.
