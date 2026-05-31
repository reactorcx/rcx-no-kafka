# Kafka no-kafka Client - Implementation Plan

(Previous completed phases omitted for brevity — see git history)

---

# KIP-932: Share Groups (Queues for Kafka)

## Overview

Share Groups introduce a new cooperative consumption model where multiple consumers can concurrently process records from the same partitions with individual acknowledgment and delivery counting. Unlike traditional consumer groups, partitions are NOT exclusively assigned — multiple members fetch from the same partitions, and the broker tracks per-record state (Available → Acquired → Acknowledged/Archived).

### Key Differences from GroupConsumer

| Aspect | GroupConsumer | ShareConsumer |
|--------|-------------|---------------|
| Membership | JoinGroup/SyncGroup/Heartbeat (keys 11/14/12) | ShareGroupHeartbeat (key 76) |
| Assignment | Client-side strategies (leader assigns) | Server-side only (broker assigns) |
| Fetching | Fetch API (key 1) | ShareFetch API (key 78) |
| Offsets | Client commits offsets | Broker manages record state |
| Acknowledgement | Implicit (offset commit) | Explicit per-record (Accept/Release/Reject) |
| Partition sharing | Exclusive (one consumer per partition) | Shared (multiple consumers per partition) |
| Sessions | Fetch sessions (optional) | Share sessions (required, epoch-based) |
| Coordinator type | FindCoordinator keyType=0 | FindCoordinator keyType=2 |

### New API Keys

- **76** ShareGroupHeartbeat — membership management, receives partition assignments
- **77** ShareGroupDescribe — describe share group (admin, defer to later)
- **78** ShareFetch — fetch records with optional piggyback acknowledgements
- **79** ShareAcknowledge — standalone acknowledgement of records

### Wire Format Notes

All 4 APIs are flexible from v0+ (KIP-482). In Kafka 4.2.0:
- ShareGroupHeartbeat: valid version = 1 (v0 was early access, removed in 4.1)
- ShareFetch: valid versions = 1-2
- ShareAcknowledge: valid versions = 1-2
- Topics use TopicId (UUID), not topic names — requires topicId ↔ name mapping from metadata

---

## Phase 1: Protocol Layer (Wire Format Definitions)

- [x] **1.1** Add API keys 76-79 and flexible version thresholds to `globals.js`
- [x] **1.2** Create `lib/protocol/share_group.js` — ShareGroupHeartbeat request/response (v1)
- [x] **1.3** Create `lib/protocol/share_fetch.js` — ShareFetch request/response (v1)
- [x] **1.4** Create `lib/protocol/share_acknowledge.js` — ShareAcknowledge request/response (v1)
- [x] **1.5** Register new protocol files in `lib/protocol/index.js`
- [x] **1.6** ~~Add UUID wire type~~ — already exists from KIP-516

## Phase 2: Client Methods

- [x] **2.1** Add `_findShareCoordinator(groupId)` + `updateShareCoordinator(groupId)` to `client.js`
- [x] **2.2** Add `shareGroupHeartbeatRequest(...)` to `client.js`
- [x] **2.3** Add `shareFetchRequest(...)` to `client.js` (with piggyback acks, record decompression)
- [x] **2.4** Add `shareAcknowledgeRequest(...)` to `client.js`

## Phase 3: ShareConsumer Class

- [x] **3.1** Create `lib/share_consumer.js` — constructor with options
- [x] **3.2** Implement `init()` — connect, discover coordinator, join via heartbeat
- [x] **3.3** Implement heartbeat loop — sends at broker-specified interval, handles assignment changes
- [x] **3.4** Implement share fetch loop — groups by leader, delivers to handler, manages session epoch
- [x] **3.5** Implement `acknowledge(records, type)` — queues acks, piggybacked on next ShareFetch
- [x] **3.6** Implement `end()` — sends leave heartbeat (epoch=-1), closes connections
- [x] **3.7** Export ShareConsumer + ACKNOWLEDGE_TYPE from `lib/index.js`

## Phase 4: Tests

- [x] **4.1** Protocol unit tests — round-trip encode/decode for all 4 request/response pairs (14 tests)
- [x] **4.2** Integration tests — ShareConsumer against Kafka 4.2.0 broker (join, fetch, acknowledge, leave) (5 tests)
- [x] **4.3** Run full existing test suite to verify no regressions (516 passing, 2 pre-existing SSL failures)

## Review

### Summary

Implemented KIP-932 Share Groups for the no-kafka client library. This adds a new `ShareConsumer` class that enables cooperative consumption where multiple consumers can concurrently process records from the same partitions with individual acknowledgment.

### Files Changed

| File | Change |
|------|--------|
| `lib/protocol/globals.js` | Added API keys 76-79 and flexible version thresholds |
| `lib/protocol/share_group.js` | **New** — ShareGroupHeartbeat v1 request/response wire format |
| `lib/protocol/share_fetch.js` | **New** — ShareFetch v1 request/response wire format |
| `lib/protocol/share_acknowledge.js` | **New** — ShareAcknowledge v1 request/response wire format |
| `lib/protocol/index.js` | Registered 3 new protocol files |
| `lib/client.js` | Added shareCoordinators map, 5 new methods (updateShareCoordinator, _findShareCoordinator, shareGroupHeartbeatRequest, shareFetchRequest, shareAcknowledgeRequest) |
| `lib/share_consumer.js` | **New** — ShareConsumer class with full lifecycle (join, heartbeat, fetch, acknowledge, leave) |
| `lib/index.js` | Exported ShareConsumer and ACKNOWLEDGE_TYPE |
| `test/27.protocol_share_group.js` | **New** — 14 protocol unit tests |
| `test/28.share_consumer_integration.js` | **New** — 5 integration tests against real Kafka 4.2.0 broker |

### Bugs Found and Fixed During Integration Testing

1. **Coordinator type**: ShareGroupHeartbeat must use GROUP coordinator (keyType=0), not SHARE coordinator (keyType=2). The share coordinator (keyType=2) is for internal broker-to-broker share partition state RPCs with keys in format "groupId:topicId:partition".

2. **Client-generated memberId**: Unlike JoinGroup, ShareGroupHeartbeat requires the client to generate its own UUID memberId. The broker rejects empty memberId with InvalidRequest.

3. **Assignment struct parsing**: The heartbeat response `assignment` field is a struct `{ _present, topicPartitions, tagCount }`, not a plain array. `_applyAssignment` needed to access `assignment.topicPartitions` and handle the `_present < 0` (null) case.

4. **Broker setup**: Share groups require the `__share_group_state` internal topic. On single-broker setups, this topic fails to auto-create because its default replication factor is 3. Must be pre-created with RF=1.

### Test Results

- Protocol unit tests: **14 passing**
- Integration tests: **5 passing** (join, assignment, leave, fetch, acknowledge)
- Full suite: **516 passing**, 3 pending, 2 failing (pre-existing SSL tests)

---

# KIP-848: Next-Gen Consumer Group Protocol

## Overview

Add a new `ConsumerGroup` class implementing the KIP-848 consumer rebalance
protocol (`ConsumerGroupHeartbeat`, API key 68, v1). Replaces the classic
JoinGroup/SyncGroup membership (deprecated by KIP-1274 in Kafka 4.3.0) with a
single heartbeat RPC + server-side assignment + client-side reconciliation.
Server-side assignor only. Classic `GroupConsumer` left untouched.

Full design: `docs/plans/2026-05-30-kip848-consumer-group-design.md`.

## Phase 1 — Protocol + client method

- [x] 1 Add `ConsumerGroupHeartbeatRequest: 68` to `API_KEYS` and `68: 0` to
      `FLEXIBLE_VERSION_THRESHOLDS` in `lib/protocol/globals.js`
- [x] 2 Create `lib/protocol/consumer_group.js` — `Protocol.define` for request v1,
      response v1, nested `TopicPartitions`, nullable `Assignment` struct
      (model on `share_group.js`)
- [x] 3 Register `'consumer_group'` in `lib/protocol/index.js`
- [x] 4 Add `consumerGroupHeartbeatRequest(...)` to `lib/client.js` (send to group
      coordinator, mirror `shareGroupHeartbeatRequest`)
- [x] 5 Add `FencedMemberEpoch` / `UnreleasedInstanceId` / `UnsupportedAssignor` /
      `StaleMemberEpoch` (110-113) to `lib/errors.js` (`UnknownMemberId` 25 already existed)
- [x] 6 Protocol round-trip unit tests in `test/29.kip848_consumer_group.js` (8 tests)
- [x] 7 Lint clean + full suite: 523 passing (only pre-existing SSL + share-group
      integration failures remain, confirmed present without these changes)

## Phase 2 — ConsumerGroup class

- [x] 8 Create `lib/consumer_group.js` extending `BaseConsumer`: state, `init`,
      `_join`, `_heartbeat`, `end` (model on `share_consumer.js`)
- [x] 9 Implement `_reconcile(target)` — revoke-before-acquire, reuse
      `subscribe`/`fetchOffset`, fire `onPartitionsRevoked`/`onPartitionsAssigned`
- [x] 10 Implement `commitOffset`/`fetchOffset` passing `memberEpoch` as the
      generation arg to `offsetCommitRequestV2`/`offsetFetchRequestV1`
- [x] 11 Export `Kafka.ConsumerGroup` in `lib/index.js`
- [x] 12 Lifecycle + reconciliation verified via live-broker integration test (item 14);
      mock-heavy unit tests skipped per codebase convention (ShareConsumer is the same)
- [x] 13 Lint clean + full suite green (no new failures)

## Phase 3 — Integration, types, docs

- [x] 14 Live-broker integration test (`test/30.kip848_consumer_group_integration.js`):
      join → assignment → onPartitionsAssigned → consume → commit → fetchOffset → leave (5 tests, all pass)
- [x] 15 Type declarations: `types/consumer_group.d.ts` + export in `types/index.d.ts`
- [x] 16 Document `ConsumerGroup` in `README.md` (new section + TOC + feature line)
- [x] 17 Lint clean (0 errors) + full suite 528 passing (only pre-existing SSL +
      share-group integration failures) + code review (below)

## Review

### Summary
Implemented KIP-848, the next-gen consumer rebalance protocol, as a new `ConsumerGroup`
class. The classic `GroupConsumer` (JoinGroup/SyncGroup) is untouched and still works —
both coexist; users opt in by instantiating `ConsumerGroup`. Server-side assignor only
(`uniform`/`range`); the client reconciles (revoke-before-acquire) the target assignment
the broker hands back. Protocol v1 (client-generated UUID member id, KIP-1082).

### Files
- `lib/protocol/consumer_group.js` (new) — ConsumerGroupHeartbeat request/response v1
  (mirrors `share_group.js`)
- `lib/consumer_group.js` (new) — the `ConsumerGroup` class (init/join/heartbeat/
  reconcile/commit/end), extends `BaseConsumer`
- `lib/protocol/globals.js` — API key 68 + flexible threshold
- `lib/protocol/index.js` — register `consumer_group`
- `lib/client.js` — `consumerGroupHeartbeatRequest(...)`
- `lib/errors.js` — error codes 110-113 (FencedMemberEpoch, UnreleasedInstanceId,
  UnsupportedAssignor, StaleMemberEpoch)
- `lib/index.js` + `types/index.d.ts` + `types/consumer_group.d.ts` — exports & types
- `README.md` — new ConsumerGroup section, TOC entry, feature line
- `test/29.kip848_consumer_group.js` — 8 protocol round-trip unit tests
- `test/30.kip848_consumer_group_integration.js` — 5 live-broker integration tests
- `docs/plans/2026-05-30-kip848-consumer-group-design.md` — design doc

### Verification
- 13 new KIP-848 tests pass (8 protocol + 5 integration: join → assignment →
  onPartitionsAssigned → consume → commit → fetchOffset round-trip → leave).
- Full suite: 528 passing, 3 pending, 5 failing — all 5 failures pre-existing
  (2 SSL on port 9093, 3 share-group integration), confirmed present without these
  changes. Lint: 0 errors. Node 18 compatible (only `crypto.randomUUID`).

### Code review (subagent) — outcome
Fixed: owned-set was mutated before `subscribe()` completed (could report a partition
acquired prematurely and leak it on subscribe failure) — now set only after subscribe
succeeds, with `_ownedChanged` re-flagged so the post-acquire set is reported;
`_revokeAll` now fires `onPartitionsRevoked` on a fence; `end()` only sends a leave
heartbeat when actually joined (`memberEpoch > 0`); distinct default `groupId`
(`no-kafka-consumer-group`) to avoid collision with `GroupConsumer`.
Pushed back / dismissed: StaleMemberEpoch is not a ConsumerGroupHeartbeat response error
(generic reschedule already refreshes the epoch); README was in fact updated (reviewer
working-tree read missed it).

(to be filled in after implementation)
