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
