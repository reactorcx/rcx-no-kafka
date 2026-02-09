# Kafka no-kafka Client - Implementation Plan

(Previous completed phases omitted for brevity — see git history)

---

# KIP-429: Cooperative/Incremental Rebalancing

## Todo

- [x] **1** Write ownedPartitions in subscription metadata (group_membership.js)
- [x] **2** Read ownedPartitions from JoinGroup response members (group_membership.js)
- [x] **3** Add cooperative state tracking to GroupConsumer constructor/init (group_consumer.js)
- [x] **4** Pass ownedPartitions in JoinGroup request (group_consumer.js)
- [x] **5** Cooperative assignment filtering in _syncGroup leader logic (group_consumer.js)
- [x] **6** Incremental _updateSubscriptions for cooperative mode (group_consumer.js)
- [x] **7** Phase 2 rejoin trigger in _rejoin (group_consumer.js)
- [x] **8** Tests for cooperative rebalancing (test/03.group_consumer.js)
- [x] **9** Run full test suite to verify no regressions
- [x] **10** Review and cleanup

---

## Review

### Summary
Implemented KIP-429 cooperative/incremental rebalancing for the no-kafka consumer group client. This is a client-side-only change — no broker protocol version changes needed.

Key design decisions:
- Cooperative mode is **opt-in** via `cooperative: true` in strategy config (default remains EAGER)
- The cooperative filtering logic lives in `GroupConsumer._syncGroup()`, NOT in individual strategies — zero changes to assignment strategy files
- Subscription metadata v1 adds `ownedPartitions` field so the leader knows what each member currently owns
- `_updateSubscriptions()` becomes incremental in cooperative mode (add/remove specific partitions instead of clear-all)
- Two-phase rejoin: phase 1 revokes migrating partitions, phase 2 assigns them to new owners

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/group_membership.js` | Write ownedPartitions in subscription metadata v1; read ownedPartitions from JoinGroup response members (both v0 and v5 formats) |
| `lib/group_consumer.js` | Track owned partitions + cooperative mode flag in constructor; pass ownedPartitions in JoinGroup; cooperative assignment filtering in _syncGroup leader; incremental _updateSubscriptionsCooperative; phase 2 rejoin trigger; rebalance callbacks (onPartitionsRevoked/onPartitionsAssigned) |
| `test/03.group_consumer.js` | Added `GroupConsumer (cooperative)` describe block with tests for: single consumer cooperative init, two-consumer cooperative rebalance, ownedPartitions tracking |

### Test Results
- **317 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- All existing tests unaffected (backward compatible)

---

# KIP-482: Flexible Versions Infrastructure

## Todo

- [x] **1** Add compact type primitives (compactString, compactBytes, compactArray + nullable variants) to `lib/protocol/common.js`
- [x] **2** Add TaggedFields primitive to `lib/protocol/common.js`
- [x] **3** Add FlexibleRequestHeader to `lib/protocol/common.js`
- [x] **4** Add FLEXIBLE_VERSION_THRESHOLDS map to `lib/protocol/globals.js`
- [x] **5** Add unit tests in `test/17.kip482_flexible_versions.js`
- [x] **6** Run lint and tests to verify no regressions

---

## Review

### Summary
Added KIP-482 flexible versions infrastructure — the encoding primitives needed for Kafka 2.6+ flexible version APIs. No existing APIs are changed; this is the foundation layer only.

### What was added

**Compact type primitives** (`lib/protocol/common.js`):
- `compactString` / `compactNullableString` — UVarint(N+1)-prefixed UTF-8 strings
- `compactBytes` / `compactNullableBytes` — UVarint(N+1)-prefixed byte buffers
- `compactArray` / `compactNullableArray` — UVarint(N+1)-prefixed element loops

**TaggedFields** (`lib/protocol/common.js`):
- Read: skips unknown tagged fields (UVarint tag count, then tag/size/data triples)
- Write: always emits `0x00` (empty tag section)

**FlexibleRequestHeader** (`lib/protocol/common.js`):
- Request header v1: same as v0 but uses compactNullableString for clientId + trailing TaggedFields

**FLEXIBLE_VERSION_THRESHOLDS** (`lib/protocol/globals.js`):
- Maps API key → first flexible version number for all APIs through Kafka 2.6

### Files Modified

| File | Changes |
|------|---------|
| `lib/protocol/common.js` | Added 8 new Protocol.define blocks (6 compact types + TaggedFields + FlexibleRequestHeader) |
| `lib/protocol/globals.js` | Added FLEXIBLE_VERSION_THRESHOLDS map |
| `test/17.kip482_flexible_versions.js` | New file: 24 round-trip encode/decode tests |

### Test Results
- **334 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
- All existing tests unaffected (backward compatible)
