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
