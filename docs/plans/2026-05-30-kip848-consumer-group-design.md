# KIP-848: Next-Gen Consumer Group Protocol

## Background

Kafka 4.3.0 introduces no new consumer wire APIs, but its consumer-facing KIPs
all point the same direction:

- **KIP-1274** — deprecates the **classic** JoinGroup/SyncGroup rebalance
  protocol (the one `no-kafka`'s `GroupConsumer` uses today); Phase 1 only logs a
  warning, removal comes later.
- **KIP-1251** / **KIP-1237** — broker-side epoch validation and a config
  deprecation for the new protocol.

The forward-looking work is therefore to adopt **KIP-848**, the next-generation
consumer rebalance protocol (GA since Kafka 4.0, now the recommended path). It
replaces the entire JoinGroup → SyncGroup → leader-assignment dance with a single
heartbeat RPC (`ConsumerGroupHeartbeat`, API key 68) plus broker-driven
("server-side") assignment and client-side reconciliation.

This is structurally almost identical to the KIP-932 share-group support already
in the codebase (`lib/share_consumer.js`, `lib/protocol/share_group.js`).

## Scope decisions

- **New `ConsumerGroup` class** — not a mode toggle on `GroupConsumer`. The two
  membership protocols share almost no code. Classic `GroupConsumer` is left
  untouched and keeps working.
- **Server-side assignor only** (`uniform` / `range`). The client never computes
  assignments; it only reconciles the target the broker hands back. Client-side
  assignors (custom logic, leader election, metadata exchange) are out of scope
  (YAGNI).
- **API mirrors `ShareConsumer`**: `init({ topics, handler })`, manual
  `commitOffset` / `fetchOffset`, optional `onPartitionsRevoked` /
  `onPartitionsAssigned` callbacks.
- **Protocol v1** (KIP-1082: client-generated UUID member IDs; adds
  `SubscribedTopicRegex`), consistent with how `ShareConsumer` already generates
  its own `memberId`.

## Wire format (verified against the Kafka 4.3.0 JSON specs)

`ConsumerGroupHeartbeatRequest` — API key 68, valid versions 0–1, flexible 0+.

```
FlexibleRequestHeader(apiKey=68, apiVersion=1)
compactString(groupId)
compactString(memberId)              // client-generated UUID
Int32BE(memberEpoch)                 // 0 = join, -1 = leave, -2 = static leave
compactNullableString(instanceId)    // groupInstanceId or null
compactNullableString(rackId)        // null
Int32BE(rebalanceTimeoutMs)          // sessionTimeout, or -1
compactNullableArray(subscribedTopicNames, compactString)  // topics on join, null when unchanged
compactNullableString(subscribedTopicRegex)                // null (v1)
compactNullableString(serverAssignor)                      // 'uniform' on join, null when unchanged
compactNullableArray(topicPartitions, TopicPartitions)     // owned set, null when unchanged
TaggedFields()

TopicPartitions := uuid(topicId) compactArray(partitions, Int32BE) TaggedFields()
```

`ConsumerGroupHeartbeatResponse` — flexible 0+.

```
Int32BE(correlationId)
TaggedFields()                       // response header v1
Int32BE(throttleTimeMs)
Int16BE(errorCode)
compactNullableString(errorMessage)
compactNullableString(memberId)
Int32BE(memberEpoch)
Int32BE(heartbeatIntervalMs)
Assignment(assignment)               // nullable struct, Int8 present flag (-1 = null)
TaggedFields()

Assignment := compactArray(topicPartitions, TopicPartitions) TaggedFields()
```

The nullable-`Assignment`-struct idiom (Int8 present flag) and the
`TopicPartitions` shape are copied directly from `share_group.js`.

## Components

| File | Change |
|------|--------|
| `lib/protocol/consumer_group.js` (new) | `Protocol.define` blocks for `ConsumerGroupHeartbeatRequest`/`Response` v1, plus the nested `TopicPartitions` and nullable `Assignment` structs — parallels `share_group.js` |
| `lib/protocol/globals.js` | `ConsumerGroupHeartbeatRequest: 68` in `API_KEYS`; `68: 0` in `FLEXIBLE_VERSION_THRESHOLDS` |
| `lib/protocol/index.js` | Register `'consumer_group'` |
| `lib/client.js` | `consumerGroupHeartbeatRequest(...)` — sends to the group coordinator, mirroring `shareGroupHeartbeatRequest` |
| `lib/consumer_group.js` (new) | `ConsumerGroup` class extending `BaseConsumer` |
| `lib/index.js` | Export `Kafka.ConsumerGroup` |
| `lib/errors.js` | Ensure `FencedMemberEpoch`, `UnreleasedInstanceId`, `UnknownMemberId` codes are mapped |
| `types/index.d.ts` | Type declarations |
| `README.md` | Document the new consumer |
| `test/29.kip848_consumer_group.js` (new) | Protocol round-trip + live-broker integration tests |

## Lifecycle (`lib/consumer_group.js`)

State: `memberId = crypto.randomUUID()`, `memberEpoch = 0`, `heartbeatIntervalMs`,
`assignment` (owned topicId→partitions[]), `_topics`, `_handler`, `_closed`.

- **`init({ topics, handler })`** → `client.init()` → `updateMetadata(topics)`
  (need topicId↔name maps, the wire uses topic IDs) → `_join()`.
- **`_join()`** (mirrors `ShareConsumer._join`, retry/backoff):
  `updateGroupCoordinator` → heartbeat with `memberEpoch=0`,
  `subscribedTopicNames=topics`, `serverAssignor='uniform'`, `topicPartitions=[]`
  → store `memberId`/`memberEpoch`/`heartbeatIntervalMs`, reconcile assignment,
  start heartbeat loop.
- **`_heartbeat()`** (setTimeout recursive loop): steady-state sends `memberEpoch`
  with nulls for unchanged fields (sends owned `topicPartitions` only when it just
  changed, to acknowledge reconciliation); on response update epoch/interval and
  reconcile; on `UnknownMemberId`/`FencedMemberEpoch` regenerate `memberId`, reset
  epoch, rejoin.
- **`end()`**: final heartbeat with `memberEpoch = -1` (or `-2` for static
  members) then `client.end()`.

## Reconciliation (`_reconcile(target)`)

Revoke-before-acquire for safe handoff:

1. Map target `topicPartitions` (IDs) → names via `client.topicNames`.
2. `revoked = owned − target`: `unsubscribe()` each, fire `onPartitionsRevoked`
   (user can commit final offsets here).
3. `added = target − owned`: `fetchOffset()` committed positions, then
   `subscribe(topic, partition, {offset|time}, handler)` (reusing
   `BaseConsumer.subscribe`, exactly as `GroupConsumer._updateSubscriptions`);
   fire `onPartitionsAssigned`.
4. `owned = target`. Next heartbeat reports this set to acknowledge.

Consumption is entirely the inherited `BaseConsumer._fetch()` loop — no new fetch
code.

## Offsets

Reuse existing client methods — **no new offset protocol code**.
`offsetCommitRequestV2(groupId, memberId, memberEpoch, requests)` already
negotiates OffsetCommit up to v10 and passes `generationId` + `memberId`; under
KIP-848 the broker reads that generation slot as the **member epoch** and
`memberId` as the UUID. `fetchOffset()` reuses `offsetFetchRequestV1`. Manual
commit only (handler owns timing), consistent with `GroupConsumer`.

## Testing

- Unit: encode→decode round-trip for request/response v1, including null
  assignment and multi-topic `topicPartitions`.
- Integration (live broker, new protocol is default since 4.0): join a group,
  receive an assignment, consume a produced message, commit, and leave cleanly.

## Open item to confirm during implementation

`rebalanceTimeoutMs` default and whether the broker requires `sessionTimeout` to
be passed — confirm against broker behavior; default to `-1` (broker default) if
unspecified.
