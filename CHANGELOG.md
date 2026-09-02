## 4.11

### Changed (CORE-4239 rebalance drain)
- **`onPartitionsRevoked` is now awaited.** Previously invoked fire-and-forget on a cooperative rebalance only; it is now called on all three revoke paths (cooperative rebalance, eager rebalance, full rejoin — the last only in cooperative mode, where `ownedPartitions` is tracked; an eager full rejoin drains through its re-subscribe instead) and, if it returns a promise, the rebalance **waits for it** before fetching offsets and re-subscribing. This lets the callback commit in-flight, already-emitted work so a rejoin does not re-deliver a batch. Existing callbacks that return a promise will now delay the rebalance by however long they take to settle.

- **A cooperative full rejoin now stops consuming while it rejoins.** Partitions this member owns are removed from the fetch loop *before* the drain rather than when the rejoin completes. Previously the fetch loop kept polling any owned partition that was not mid-handler for the whole drain, and work fetched in that window was parked after the drain had captured its entry list — invisible to it, and free to commit an offset for a partition the member no longer owned. The cost is that a cooperative full rejoin now yields no throughput until it succeeds, retries included (backoff caps at 30s, `maxRejoinAttempts` defaults to `Infinity`); previously it kept consuming from a stale assignment, part of which another member already owned. Eager mode is unaffected: `ownedPartitions` is not tracked there, so nothing is removed and the re-subscribe drain is unchanged.

### Added
- `revokeTimeout` — bound in ms on the `onPartitionsRevoked` wait, defaults to half of `sessionTimeout`. If the callback rejects or exceeds this, a warning naming the affected partitions is logged and the rebalance continues from the last committed offset (the pre-4.11 behaviour). It is a budget for the whole rebalance, not a bound on each drain: a cooperative rebalance can revoke in two phases and both run inside one heartbeat-suspended `_rejoin`, so bounding each separately would let one rebalance spend `2 x revokeTimeout` — at the default, the entire session budget, which gets the member evicted and causes another rebalance. Phase 1 keeps the full window; a later drain in the same rebalance gets whatever is left, and is notified without being awaited (with a warning) once it is spent. An explicit `0` means "notify but do not wait" — the callback is still invoked and nothing is awaited; the drain is disabled, which is warned about once at construction. Invalid values (non-numeric, negative, NaN) are warned about and fall back to the default rather than silently disabling the drain. A value above half of `sessionTimeout` is warned about at construction: heartbeats are suspended for the whole rebalance and the drain is only one step of it, so past that point a slow drain crowds out JoinGroup/SyncGroup/fetchOffset. The drain reduces re-delivery but does not eliminate it — handlers must stay idempotent.
- TypeScript: `Strategy` now declares `cooperative`, `onPartitionsRevoked` and `onPartitionsAssigned`, which the runtime has always read but the types omitted.

## 4.10

### Added (KIP-899 Re-bootstrap, KIP-390 Compression Level)
- Client re-bootstrap (KIP-899) — `rebootstrap: true` option enables automatic re-resolution of bootstrap servers when all known brokers are unavailable
- Compression level support (KIP-390) — `compressionLevel` option for producer to control compression ratio vs. speed (Gzip 0-9, Zstd 1-22, LZ4 high compression mode)

## 4.9

### Added (Multi-Group OffsetFetch, Producer Epoch Recovery, LeaveGroup Reason)
- OffsetFetch v7-v9 (KIP-447, KIP-709, KIP-848) — `requireStable` flag, multi-group batch fetch with `groups` array, `memberId`/`memberEpoch` per group for new consumer protocol
- InitProducerId v3-v5 (KIP-588, KIP-890) — `producerId`/`producerEpoch` in request for epoch recovery after transactional failures
- LeaveGroup v5 (KIP-800) — `reason` field per member item

## 4.8

### Added (KIP-699: Batch FindCoordinator)
- FindCoordinator v4-v6 (KIP-699, KIP-890, KIP-932) — batch coordinator lookup with `coordinatorKeys` array, per-coordinator error handling in response

## 4.7

### Added (Kafka 3.7+ Fetch Protocol — KIP-405, KIP-903, KIP-951)
- Fetch v14 (KIP-405 Tiered Storage) — version bump signaling tiered storage awareness
- Fetch v15 (KIP-903 Broker Epoch) — removes replicaId from request body
- Fetch v16 (KIP-951 Leader Discovery) — response adds NodeEndpoints tagged field for direct leader connection without metadata refresh

## 4.6

### Added (KIP-951 Leader Discovery + KIP-516 Completion)
- Produce v10/v11 (KIP-951) — CurrentLeader per-partition and NodeEndpoints in Produce responses
- Metadata v11/v12 (KIP-516 completion) — nullable topicName, removed clusterAuthorizedOperations
- Fetch v12/v13 CurrentLeader parsing — inline tagged field parsing for leader hints
- `_applyLeaderHints()` — updates metadata cache from leader hints without full metadata refresh
- Transaction API v3 bumps (AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit)
- KIP-516 topic IDs (UUIDs) — Metadata v10, Fetch v13 with topicId, uuid primitive type

## 4.5

### Added (Kafka 2.6 Protocol Support — KIP-482 Flexible Versions)
- All 12 APIs bumped to their first flexible version with compact encoding (compactString, compactArray, compactBytes, TaggedFields)
- Metadata v9, FindCoordinator v3, JoinGroup v6, Heartbeat v4, SyncGroup v4, LeaveGroup v4
- OffsetCommit v8, OffsetFetch v6, ListGroups v3, DescribeGroups v5, InitProducerId v2
- ApiVersions v3 protocol definitions (client still sends v0 for bootstrap)
- Flexible request header v2 with TaggedFields support

## 4.4

### Added
- Cooperative/incremental rebalancing (KIP-429) — `cooperative: true` strategy option for GroupConsumer enables two-phase rebalancing where only migrating partitions are revoked, keeping unaffected partitions active throughout
- Rebalance lifecycle callbacks — `onPartitionsRevoked` and `onPartitionsAssigned` optional callbacks in strategy options
- Subscription metadata v1 with `ownedPartitions` field for cooperative protocol

## 4.3.1

### Fixed
- Initial (seed) brokers now query ApiVersions on connect, enabling version-negotiated metadata and InitProducerId requests instead of always falling back to v0

## 4.3

### Added (Kafka 2.4 Protocol Support)
- Upgraded all APIs to Kafka 2.4 maximum protocol versions with automatic version negotiation
- Static group membership (KIP-345) — `groupInstanceId` option for GroupConsumer, reduces rebalances on restart
- Fetch from closest replica / rack awareness (KIP-392) — `rackId` option for consumers, `preferredReadReplica` handling
- Authorized operations in Metadata v8 and DescribeGroups v3/v4 (KIP-430)
- Two-phase JoinGroup (KIP-394) — automatic retry with assigned memberId on MemberIdRequired
- Produce v8, Fetch v11, ListOffsets v5, Metadata v8, InitProducerId v1
- JoinGroup v5, Heartbeat v3, SyncGroup v3, LeaveGroup v3 (batch leave), OffsetCommit v7, DescribeGroups v4
- Error codes 68-89 (NonEmptyGroup through ThrottlingQuotaExceeded)
- FencedLeaderEpoch error recovery in consumers

## 4.2

### Added (Kafka 2.1 Protocol Support)
- Upgraded all APIs to Kafka 2.1 maximum protocol versions with automatic version negotiation
- Fetch sessions support (KIP-227) — bypassed with full fetch mode (sessionId=0)
- Leader epoch fencing fields (KIP-320) — parsed from responses, currentLeaderEpoch=-1 in requests
- Produce v7, Fetch v10, Metadata v7, ListOffsets v4, OffsetCommit v6, OffsetFetch v5
- JoinGroup v3 (throttleTime in response), FindCoordinator v2
- Simple version bumps to v2: Heartbeat, LeaveGroup, SyncGroup, DescribeGroups, ListGroups, ApiVersions
- Error codes 52-66 (UnsupportedCompressionType through DelegationTokenExpired)

## 4.1

### Added (Kafka 1.0 Protocol Support)
- Upgraded all APIs to Kafka 1.0 maximum protocol versions with automatic version negotiation
- Produce v5 and Fetch v6 with logStartOffset support
- Metadata v5 with throttleTime, clusterId, controllerId, offlineReplicas, allowAutoTopicCreation
- ListOffsets v2 with throttleTime and isolationLevel
- OffsetCommit v3 with throttleTime, OffsetFetch v3 with throttleTime and null topics support
- JoinGroup v1 with rebalanceTimeout (v2 for version negotiation)
- Throttle-time version bumps: Heartbeat v1, LeaveGroup v1, SyncGroup v1, DescribeGroups v1, ListGroups v1, ApiVersions v1
- Version discovery on coordinator connections (group and transaction coordinators)

## 4.0

### Added
- Kafka 0.11 protocol support with automatic version negotiation via ApiVersions API
- RecordBatch v2 format (magic byte 2) for produce and fetch
- LZ4 compression support (`Kafka.COMPRESSION_LZ4`), requires `lz4` npm module
- Message timestamps and headers for Kafka 0.11+ brokers
- CRC-32C (Castagnoli) checksums for Record Batch v2

### Added (Idempotent & Transactional Producer)
- Idempotent producer (`idempotent: true`) for exactly-once delivery per partition
- Transactional producer (`transactionalId: '...'`) for atomic writes across partitions
- Transaction lifecycle methods: `beginTransaction()`, `commitTransaction()`, `abortTransaction()`, `sendOffsets()`
- Consumer `isolationLevel` option (`0` = read_uncommitted, `1` = read_committed)
- InitProducerId protocol (apiKey 22)
- FindCoordinator v1 protocol (coordinatorType 0=group, 1=transaction)
- Transaction protocols: AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26), TxnOffsetCommit (28)

### Fixed
- RecordBatch write incorrectly treated `producerId=0` as falsy due to `|| -1` bug

### Changed
- Snappy now requires `snappy` v7+ for Node.js 18+ compatibility
- Consumer `maxBytes` operates at the RecordBatch level (Kafka always returns at least one complete batch)

### Backward Compatibility
- All existing v0 code paths remain as fallback for older brokers
- Version negotiation automatically uses the highest mutually supported protocol version
- No changes to producer/consumer public API — new fields (timestamp, headers) are optional

## 3.0

### Backward incompatible changes
- Producer partitioner is now implemented as a class and `Kafka.DefaultPartitioner` matches Java client implementation. Custom partitioners should inherit `Kafka.DefaultPartitioner`
- GroupConsumer assignment strategies are also now implemented as classes. Custom strategies should inherit from `Kafka.DefaultAssignmentStrategy`
- Using async compression now by default
- Producer retries delay is now progressive and configured with two values `delay: { min, max }`. See [README](README.md#producer-options) for more.
- Default producer ack timeout has been changed from 100ms to 30000ms to match Java defaults

### Added
- SSL support
- Broker redirection (map host/port to alternate/internal host/port pair)
