## Unreleased

### Changed
- `batch.size` counts the bytes queued per partition, as Java's does, not across all partitions: sends over many partitions are batched instead of going out one or two records per partition (a request can now hold `batch.size` per partition)
- Each fetch starts at the next of a broker's partitions, so one backlogged partition no longer takes every response (as Java rotates them)
- Consumers fetch again at once after a round whose handlers moved an offset forward, as Java's `poll()` does; `idleTimeout` now applies only after a round that delivered nothing, failed or hit an error (it capped a consumer at one response per second per broker)
- Partitioners hash a key that is neither a Buffer nor a string as `String(key)`, the bytes sent — a number key moves partition (README shows how to keep the old mapping)
- During a rolling upgrade old and new producers split a number key across two partitions; on compacted topics the old partition keeps its last value
- A topic whose metadata comes back with an error is no longer cached as having no partitions — `subscribe(topic)` without partitions now rejects with `UnknownTopicOrPartition` instead of resolving with nothing (the real error is logged)
- Metadata is requested by topic name only (as Java does), never by a cached topic ID — on a broker with `auto.create.topics.enable=true` a consumer still subscribed to a deleted topic can re-create it
- `ShareConsumer.acknowledge()` takes a delivery's records only while its handler runs (as Java takes them only until the next `poll()`); later acks are logged and the records redelivered
- `ShareConsumer.end()` sends the acknowledgements made so far, closes every open share session and leaves within 30 s, without waiting for running handlers; nothing is fetched or delivered after it
- `ShareConsumer` acknowledges acquired offsets that hold no record as gaps (as Java does), and sends acks to the broker that delivered the records
- An idempotent send that finds its topic recreated (a new topic ID) fails with `UnknownTopicId` instead of reusing the old log's sequence numbers; the next send starts a fresh producer epoch
- A running SimpleConsumer or GroupConsumer finds a recreated topic and keeps its position, as Java's does; a shorter new topic restarts it at `recoveryOffset` (README, Known limitations)
- `ShareConsumer` sends a revoked partition's queued acks on the next fetch to the broker that delivered them; acks for a broker that left the cluster are dropped with a warning
- Legacy (pre-0.11) messages are verified against their CRC-32; a corrupt one fails the fetch
- `ShareConsumer` skips a batch holding a record outside the batch's offsets, as undecodable (the broker redelivers it, then archives it at the delivery limit)
- TLS `cert`, `key` and `ca` are each read as a file path unless they hold a PEM header line, so a file cert can go with an inline key

### Fixed
- Compressed legacy (magic 1) messages are delivered at their absolute offsets; their inner offsets were relative
- `DefaultPartitioner` hashed a number key as that many zero bytes, and threw on negative, boolean and object keys
- `HashCRC32Partitioner` sent every number key to partition 0 and threw on Buffer keys
- Decompression stays within `compression.maxOutputSize` for every codec: LZ4 skippable frames and tiny blocks, and multi-frame zstd data, could hang the decoder or allocate far past the cap
- A top-level Metadata v13 error (e.g. `RebootstrapRequired`) was ignored; the refresh now fails and keeps the known brokers
- A legacy message with a corrupt key or value length, or a RecordBatch after legacy messages in one response (a segment written across a format upgrade), failed the whole fetch for every partition on every poll; it now counts as a corrupt message, and a consumer's next fetch starts at the batch (a ShareConsumer gets the batch's records again once their lock expires)
- Typings: `GroupAdmin` methods return Promises, `SendMessage` keys may be numbers, booleans or objects, and `Result.error` may be null

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
