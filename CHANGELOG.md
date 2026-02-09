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
