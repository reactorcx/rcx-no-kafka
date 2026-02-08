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
