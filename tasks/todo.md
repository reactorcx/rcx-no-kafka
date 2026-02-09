# Kafka no-kafka Client - Implementation Plan

(Previous completed phases omitted for brevity — see git history)

---

# Per-Broker API Version Negotiation for Initial Brokers

## Problem

`initialBrokers` (seed brokers) never get `apiVersionsRequest` called on them. Their `apiVersions` stays `null` forever. This causes:

1. **`metadataRequest()` always uses v0** — The version selection loop checks `initialBrokers[*].apiVersions`, which are always `null`, so `metadataMax` stays 0. Newer metadata features (controllerId, clusterId, rack info, authorized ops) are never used via the initial brokers.

2. **`initProducerIdRequest()` always uses v0** — Same pattern at line 1524: checks initial broker versions, gets `null`, defaults to v0.

Note: `brokerConnections` already get per-broker API versions correctly — `updateMetadata()` calls `apiVersionsRequest` for each new broker connection. `groupCoordinators` and `transactionCoordinators` also get their own `apiVersionsRequest` on creation. The gap is only `initialBrokers`.

## Tasks

- [x] **1** Add `apiVersionsRequest` calls for initial brokers in `init()` — In `lib/client.js`, after creating `initialBrokers` and before `updateMetadata()`, call `apiVersionsRequest` on each initial broker.
- [x] **2** Add integration test in `test/11.v011_integration.js` verifying initial brokers have API versions populated after `init()`
- [x] **3** Run full test suite and eslint to verify no regressions

---

## Review

### Summary
Added API version discovery for initial (seed) brokers during `init()`. Previously, `initialBrokers` never had `apiVersionsRequest` called on them, so their `apiVersions` stayed `null` forever. This meant `metadataRequest()` and `initProducerIdRequest()` always fell back to v0 regardless of broker capabilities.

### Files Modified

| File | Changes |
|------|---------|
| `lib/client.js` | Added `Promise.all(initialBrokers.map(apiVersionsRequest))` step in `init()` before `updateMetadata()` |
| `test/11.v011_integration.js` | Added test verifying initial brokers have populated `apiVersions` after init |

### Test Results
- **314 passing**, 2 failing (pre-existing SSL failures on port 9093)
- ESLint: clean
