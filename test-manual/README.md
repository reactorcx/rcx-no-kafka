# Kafka 4.2 Multi-Datacenter Rack Awareness Simulator

Simulates a 3-datacenter Kafka deployment with configurable client-to-broker latency for testing rack-aware client libraries. Uses **Toxiproxy** for latency injection so it works on macOS Docker Desktop without kernel modules.

## Architecture

```
                          Host (your machine)
                    ┌─────────────────────────────┐
                    │       Your client library   │
                    │  bootstrap: localhost:19092 │
                    │  client.rack: dc-east       │
                    └──────┬──────┬──────┬────────┘
                           │      │      │
                    ┌──────┴──────┴──────┴──────────┐
                    │         Toxiproxy             │
                    │                               │
                    │  :19092 → broker-east   ~2ms  │
                    │  :29092 → broker-west  ~40ms  │
                    │  :39092 → broker-central ~20ms│
                    └──────┬──────┬──────┬──────────┘
                           │      │      │
              ┌────────────┘      │      └────────────┐
              ▼                   ▼                   ▼
     ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
     │ broker-east  │    │ broker-west  │    │ broker-central   │
     │ node.id=1    │    │ node.id=2    │    │ node.id=3        │
     │ rack=dc-east │    │ rack=dc-west │    │ rack=dc-central  │
     └──────────────┘    └──────────────┘    └──────────────────┘
            ←──── INTERNAL listeners (direct, no proxy) ────→
```

Inter-broker replication uses INTERNAL listeners directly (no latency). Client-facing EXTERNAL listeners route through Toxiproxy where latency is injected.

## Quick Start

All scripts run from the **host machine**.

```bash
cd kafka-rack-sim
chmod +x scripts/*.sh

# 1. Start the cluster
docker compose up -d

# 2. Wait for KRaft quorum to form
sleep 15

# 3. Create proxies and inject latency
bash scripts/setup-proxies.sh

# 4. Run the rack awareness test
bash scripts/test-rack-awareness.sh
```

## Connecting Your Client Library

| Bootstrap         | Routes through | Broker         | Simulated latency |
|-------------------|----------------|----------------|-------------------|
| `localhost:19092` | Toxiproxy      | broker-east    | ~2ms (local DC)   |
| `localhost:29092` | Toxiproxy      | broker-west    | ~40ms (far DC)    |
| `localhost:39092` | Toxiproxy      | broker-central | ~20ms (mid DC)    |

Brokers advertise the Toxiproxy addresses, so after the initial metadata fetch your client will automatically route produce/fetch requests through the correct proxy for each broker.

### Client config for rack-aware fetching

```properties
client.rack=dc-east
```

The cluster is configured with `replica.selector.class=RackAwareReplicaSelector`, so the broker will serve fetch requests from the replica matching the consumer's `client.rack` when possible.

### Node.js example

```javascript
const consumer = kafka.consumer({
  groupId: 'my-test-group',
  rackId: 'dc-east',
});
```

### Java example

```java
props.put(ConsumerConfig.CLIENT_RACK_CONFIG, "dc-east");
```

## Tuning Latency

### Via the setup script

Edit the variables at the top of `scripts/setup-proxies.sh`:

```bash
LATENCY_EAST=2       # ms
LATENCY_WEST=40      # ms
LATENCY_CENTRAL=20   # ms
```

Then re-run: `bash scripts/setup-proxies.sh`

### Via the Toxiproxy REST API

Update a toxic on the fly (no restart needed):

```bash
# Increase broker-west latency to 100ms
curl -X PATCH http://localhost:8474/proxies/broker-west/toxics/broker-west_latency_up \
  -d '{"attributes":{"latency":100,"jitter":10}}'

curl -X PATCH http://localhost:8474/proxies/broker-west/toxics/broker-west_latency_down \
  -d '{"attributes":{"latency":100,"jitter":10}}'

# Check current state
curl -s http://localhost:8474/proxies/broker-west/toxics | python3 -m json.tool
```

### Reset all latency

```bash
bash scripts/reset-latency.sh
```

## What to Look For

1. **Replica placement**: `kafka-topics.sh --describe` should show leaders and replicas spread across all three broker IDs, not clustered on one.

2. **Fetch locality**: With `client.rack=dc-east` and bootstrap `localhost:19092`, fetches should be served by broker 1 (dc-east) at ~4ms RTT. Without `client.rack`, fetches for partitions led by broker-west go through the 40ms proxy at ~80ms RTT.

3. **Latency delta**: Compare fetch timing with and without `client.rack` set. The difference should be clearly visible — that's rack awareness working.

## Cleanup

```bash
docker compose down -v
```
