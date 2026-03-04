#!/bin/bash
##############################################################################
# setup-proxies.sh — Run from the HOST
#
# Creates Toxiproxy proxies for each broker and adds latency toxics.
# Simulates the client being in dc-east:
#
#   localhost:19092 → broker-east    ~2ms   (local DC)
#   localhost:29092 → broker-west    ~40ms  (cross-country)
#   localhost:39092 → broker-central ~20ms  (regional)
#
# Adjust the latency values below as needed.
##############################################################################

set -euo pipefail

TOXI_API="http://localhost:8474"

LATENCY_EAST=2        # ms — local datacenter
LATENCY_WEST=40       # ms — cross-country
LATENCY_CENTRAL=20    # ms — regional
JITTER_EAST=1
JITTER_WEST=5
JITTER_CENTRAL=3

echo "============================================================"
echo " Setting up Toxiproxy for Kafka rack-awareness testing"
echo "============================================================"
echo ""

# ── Wait for Toxiproxy to be ready ──
echo -n "Waiting for Toxiproxy API..."
until curl -sf "$TOXI_API/version" >/dev/null 2>&1; do
    echo -n "."
    sleep 1
done
echo " ready ($(curl -s "$TOXI_API/version"))"
echo ""

# ── Helper: create or update a proxy ──
create_proxy() {
    local name=$1 listen=$2 upstream=$3

    # Delete if exists (idempotent re-runs)
    curl -sf -X DELETE "$TOXI_API/proxies/$name" >/dev/null 2>&1 || true

    curl -sf -X POST "$TOXI_API/proxies" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"$name\",
            \"listen\": \"0.0.0.0:$listen\",
            \"upstream\": \"$upstream\",
            \"enabled\": true
        }" >/dev/null

    echo "  Created proxy: $name (0.0.0.0:$listen → $upstream)"
}

# ── Helper: add latency toxic to a proxy ──
add_latency() {
    local name=$1 latency=$2 jitter=$3

    curl -sf -X POST "$TOXI_API/proxies/$name/toxics" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"${name}_latency_up\",
            \"type\": \"latency\",
            \"stream\": \"upstream\",
            \"attributes\": { \"latency\": $latency, \"jitter\": $jitter }
        }" >/dev/null

    curl -sf -X POST "$TOXI_API/proxies/$name/toxics" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"${name}_latency_down\",
            \"type\": \"latency\",
            \"stream\": \"downstream\",
            \"attributes\": { \"latency\": $latency, \"jitter\": $jitter }
        }" >/dev/null

    echo "  Added latency: ${latency}ms ± ${jitter}ms (both directions)"
}

# ── Create proxies ──
echo "Creating proxies..."
create_proxy "broker-east"    19092 "broker-east:9094"
create_proxy "broker-west"    29092 "broker-west:9094"
create_proxy "broker-central" 39092 "broker-central:9094"
echo ""

# ── Add latency toxics ──
echo "Injecting latency..."

echo "  broker-east (dc-east) — local DC:"
add_latency "broker-east" "$LATENCY_EAST" "$JITTER_EAST"

echo "  broker-west (dc-west) — cross-country:"
add_latency "broker-west" "$LATENCY_WEST" "$JITTER_WEST"

echo "  broker-central (dc-central) — regional:"
add_latency "broker-central" "$LATENCY_CENTRAL" "$JITTER_CENTRAL"

echo ""
echo "============================================================"
echo " ✓ Toxiproxy configured. Summary:"
echo ""
echo "   localhost:19092 → broker-east    ${LATENCY_EAST}ms ± ${JITTER_EAST}ms   (dc-east, local)"
echo "   localhost:29092 → broker-west    ${LATENCY_WEST}ms ± ${JITTER_WEST}ms  (dc-west, far)"
echo "   localhost:39092 → broker-central ${LATENCY_CENTRAL}ms ± ${JITTER_CENTRAL}ms  (dc-central, mid)"
echo ""
echo " Current proxy state:"
curl -s "$TOXI_API/proxies" | python3 -m json.tool 2>/dev/null || curl -s "$TOXI_API/proxies"
echo ""
echo "============================================================"
echo ""
echo " Toxiproxy API: $TOXI_API"
echo " To update latency:  curl -X POST $TOXI_API/proxies/broker-west/toxics/broker-west_latency_up -d '{\"attributes\":{\"latency\":100,\"jitter\":10}}'"
echo " To list toxics:     curl $TOXI_API/proxies/broker-west/toxics"
echo " To remove a toxic:  curl -X DELETE $TOXI_API/proxies/broker-west/toxics/broker-west_latency_up"
