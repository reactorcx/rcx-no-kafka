#!/bin/bash
##############################################################################
# reset-latency.sh — Run from the HOST
#
# Removes all latency toxics from Toxiproxy, leaving proxies intact.
##############################################################################

set -euo pipefail

TOXI_API="http://localhost:8474"

echo "Removing all toxics..."

for PROXY in broker-east broker-west broker-central; do
    for DIR in up down; do
        curl -sf -X DELETE "$TOXI_API/proxies/$PROXY/toxics/${PROXY}_latency_$DIR" >/dev/null 2>&1 \
            && echo "  Removed ${PROXY}_latency_$DIR" \
            || echo "  (${PROXY}_latency_$DIR not found, skipping)"
    done
done

echo ""
echo "✓ All latency toxics removed. Proxies still active with zero latency."
