#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_all.sh — start the full trading streaming system
#
# Prerequisites:
#   - uv installed (https://docs.astral.sh/uv/)
#   - Docker Desktop running
#
# Usage:
#   chmod +x run_all.sh
#   ./run_all.sh
#
# Stop everything with Ctrl+C (or kill the script; background processes
# are tracked in /tmp/rfq_pids and cleaned up on exit).
# ---------------------------------------------------------------------------

set -eo pipefail

PIDS=()

cleanup() {
  echo ""
  echo "Shutting down all services..."
  if [ ${#PIDS[@]} -gt 0 ]; then
    for pid in "${PIDS[@]}"; do
      kill "$pid" 2>/dev/null || true
    done
  fi
  docker compose down --remove-orphans 2>/dev/null || true
  echo "Done."
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Step 1: Install Python dependencies
# ---------------------------------------------------------------------------
echo ">>> Installing Python dependencies ..."
uv sync

# ---------------------------------------------------------------------------
# Step 2: Seed the SQLite database (Soul needs this file)
# ---------------------------------------------------------------------------
echo ">>> Seeding instruments database ..."
uv run python -m mock.local.seed_db

# ---------------------------------------------------------------------------
# Step 3: Start NATS + Soul via Docker Compose
# ---------------------------------------------------------------------------
echo ">>> Starting NATS and Soul via Docker Compose ..."
docker compose up -d

# Wait for NATS to be ready (check client port 4222 directly)
echo -n "Waiting for NATS..."
until nc -z localhost 4222 2>/dev/null; do
  printf "."
  sleep 0.5
done
echo " ready."

# Wait for Soul to be ready
echo -n "Waiting for Soul..."
until curl -sf http://localhost:8000/api/tables >/dev/null 2>&1; do
  printf "."
  sleep 0.5
done
echo " ready."

# ---------------------------------------------------------------------------
# Step 4: Start Python services in background
# ---------------------------------------------------------------------------
echo ">>> Starting Market Data Publisher ..."
uv run python -m mock.upstream.market_data_publisher &
PIDS+=($!)

sleep 0.5

echo ">>> Starting Pricing Service ..."
uv run python -m services.pricing_service &
PIDS+=($!)

sleep 0.5

echo ">>> Starting RFQ Publisher ..."
uv run python -m mock.upstream.rfq_publisher &
PIDS+=($!)

sleep 0.5

echo ">>> Starting Bytewax Pipeline ..."
uv run python -m bytewax.run bytewax_pipeline:flow &
PIDS+=($!)

sleep 1

# ---------------------------------------------------------------------------
# Step 5: Start FastAPI WebSocket server (foreground)
# ---------------------------------------------------------------------------
echo ">>> Starting WebSocket server on http://localhost:9000"
echo "    Open http://localhost:9000 in a browser to watch live RFQs."
echo "    Press Ctrl+C to stop everything."
echo ""
uv run uvicorn websocket_server:app --host 0.0.0.0 --port 9000
