# RFQ Streaming System

A real-time bond trading Request for Quote (RFQ) pipeline that models how an investment bank receives client quote requests from a venue, enriches them with live market data, drives them through a full lifecycle state machine, and pushes the results to a WebSocket API.

---

## Architecture

```text
                     ┌───────────────────────┐
                     │  Mock RFQ Publisher   │
                     │  mock/upstream/       │
                     │  rfq_publisher.py     │
                     └──────────┬────────────┘
                                │ rfq.new
                                │ rfq.client_response
                                ▼
┌──────────────────┐    ┌───────────────┐    ┌──────────────────────┐
│  Market Data     │    │               │    │  Soul REST API       │
│  Publisher       │───▶│     NATS      │    │  (SQLite via Docker) │
│  mock/upstream/  │    │               │    │  localhost:8000      │
│  market_data_    │    └───────┬───────┘    └──────────┬───────────┘
│  publisher.py    │            │                       │ HTTP lookup
└──────────────────┘            │ pricing.stream        │ (ISIN → name)
                                ▼                       │
                     ┌──────────────────┐               │
                     │ Pricing Service  │               │
                     │ services/        │               │
                     │ pricing_service  │               │
                     └──────────┬───────┘               │
                                │ pricing.stream        │
                                ▼                       │
                     ┌──────────────────────────────────┴──┐
                     │        Bytewax Pipeline             │
                     │        bytewax_pipeline.py          │
                     │                                     │
                     │  rfq.new ──┐                        │
                     │  pricing   ├──▶ State Machine ──▶   │
                     │  .stream ──┤    (per rfq_id)        │
                     │  rfq.      │                        │
                     │  client_   ┘                        │
                     │  response                           │
                     └──────────────────┬──────────────────┘
                                        │ WebSocket (port 9001)
                                        │ WebSocketSink — direct broadcast
                                        ▼
                             Browser / WS Clients

                     ┌──────────────────────────────────────┐
                     │         FastAPI REST Server          │
                     │         websocket_server.py          │
                     │         localhost:9000               │
                     │  GET /         browser test client   │
                     │  GET /health   liveness              │
                     │  GET /isins    instrument list       │
                     └──────────────────────────────────────┘
```

---

## RFQ Lifecycle

Every RFQ follows a deterministic state machine from arrival to conclusion:

```text
  [Venue]
     │
     │  RECEIVED        RFQ arrives from venue (rfq.new)
     ▼
  ENRICHED              Soul lookup resolves ISIN → instrument name
     │
     │  (price cache hit or next pricing.stream tick)
     ▼
  PRICED                Live bid/ask attached from pricing service
     │
     ▼
  QUOTED                Enriched RFQ emitted to rfq.enriched → WebSocket
     │
     ├──── client ACKNOWLEDGE ────▶  ACKNOWLEDGED ──▶  CONCLUDED  ✓
     │
     ├──── client PASS        ────▶  REJECTED                      ✗
     │
     └──── silence > 30s      ────▶  EXPIRED                       ⏱
```

Terminal states (`CONCLUDED`, `REJECTED`, `EXPIRED`) are emitted to the WebSocket so clients always see the final outcome. State is cleaned up after emission.

---

## Components

### Infrastructure (Docker)

| Service | Image                    | Port        | Purpose                               |
| ------- | ------------------------ | ----------- | ------------------------------------- |
| NATS    | `nats:latest`            | 4222 / 8222 | Message broker with JetStream         |
| Soul    | `dddmaster/soul-docker`  | 8000        | SQLite REST API for instrument lookup |

### Python Services

| Module                                   | NATS subjects                                               | Role                                                                                          |
| ---------------------------------------- | ----------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `mock/upstream/market_data_publisher.py` | → `market.data`                                             | Publishes mid-prices for 100 instruments every 10ms using a Gaussian random walk              |
| `services/pricing_service.py`            | `market.data` → `pricing.stream`                            | Computes bid/ask spread (2–15 bps per instrument, deterministic)                              |
| `mock/upstream/rfq_publisher.py`         | → `rfq.new`, → `rfq.client_response`                        | Publishes new RFQs at ~1/s; after 1–4s sends ACKNOWLEDGE (70%), PASS (20%), or silent (10%) |
| `bytewax_pipeline.py`                    | `rfq.new`, `pricing.stream`, `rfq.client_response` → `rfq.enriched` | Merges all streams, runs the RFQ state machine, publishes enriched RFQs              |
| `websocket_server.py`                    | `rfq.enriched`                                              | Broadcasts enriched RFQs over WebSocket; serves ISIN cache from Soul on connect               |

### Shared Modules

| File                       | Purpose                                                                                                   |
| -------------------------- | --------------------------------------------------------------------------------------------------------- |
| `instruments_config.py`    | 100 deterministic ISINs and instrument names, imported by all services                                    |
| `models.py`                | Pydantic models (`RFQ`, `EnrichedRFQ`, `ClientResponse`) and dataclasses (`MarketData`, `PricedQuote`); shared serialisation helpers |
| `mock/local/seed_db.py`    | Creates `instruments.db` with the 100 instruments — must run before Soul starts                           |

---

## Bytewax Pipeline Design

The pipeline is the core of the system. It merges three NATS streams and one synchronous HTTP lookup into a single stateful dataflow.

### Keying Strategy

All events (RFQs and client responses) are keyed by **`rfq_id`**, ensuring every state transition for a given RFQ lands on the same Bytewax worker partition. Pricing events are different — they arrive per-ISIN at high frequency (10,000/s) and are not stateful per-RFQ, so they are written directly to a **module-level price cache** (`dict[isin → PricedQuote]`) as a side-effect during tagging, then discarded from the keyed stream. This avoids a fan-out problem where a single price tick would need to touch every in-flight RFQ for that ISIN.

### Stream Merge

```text
rfq.new           ──map(tag_rfq)──────────┐
pricing.stream    ──map(tag_price)─────── merge ──key_on(rfq_id)──▶ stateful_map ──flat_map──▶ rfq.enriched
rfq.client_resp   ──map(tag_response)─────┘
```

### State Machine (`RFQStateMachine`)

One instance per `rfq_id`, managed by Bytewax's `stateful_map`. On each event:

- **`rfq` event**: Soul HTTP lookup (cached per ISIN), transitions `RECEIVED → ENRICHED`. If a price exists in the cache, immediately continues to `PRICED → QUOTED`.
- **`price` event**: Updates the module-level cache only. No state machine transition — the next RFQ event for that ISIN will pick up the price.
- **`client_response` event**: `ACKNOWLEDGE` → `ACKNOWLEDGED → CONCLUDED`; `PASS` → `REJECTED`.
- **TTL check**: On every event, if the RFQ has been alive > 30s without reaching a terminal state → `EXPIRED`.

### NATS Bridge

Bytewax's runtime is synchronous. NATS-py is async. The bridge:

- `NatsSource`: daemon thread runs its own `asyncio` event loop, pushes messages into a `queue.SimpleQueue`; `next_batch()` drains the queue without blocking.
- `NatsSink`: dedicated `asyncio` event loop per sink partition; `write_batch()` calls `loop.run_until_complete()` per message.

---

## Data Models

### `EnrichedRFQ` — the fully assembled RFQ

```json
{
  "rfq_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "isin": "GB00B000001",
  "instrument_name": "Acme Corp 1.5% 2026 Bond",
  "quantity": 1000000.0,
  "direction": "BUY",
  "client_id": "CLIENT_003",
  "state": "CONCLUDED",
  "bid": 101.2300,
  "ask": 101.2554,
  "spread_bps": 5.0,
  "created_at":      "2026-03-29T10:00:00.000Z",
  "enriched_at":     "2026-03-29T10:00:00.012Z",
  "priced_at":       "2026-03-29T10:00:00.013Z",
  "quoted_at":       "2026-03-29T10:00:00.013Z",
  "acknowledged_at": "2026-03-29T10:00:01.823Z",
  "concluded_at":    "2026-03-29T10:00:01.823Z",
  "rejected_at": null,
  "expired_at": null
}
```

### `ClientResponse` — venue feedback

```json
{ "rfq_id": "3fa85f64-...", "action": "ACKNOWLEDGE" }
```

`action` is either `ACKNOWLEDGE` (client hits/lifts the quote) or `PASS` (client declines).

---

## Instruments

100 fixed instruments are generated deterministically in `instruments_config.py` and stored in SQLite. The Soul REST API exposes them at:

```text
GET http://localhost:8000/api/tables/instruments/rows
GET http://localhost:8000/api/tables/instruments/rows?_filters=isin:GB00B000001&_limit=1
```

ISINs follow the pattern `{CC}00B{NNNNNN}` (e.g., `GB00B000001`, `US00B000002`) across 10 country codes. Names combine issuer, coupon, maturity, and instrument type (e.g., `Acme Corp 1.5% 2026 Bond`).

---

## WebSocket API

Connect to `ws://localhost:9000/ws`.

On connection you immediately receive the ISIN cache:

```json
{ "type": "isin_cache", "instruments": [{ "isin": "GB00B000001", "name": "Acme Corp 1.5% 2026 Bond" }, ...] }
```

Subsequent messages are `EnrichedRFQ` JSON objects pushed as each state transition occurs. A single RFQ will appear multiple times as it transitions (e.g., `QUOTED` then `CONCLUDED`).

**REST endpoints:**

| Method | Path      | Description                             |
| ------ | --------- | --------------------------------------- |
| `GET`  | `/`       | Browser test client (dark-themed live log) |
| `GET`  | `/health` | `{"status": "ok", "clients": N}`        |
| `GET`  | `/isins`  | Full instrument list from Soul cache    |
| `WS`   | `/ws`     | Live enriched RFQ stream                |

---

## Getting Started

### Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/) package manager
- Docker Desktop

### Run everything

```bash
./run_all.sh
```

Then open <http://localhost:9000> in a browser to watch RFQs flow through their lifecycle in real time.

### Run services individually

```bash
# 1. Seed the instrument database (once)
uv run python -m mock.local.seed_db

# 2. Start infrastructure
docker compose up -d

# 3. Start services (each in a separate terminal)
uv run python -m mock.upstream.market_data_publisher
uv run python -m services.pricing_service
uv run python -m mock.upstream.rfq_publisher
uv run python -m bytewax.run bytewax_pipeline:flow
uv run uvicorn websocket_server:app --host 0.0.0.0 --port 9000
```

### RFQ publisher options

```bash
# Send 2 RFQs per second instead of 1
uv run python -m mock.upstream.rfq_publisher --rate 0.5
```

---

## Project Structure

```text
.
├── docker-compose.yml              # NATS + Soul
├── pyrightconfig.json              # Type checker config
├── pyproject.toml                  # Dependencies (uv)
├── run_all.sh                      # One-command startup
│
├── instruments_config.py           # 100 ISINs and names (shared constant)
├── models.py                       # All data models and enums
│
├── mock/
│   ├── upstream/
│   │   ├── market_data_publisher.py  # Simulates market data feed
│   │   └── rfq_publisher.py          # Simulates venue RFQ + client responses
│   └── local/
│       └── seed_db.py                # Seeds instruments.db for Soul
│
├── services/
│   └── pricing_service.py          # Bid/ask pricing engine
│
├── bytewax_pipeline.py             # Core streaming pipeline + state machine
└── websocket_server.py             # FastAPI WebSocket + REST API
```

---

## NATS Subjects

| Subject               | Producer               | Consumer           | Content                                    |
| --------------------- | ---------------------- | ------------------ | ------------------------------------------ |
| `market.data`         | `market_data_publisher`| `pricing_service`  | `MarketData` — mid price per ISIN          |
| `pricing.stream`      | `pricing_service`      | `bytewax_pipeline` | `PricedQuote` — bid/ask per ISIN           |
| `rfq.new`             | `rfq_publisher`        | `bytewax_pipeline` | `RFQ` — new client quote request           |
| `rfq.client_response` | `rfq_publisher`        | `bytewax_pipeline` | `ClientResponse` — ACK or PASS             |
| `rfq.enriched`        | `bytewax_pipeline`     | `websocket_server` | `EnrichedRFQ` — every state transition     |

---

## Technology Choices

| Concern            | Choice              | Reason                                                           |
| ------------------ | ------------------- | ---------------------------------------------------------------- |
| Message broker     | NATS                | Low-latency pub/sub; simple deploy; JetStream for future durability |
| Stream processing  | Bytewax             | Python-native dataflow with stateful operators; no JVM           |
| Instrument lookup  | Soul + SQLite       | Zero-code REST API over SQLite; fits the read-heavy, write-once pattern |
| API / WebSocket    | FastAPI + uvicorn   | Async-native; WebSocket support built in                         |
| Serialisation      | Pydantic v2         | Fast JSON validation; schema enforcement across service boundaries |
| Package management | uv                  | Fast dependency resolution; Python version pinning               |
