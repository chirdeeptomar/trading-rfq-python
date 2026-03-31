"""
Bytewax Streaming Pipeline

Merges four data streams:
  1. rfq.new              — incoming RFQs from the venue (RECEIVED)
  2. pricing.stream       — live bid/ask prices per ISIN
  3. rfq.client_response  — client ACK / PASS responses from the venue
  4. Soul REST API        — instrument name lookup (called once per ISIN, cached)

Full RFQ lifecycle driven entirely by this pipeline:

  RECEIVED
    └─ Soul lookup         → ENRICHED
         └─ price arrives  → PRICED
              └─ emit      → QUOTED       (pushed directly to WebSocket clients)
                   ├─ client ACKNOWLEDGE  → ACKNOWLEDGED → CONCLUDED
                   ├─ client PASS         → REJECTED
                   └─ TTL exceeded        → EXPIRED

Output
------
Enriched RFQs are pushed directly to connected WebSocket clients via a
WebSocketSink (port 9001) — no NATS round-trip. The sink runs a
websockets.serve server in a background thread and uses websockets.broadcast
(synchronous) inside write_batch, which fits Bytewax's synchronous sink API.

Keying strategy
---------------
All events are keyed by rfq_id so every state transition for a given RFQ
lands on the same Bytewax worker partition. Pricing events are written to
a module-level price cache (dict keyed by ISIN) that all partitions share
in-process; the "price" tagged events are only used to update that cache,
not to trigger state transitions directly through the keyed state machine.

Run with:
    python -m bytewax.run bytewax_pipeline:flow
"""

from __future__ import annotations

import asyncio
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Optional, TypedDict

import bytewax.operators as op
import httpx
import nats
import websockets
import websockets.asyncio.server
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from nats.aio.msg import Msg as NatsMsg

from models import (
    ClientResponse,
    EnrichedRFQ,
    PricedQuote,
    RFQ,
    RFQState,
    from_nats_bytes,
)

NATS_URL = "nats://localhost:4222"
SOUL_URL = "http://localhost:8000"
RFQ_TTL_SECONDS = 60.0  # 1 minute — UI also enforces this client-side

# Tagged stream item: (tag, payload)
type TaggedEvent = tuple[str, RFQ | PricedQuote | ClientResponse | None]

# Tagged event for the ISIN-keyed repricing map
type RepricingEvent = tuple[str, PricedQuote | EnrichedRFQ]

# ---------------------------------------------------------------------------
# Module-level price cache — updated by price events, read by RFQ handler
# ---------------------------------------------------------------------------

_price_cache: dict[str, PricedQuote] = {}  # keyed by ISIN


# ---------------------------------------------------------------------------
# Soul REST lookup (synchronous, cached)
# ---------------------------------------------------------------------------

_soul_cache: dict[str, str] = {}
_soul_client = httpx.Client(base_url=SOUL_URL, timeout=2.0)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def soul_lookup(isin: str) -> str:
    """Return instrument name for ISIN, cached after first call."""
    if isin in _soul_cache:
        return _soul_cache[isin]
    try:
        resp = _soul_client.get(
            "/api/tables/instruments/rows",
            params={"_filters": f"isin:{isin}", "_limit": "1"},
        )
        resp.raise_for_status()
        data: list[dict[str, str]] = resp.json().get("data", [])
        name = data[0]["name"] if data else f"Unknown ({isin})"
    except Exception as e:
        print(f"[soul_lookup] Failed for {isin}: {e}")
        name = f"Unknown ({isin})"
    _soul_cache[isin] = name
    return name


# ---------------------------------------------------------------------------
# NATS Source
# ---------------------------------------------------------------------------


class _NatsSourcePartition(StatelessSourcePartition[bytes]):
    """
    Bridges async NATS-py into Bytewax's synchronous next_batch() API.
    A daemon thread runs its own asyncio event loop for NATS I/O.
    """

    def __init__(self, subject: str) -> None:
        self._subject = subject
        self._q: queue.SimpleQueue[bytes] = queue.SimpleQueue()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"nats-source-{subject}",
            daemon=True,
        )
        self._thread.start()

    def _run_loop(self) -> None:
        self._loop.run_until_complete(self._subscribe())

    async def _subscribe(self) -> None:
        nc = await nats.connect(NATS_URL)
        print(f"[NatsSource] Subscribed to '{self._subject}'")

        async def _cb(msg: NatsMsg) -> None:
            self._q.put_nowait(msg.data)

        await nc.subscribe(self._subject, cb=_cb)
        while True:
            await asyncio.sleep(1)

    def next_batch(self) -> list[bytes]:
        items: list[bytes] = []
        while True:
            try:
                items.append(self._q.get_nowait())
            except queue.Empty:
                break
        return items

    def close(self) -> None:
        pass


class _EmptyPartition(StatelessSourcePartition[bytes]):
    def next_batch(self) -> list[bytes]:
        # Minimal yield — avoids spinning the CPU on idle worker partitions
        time.sleep(0)
        return []

    def close(self) -> None:
        pass


class NatsSource(DynamicSource[bytes]):
    def __init__(self, subject: str) -> None:
        self._subject = subject

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSourcePartition[bytes]:
        if worker_index == 0:
            return _NatsSourcePartition(self._subject)
        return _EmptyPartition()


# ---------------------------------------------------------------------------
# WebSocket Sink
# ---------------------------------------------------------------------------


class _WebSocketSinkPartition(StatelessSinkPartition[EnrichedRFQ]):
    """Bytewax sink that broadcasts enriched RFQs to WebSocket clients."""

    def __init__(self, port: int) -> None:
        self._connections: set[websockets.asyncio.server.ServerConnection] = set()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_server,
            name="ws-sink-server",
            daemon=True,
        )
        self._port = port
        self._server_ready = threading.Event()
        self._thread.start()
        self._server_ready.wait(timeout=5.0)

    def _run_server(self) -> None:
        self._loop.run_until_complete(self._serve())

    async def _serve(self) -> None:
        async def _handler(ws: websockets.asyncio.server.ServerConnection) -> None:
            self._connections.add(ws)
            client = ws.remote_address
            print(f"[WebSocketSink] Client connected: {client} (total: {len(self._connections)})")
            try:
                await ws.wait_closed()
            finally:
                self._connections.discard(ws)
                print(f"[WebSocketSink] Client disconnected: {client} (remaining: {len(self._connections)})")

        async with websockets.asyncio.server.serve(_handler, "0.0.0.0", self._port) as server:
            print(f"[WebSocketSink] Listening on port {self._port}")
            self._server_ready.set()
            await server.serve_forever()

    def write_batch(self, items: list[EnrichedRFQ]) -> None:
        if not self._connections or not items:
            return
        for enriched in items:
            payload = enriched.model_dump_json()
            websockets.broadcast(self._connections, payload)

    def close(self) -> None:
        pass


class WebSocketSink(DynamicSink[EnrichedRFQ]):
    """DynamicSink that serves a websockets server and broadcasts enriched RFQs."""

    def __init__(self, port: int) -> None:
        self._port = port

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _WebSocketSinkPartition:
        if worker_index == 0:
            return _WebSocketSinkPartition(self._port)
        # Non-primary workers return a no-op partition
        return _NoOpSinkPartition()


class _NoOpSinkPartition(StatelessSinkPartition[EnrichedRFQ]):
    """No-op sink for non-primary Bytewax workers."""

    def write_batch(self, items: list[EnrichedRFQ]) -> None:
        pass

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Stream tagging and keying
# ---------------------------------------------------------------------------


def _tag_rfq(raw: bytes) -> TaggedEvent:
    try:
        return ("rfq", from_nats_bytes(raw, RFQ))
    except Exception as e:
        print(f"[pipeline] Failed to parse RFQ: {e}")
        return ("error", None)


def _tag_price(raw: bytes) -> TaggedEvent:
    try:
        quote = PricedQuote.from_bytes(raw)
        # Update the shared price cache immediately — all workers see this
        _price_cache[quote.isin] = quote
        return ("price", quote)
    except Exception as e:
        print(f"[pipeline] Failed to parse PricedQuote: {e}")
        return ("error", None)


def _tag_client_response(raw: bytes) -> TaggedEvent:
    try:
        return ("client_response", from_nats_bytes(raw, ClientResponse))
    except Exception as e:
        print(f"[pipeline] Failed to parse ClientResponse: {e}")
        return ("error", None)


def _key_by_rfq_id(tagged: TaggedEvent) -> str:
    """
    Key every event by rfq_id so all transitions for a given RFQ land on
    the same worker partition.

    Price events are keyed to a fixed sentinel — they only exist to update
    _price_cache and are not processed by the stateful machine.
    """
    tag, payload = tagged
    if tag == "error" or payload is None:
        return "__error__"
    if isinstance(payload, PricedQuote):
        return "__price_sink__"  # price already stored in _price_cache
    if isinstance(payload, ClientResponse):
        return payload.rfq_id
    if isinstance(payload, RFQ):
        return payload.rfq_id
    return "__error__"


# ---------------------------------------------------------------------------
# State types
# ---------------------------------------------------------------------------


class _RFQRecord(TypedDict):
    enriched: EnrichedRFQ
    created_ts: float


_TERMINAL = {RFQState.CONCLUDED, RFQState.REJECTED, RFQState.EXPIRED}


# ---------------------------------------------------------------------------
# RFQ State Machine  (keyed by rfq_id — one RFQ per partition key)
# ---------------------------------------------------------------------------


class RFQStateMachine:
    """Drives a single RFQ through its complete lifecycle."""

    def __init__(self) -> None:
        self._record: Optional[_RFQRecord] = None

    def on_item(self, tagged: TaggedEvent) -> list[EnrichedRFQ]:
        tag, payload = tagged

        if tag == "error" or payload is None:
            return []
        if tag == "price":
            # Price was already written to _price_cache in _tag_price;
            # nothing to do in the state machine.
            return []

        outputs: list[EnrichedRFQ] = []

        if tag == "rfq":
            outputs.extend(self._handle_rfq(payload))  # type: ignore[arg-type]
        elif tag == "client_response":
            outputs.extend(self._handle_client_response(payload))  # type: ignore[arg-type]

        # Check TTL on every event for this rfq_id
        outputs.extend(self._check_ttl())
        return outputs

    # ------------------------------------------------------------------

    def _handle_rfq(self, rfq: RFQ) -> list[EnrichedRFQ]:
        # RECEIVED → ENRICHED
        name = soul_lookup(rfq.isin)
        enriched = EnrichedRFQ(
            rfq_id=rfq.rfq_id,
            isin=rfq.isin,
            instrument_name=name,
            quantity=rfq.quantity,
            direction=rfq.direction,
            client_id=rfq.client_id,
            state=RFQState.ENRICHED,
            created_at=rfq.created_at,
            enriched_at=_now_iso(),
        )
        self._record = _RFQRecord(enriched=enriched, created_ts=time.time())
        print(f"[pipeline] {rfq.rfq_id[:8]}... RECEIVED → ENRICHED  ({rfq.isin})")

        # If a price is already in the cache, proceed immediately to QUOTED
        quote = _price_cache.get(rfq.isin)
        if quote is not None:
            return self._apply_price(quote)

        return []  # park — wait for next event to trigger re-check

    def _handle_client_response(self, response: ClientResponse) -> list[EnrichedRFQ]:
        if self._record is None:
            return []

        enriched = self._record["enriched"]

        # Only act on QUOTED RFQs — ignore responses in other states
        if enriched.state != RFQState.QUOTED:
            # RFQ may still be ENRICHED/PRICED — try to price it first
            if enriched.state in (RFQState.ENRICHED, RFQState.PRICED):
                quote = _price_cache.get(enriched.isin)
                if quote is not None:
                    priced = self._apply_price(quote)
                    if priced:
                        enriched = self._record["enriched"]
                    else:
                        return []
                else:
                    return []
            else:
                return []

        now = _now_iso()
        if response.action == "ACKNOWLEDGE":
            enriched = enriched.model_copy(update={
                "state": RFQState.ACKNOWLEDGED,
                "acknowledged_at": now,
            })
            enriched = enriched.model_copy(update={
                "state": RFQState.CONCLUDED,
                "concluded_at": now,
            })
            print(f"[pipeline] {response.rfq_id[:8]}... → ACKNOWLEDGED → CONCLUDED")
        elif response.action == "PASS":
            enriched = enriched.model_copy(update={
                "state": RFQState.REJECTED,
                "rejected_at": now,
            })
            print(f"[pipeline] {response.rfq_id[:8]}... → REJECTED (client passed)")

        self._record["enriched"] = enriched
        if enriched.state in _TERMINAL:
            self._record = None
        return [enriched]

    def _apply_price(self, quote: PricedQuote) -> list[EnrichedRFQ]:
        """Transition ENRICHED → PRICED → QUOTED and emit."""
        if self._record is None:
            return []

        enriched = self._record["enriched"]
        if enriched.state not in (RFQState.ENRICHED, RFQState.PRICED):
            return []

        now = _now_iso()
        enriched = enriched.model_copy(update={
            "bid": quote.bid,
            "ask": quote.ask,
            "spread_bps": quote.spread_bps,
            "state": RFQState.PRICED,
            "priced_at": now,
        })
        enriched = enriched.model_copy(update={
            "state": RFQState.QUOTED,
            "quoted_at": now,
        })
        self._record["enriched"] = enriched
        print(
            f"[pipeline] {enriched.rfq_id[:8]}... → QUOTED  "
            f"bid={enriched.bid} ask={enriched.ask}"
        )
        return [enriched]

    def _check_ttl(self) -> list[EnrichedRFQ]:
        if self._record is None:
            return []
        enriched = self._record["enriched"]
        if enriched.state in _TERMINAL:
            return []
        elapsed = time.time() - self._record["created_ts"]
        if elapsed > RFQ_TTL_SECONDS:
            enriched = enriched.model_copy(update={
                "state": RFQState.EXPIRED,
                "expired_at": _now_iso(),
            })
            self._record = None
            print(f"[pipeline] {enriched.rfq_id[:8]}... → EXPIRED (TTL {RFQ_TTL_SECONDS}s)")
            return [enriched]
        return []

    def snapshot(self) -> Optional[_RFQRecord]:
        return self._record

    def restore(self, state: Optional[_RFQRecord]) -> None:
        self._record = state


# ---------------------------------------------------------------------------
# ISIN-keyed repricing map
# Tracks all QUOTED RFQs per ISIN and re-emits them on every price tick.
# ---------------------------------------------------------------------------


class RepricingStateMachine:
    """Keyed by ISIN. Holds all currently-QUOTED RFQs for that ISIN."""

    def __init__(self) -> None:
        self._quoted: dict[str, EnrichedRFQ] = {}  # rfq_id → EnrichedRFQ

    def on_item(self, event: RepricingEvent) -> list[EnrichedRFQ]:
        tag, payload = event

        if tag == "quoted":
            # Register or update a QUOTED RFQ
            enriched = payload  # type: ignore[assignment]
            self._quoted[enriched.rfq_id] = enriched
            return []

        if tag == "terminal":
            # Remove RFQ from the repricing set — it's done
            enriched = payload  # type: ignore[assignment]
            self._quoted.pop(enriched.rfq_id, None)
            return []

        if tag == "price":
            # New price tick — reprice every QUOTED RFQ for this ISIN
            quote: PricedQuote = payload  # type: ignore[assignment]
            now = _now_iso()
            outputs: list[EnrichedRFQ] = []
            for rfq_id, enriched in list(self._quoted.items()):
                repriced = enriched.model_copy(update={
                    "bid": quote.bid,
                    "ask": quote.ask,
                    "spread_bps": quote.spread_bps,
                    "priced_at": now,
                })
                self._quoted[rfq_id] = repriced
                outputs.append(repriced)
            return outputs

        return []

    def snapshot(self) -> dict[str, EnrichedRFQ]:
        return self._quoted

    def restore(self, state: dict[str, EnrichedRFQ]) -> None:
        self._quoted = state


def _run_repricing(
    state: Optional[RepricingStateMachine],
    event: RepricingEvent,
) -> tuple[RepricingStateMachine, list[EnrichedRFQ]]:
    if state is None:
        state = RepricingStateMachine()
    return state, state.on_item(event)


def _key_repricing_by_isin(event: RepricingEvent) -> str:
    tag, payload = event
    if tag == "price":
        return payload.isin  # type: ignore[union-attr]
    return payload.isin  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Dataflow assembly
# ---------------------------------------------------------------------------


def _run_state_machine(
    state: Optional[RFQStateMachine],
    tagged: TaggedEvent,
) -> tuple[RFQStateMachine, list[EnrichedRFQ]]:
    if state is None:
        state = RFQStateMachine()
    return state, state.on_item(tagged)


flow = Dataflow("rfq_pipeline")

# Three NATS input streams
rfq_raw      = op.input("rfq_source",      flow, NatsSource(subject="rfq.new"))
price_raw    = op.input("price_source",    flow, NatsSource(subject="pricing.stream"))
response_raw = op.input("response_source", flow, NatsSource(subject="rfq.client_response"))

# Tag — price events update _price_cache as a side-effect in _tag_price
rfq_tagged      = op.map("tag_rfq",      rfq_raw,      _tag_rfq)
price_tagged    = op.map("tag_price",    price_raw,    _tag_price)
response_tagged = op.map("tag_response", response_raw, _tag_client_response)

# Merge all three and run the RFQ lifecycle state machine (keyed by rfq_id)
merged   = op.merge("merge_streams", rfq_tagged, price_tagged, response_tagged)
keyed    = op.key_on("key_by_rfq_id", merged, _key_by_rfq_id)
state_out = op.stateful_map("rfq_state_machine", keyed, _run_state_machine)
lifecycle_stream = op.flat_map("flatten_lifecycle", state_out, lambda pair: pair[1])

# ---------- Repricing branch ----------
# Split lifecycle output: QUOTED → register for repricing; terminal → deregister; others → drop
def _lifecycle_to_repricing(enriched: EnrichedRFQ) -> Optional[RepricingEvent]:
    if enriched.state in _TERMINAL:
        return ("terminal", enriched)
    if enriched.state == RFQState.QUOTED:
        return ("quoted", enriched)
    return None

repricing_filtered = op.filter_map(
    "lifecycle_to_repricing", lifecycle_stream, _lifecycle_to_repricing
)

# Convert price_tagged (TaggedEvent) → RepricingEvent, dropping errors
def _tagged_price_to_repricing(tagged: TaggedEvent) -> Optional[RepricingEvent]:
    tag, payload = tagged
    if tag == "price" and payload is not None:
        return ("price", payload)  # type: ignore[return-value]
    return None

price_for_repricing = op.filter_map(
    "price_to_repricing", price_tagged, _tagged_price_to_repricing
)

# Merge lifecycle events and price ticks into the ISIN-keyed repricing map
repricing_merged = op.merge("merge_repricing", repricing_filtered, price_for_repricing)
repricing_keyed  = op.key_on("key_repricing", repricing_merged, _key_repricing_by_isin)
repricing_out    = op.stateful_map("repricing_machine", repricing_keyed, _run_repricing)
repriced_stream  = op.flat_map("flatten_repriced", repricing_out, lambda pair: pair[1])

# Merge lifecycle transitions + live repriced quotes → single output stream
final_stream = op.merge("final_merge", lifecycle_stream, repriced_stream)

# Broadcast all enriched RFQs directly to WebSocket clients on port 9001
op.output("ws_out", final_stream, WebSocketSink(port=9001))
