"""
RFQ Streaming Pipeline

Merges three NATS streams and one synchronous HTTP lookup (Soul REST API)
into a stateful Bytewax dataflow that drives the full RFQ lifecycle:

  RECEIVED → ENRICHED → PRICED → QUOTED
       ├─ client ACKNOWLEDGE → ACKNOWLEDGED → CONCLUDED
       ├─ client PASS        → REJECTED
       └─ TTL exceeded       → EXPIRED

A second ISIN-keyed repricing map re-emits QUOTED rows with fresh bid/ask
on every pricing.stream tick so live prices flow to the UI continuously.

Run with:
    python -m bytewax.run pipeline.pipeline:flow
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional, TypedDict

import bytewax.operators as op
import httpx
from bytewax.dataflow import Dataflow

from models import ClientResponse, EnrichedRFQ, PricedQuote, RFQ, RFQState, from_nats_bytes
from pipeline.sources.nats_source import NatsSource
from pipeline.sinks.websocket_sink import WebSocketSink

SOUL_URL = "http://localhost:8000"
RFQ_TTL_SECONDS = 60.0

type TaggedEvent = tuple[str, RFQ | PricedQuote | ClientResponse | None]
type RepricingEvent = tuple[str, PricedQuote | EnrichedRFQ]

# ---------------------------------------------------------------------------
# Module-level price cache — updated by price events, read by RFQ handler
# ---------------------------------------------------------------------------

_price_cache: dict[str, PricedQuote] = {}

# ---------------------------------------------------------------------------
# Soul REST lookup — synchronous, cached per ISIN
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
    tag, payload = tagged
    if tag == "error" or payload is None:
        return "__error__"
    if isinstance(payload, PricedQuote):
        return "__price_sink__"
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
# RFQ lifecycle state machine — one instance per rfq_id
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
            return []

        outputs: list[EnrichedRFQ] = []
        if tag == "rfq":
            outputs.extend(self._handle_rfq(payload))  # type: ignore[arg-type]
        elif tag == "client_response":
            outputs.extend(self._handle_client_response(payload))  # type: ignore[arg-type]
        outputs.extend(self._check_ttl())
        return outputs

    def _handle_rfq(self, rfq: RFQ) -> list[EnrichedRFQ]:
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
        print(f"[pipeline] {rfq.rfq_id[:8]}... RECEIVED → ENRICHED ({rfq.isin})")
        quote = _price_cache.get(rfq.isin)
        if quote is not None:
            return self._apply_price(quote)
        return []

    def _handle_client_response(self, response: ClientResponse) -> list[EnrichedRFQ]:
        if self._record is None:
            return []
        enriched = self._record["enriched"]
        if enriched.state != RFQState.QUOTED:
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
            enriched = enriched.model_copy(update={"state": RFQState.ACKNOWLEDGED, "acknowledged_at": now})
            enriched = enriched.model_copy(update={"state": RFQState.CONCLUDED, "concluded_at": now})
            print(f"[pipeline] {response.rfq_id[:8]}... → ACKNOWLEDGED → CONCLUDED")
        elif response.action == "PASS":
            enriched = enriched.model_copy(update={"state": RFQState.REJECTED, "rejected_at": now})
            print(f"[pipeline] {response.rfq_id[:8]}... → REJECTED (client passed)")
        self._record["enriched"] = enriched
        if enriched.state in _TERMINAL:
            self._record = None
        return [enriched]

    def _apply_price(self, quote: PricedQuote) -> list[EnrichedRFQ]:
        if self._record is None:
            return []
        enriched = self._record["enriched"]
        if enriched.state not in (RFQState.ENRICHED, RFQState.PRICED):
            return []
        now = _now_iso()
        enriched = enriched.model_copy(update={
            "bid": quote.bid, "ask": quote.ask, "spread_bps": quote.spread_bps,
            "state": RFQState.PRICED, "priced_at": now,
        })
        enriched = enriched.model_copy(update={"state": RFQState.QUOTED, "quoted_at": now})
        self._record["enriched"] = enriched
        print(f"[pipeline] {enriched.rfq_id[:8]}... → QUOTED bid={enriched.bid} ask={enriched.ask}")
        return [enriched]

    def _check_ttl(self) -> list[EnrichedRFQ]:
        if self._record is None:
            return []
        enriched = self._record["enriched"]
        if enriched.state in _TERMINAL:
            return []
        if time.time() - self._record["created_ts"] > RFQ_TTL_SECONDS:
            enriched = enriched.model_copy(update={"state": RFQState.EXPIRED, "expired_at": _now_iso()})
            self._record = None
            print(f"[pipeline] {enriched.rfq_id[:8]}... → EXPIRED (TTL {RFQ_TTL_SECONDS}s)")
            return [enriched]
        return []

    def snapshot(self) -> Optional[_RFQRecord]:
        return self._record

    def restore(self, state: Optional[_RFQRecord]) -> None:
        self._record = state


# ---------------------------------------------------------------------------
# ISIN-keyed repricing map — re-emits QUOTED rows on every price tick
# ---------------------------------------------------------------------------

class RepricingStateMachine:
    """Keyed by ISIN. Holds all currently-QUOTED RFQs for that ISIN."""

    def __init__(self) -> None:
        self._quoted: dict[str, EnrichedRFQ] = {}

    def on_item(self, event: RepricingEvent) -> list[EnrichedRFQ]:
        tag, payload = event
        if tag == "quoted":
            enriched: EnrichedRFQ = payload  # type: ignore[assignment]
            self._quoted[enriched.rfq_id] = enriched
            return []
        if tag == "terminal":
            enriched = payload  # type: ignore[assignment]
            self._quoted.pop(enriched.rfq_id, None)
            return []
        if tag == "price":
            quote: PricedQuote = payload  # type: ignore[assignment]
            now = _now_iso()
            outputs: list[EnrichedRFQ] = []
            for rfq_id, enriched in list(self._quoted.items()):
                repriced = enriched.model_copy(update={
                    "bid": quote.bid, "ask": quote.ask,
                    "spread_bps": quote.spread_bps, "priced_at": now,
                })
                self._quoted[rfq_id] = repriced
                outputs.append(repriced)
            return outputs
        return []

    def snapshot(self) -> dict[str, EnrichedRFQ]:
        return self._quoted

    def restore(self, state: dict[str, EnrichedRFQ]) -> None:
        self._quoted = state


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


def _run_repricing(
    state: Optional[RepricingStateMachine],
    event: RepricingEvent,
) -> tuple[RepricingStateMachine, list[EnrichedRFQ]]:
    if state is None:
        state = RepricingStateMachine()
    return state, state.on_item(event)


def _key_repricing_by_isin(event: RepricingEvent) -> str:
    return event[1].isin  # type: ignore[union-attr]


def _lifecycle_to_repricing(enriched: EnrichedRFQ) -> Optional[RepricingEvent]:
    if enriched.state in _TERMINAL:
        return ("terminal", enriched)
    if enriched.state == RFQState.QUOTED:
        return ("quoted", enriched)
    return None


def _tagged_price_to_repricing(tagged: TaggedEvent) -> Optional[RepricingEvent]:
    tag, payload = tagged
    if tag == "price" and payload is not None:
        return ("price", payload)  # type: ignore[return-value]
    return None


flow = Dataflow("rfq_pipeline")

rfq_raw      = op.input("rfq_source",      flow, NatsSource(subject="rfq.new"))
price_raw    = op.input("price_source",    flow, NatsSource(subject="pricing.stream"))
response_raw = op.input("response_source", flow, NatsSource(subject="rfq.client_response"))

rfq_tagged      = op.map("tag_rfq",      rfq_raw,      _tag_rfq)
price_tagged    = op.map("tag_price",    price_raw,    _tag_price)
response_tagged = op.map("tag_response", response_raw, _tag_client_response)

merged          = op.merge("merge_streams", rfq_tagged, price_tagged, response_tagged)
keyed           = op.key_on("key_by_rfq_id", merged, _key_by_rfq_id)
state_out       = op.stateful_map("rfq_state_machine", keyed, _run_state_machine)
lifecycle_stream = op.flat_map("flatten_lifecycle", state_out, lambda pair: pair[1])

repricing_filtered  = op.filter_map("lifecycle_to_repricing", lifecycle_stream, _lifecycle_to_repricing)
price_for_repricing = op.filter_map("price_to_repricing", price_tagged, _tagged_price_to_repricing)
repricing_merged    = op.merge("merge_repricing", repricing_filtered, price_for_repricing)
repricing_keyed     = op.key_on("key_repricing", repricing_merged, _key_repricing_by_isin)
repricing_out       = op.stateful_map("repricing_machine", repricing_keyed, _run_repricing)
repriced_stream     = op.flat_map("flatten_repriced", repricing_out, lambda pair: pair[1])

final_stream = op.merge("final_merge", lifecycle_stream, repriced_stream)
op.output("ws_out", final_stream, WebSocketSink(port=9001))
