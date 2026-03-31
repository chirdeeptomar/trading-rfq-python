"""
RFQ Streaming Pipeline

Merges three NATS streams into a stateful Bytewax dataflow that drives
the full RFQ lifecycle:

  RECEIVED → ENRICHED → PRICED → QUOTED
       ├─ client ACKNOWLEDGE → ACKNOWLEDGED → CONCLUDED
       ├─ client PASS        → REJECTED
       └─ TTL exceeded       → EXPIRED

Instrument name resolution uses op.enrich_cached with a Soul REST source.
The TTLCache is keyed by ISIN and re-fetches names every 24 hours — all
other lookups are served from memory with no HTTP call.

A second ISIN-keyed repricing map re-emits QUOTED rows with fresh bid/ask
on every pricing.stream tick so live prices flow to the UI continuously.

Run with:
    python -m bytewax.run pipeline.pipeline:flow
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Optional, TypedDict, cast

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from models import ClientResponse, EnrichedRFQ, PricedQuote, RFQ, RFQState, from_nats_bytes
from pipeline.sources.nats_source import NatsSource
from pipeline.sources.soul_source import soul_getter, soul_mapper
from pipeline.sinks.websocket_sink import WebSocketSink

RFQ_TTL_SECONDS = 60.0

# (tag, payload) — RFQ tag carries (RFQ, instrument_name) pair after enrichment
type TaggedEvent = tuple[str, tuple[RFQ, str] | PricedQuote | ClientResponse | None]
type RepricingEvent = tuple[str, PricedQuote | EnrichedRFQ]

# ---------------------------------------------------------------------------
# Module-level price cache — updated by price events, read by RFQ handler
# ---------------------------------------------------------------------------

_price_cache: dict[str, PricedQuote] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Stream tagging and keying
# ---------------------------------------------------------------------------

def _parse_rfq(raw: bytes) -> Optional[RFQ]:
    """Parse raw bytes → RFQ. Returns None on error (filtered downstream)."""
    try:
        return from_nats_bytes(raw, RFQ)
    except (ValueError, KeyError) as e:
        print(f"[pipeline] Failed to parse RFQ: {e}")
        return None


def _tag_enriched_rfq(pair: tuple[RFQ, str]) -> TaggedEvent:
    return ("rfq", pair)


def _tag_price(raw: bytes) -> TaggedEvent:
    try:
        quote = PricedQuote.from_bytes(raw)
        _price_cache[quote.isin] = quote
        return ("price", quote)
    except (ValueError, KeyError) as e:
        print(f"[pipeline] Failed to parse PricedQuote: {e}")
        return ("error", None)


def _tag_client_response(raw: bytes) -> TaggedEvent:
    try:
        return ("client_response", from_nats_bytes(raw, ClientResponse))
    except (ValueError, KeyError) as e:
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
    if isinstance(payload, tuple):  # (RFQ, instrument_name)
        return payload[0].rfq_id
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
            rfq, instrument_name = cast(tuple[RFQ, str], payload)
            outputs.extend(self._handle_rfq(rfq, instrument_name))
        elif tag == "client_response":
            outputs.extend(self._handle_client_response(payload))  # type: ignore[arg-type]
        outputs.extend(self._check_ttl())
        return outputs

    def _handle_rfq(self, rfq: RFQ, instrument_name: str) -> list[EnrichedRFQ]:
        enriched = EnrichedRFQ(
            rfq_id=rfq.rfq_id,
            isin=rfq.isin,
            instrument_name=instrument_name,
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
# Dataflow runner functions
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


# ---------------------------------------------------------------------------
# Dataflow assembly
# ---------------------------------------------------------------------------

flow = Dataflow("rfq_pipeline")

rfq_raw      = op.input("rfq_source",      flow, NatsSource(subject="rfq.new"))
price_raw    = op.input("price_source",    flow, NatsSource(subject="pricing.stream"))
response_raw = op.input("response_source", flow, NatsSource(subject="rfq.client_response"))

# Parse raw RFQ bytes → RFQ objects, drop parse errors
rfq_parsed = op.filter_map("parse_rfq", rfq_raw, _parse_rfq)

# Enrich with instrument name via Soul REST API — TTLCache keyed by ISIN,
# re-fetches every 24 h, all other lookups are served from memory.
rfq_enriched_named = op.enrich_cached(
    "soul_enrich",
    rfq_parsed,
    soul_getter,
    soul_mapper,
    ttl=timedelta(hours=24),
)

# Tag all three streams
rfq_tagged      = op.map("tag_rfq",      rfq_enriched_named, _tag_enriched_rfq)
price_tagged    = op.map("tag_price",    price_raw,          _tag_price)
response_tagged = op.map("tag_response", response_raw,       _tag_client_response)

# RFQ lifecycle state machine (keyed by rfq_id)
merged           = op.merge("merge_streams", rfq_tagged, price_tagged, response_tagged)
keyed            = op.key_on("key_by_rfq_id", merged, _key_by_rfq_id)
state_out        = op.stateful_map("rfq_state_machine", keyed, _run_state_machine)
lifecycle_stream = op.flat_map("flatten_lifecycle", state_out, lambda pair: pair[1])

# Repricing branch — re-emits QUOTED rows on every price tick for that ISIN
repricing_filtered  = op.filter_map("lifecycle_to_repricing", lifecycle_stream, _lifecycle_to_repricing)
price_for_repricing = op.filter_map("price_to_repricing", price_tagged, _tagged_price_to_repricing)
repricing_merged    = op.merge("merge_repricing", repricing_filtered, price_for_repricing)
repricing_keyed     = op.key_on("key_repricing", repricing_merged, _key_repricing_by_isin)
repricing_out       = op.stateful_map("repricing_machine", repricing_keyed, _run_repricing)
repriced_stream     = op.flat_map("flatten_repriced", repricing_out, lambda pair: pair[1])

final_stream = op.merge("final_merge", lifecycle_stream, repriced_stream)
op.output("ws_out", final_stream, WebSocketSink(port=9001))
