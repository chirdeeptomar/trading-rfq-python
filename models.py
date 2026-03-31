"""
Shared data models for the trading streaming system.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Literal, Optional, Type, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


# ---------------------------------------------------------------------------
# Market data (internal, lightweight)
# ---------------------------------------------------------------------------

@dataclass
class MarketData:
    isin: str
    mid_price: float
    timestamp: str

    def to_bytes(self) -> bytes:
        return json.dumps({
            "isin": self.isin,
            "mid_price": self.mid_price,
            "timestamp": self.timestamp,
        }).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "MarketData":
        d = json.loads(data)
        return cls(isin=d["isin"], mid_price=d["mid_price"], timestamp=d["timestamp"])


@dataclass
class PricedQuote:
    isin: str
    bid: float
    ask: float
    spread_bps: float
    timestamp: str

    def to_bytes(self) -> bytes:
        return json.dumps({
            "isin": self.isin,
            "bid": self.bid,
            "ask": self.ask,
            "spread_bps": self.spread_bps,
            "timestamp": self.timestamp,
        }).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "PricedQuote":
        d = json.loads(data)
        return cls(
            isin=d["isin"],
            bid=d["bid"],
            ask=d["ask"],
            spread_bps=d["spread_bps"],
            timestamp=d["timestamp"],
        )


# ---------------------------------------------------------------------------
# RFQ state machine
# ---------------------------------------------------------------------------

class RFQState(str, Enum):
    RECEIVED = "RECEIVED"
    ENRICHED = "ENRICHED"       # instrument name resolved via Soul
    PRICED = "PRICED"           # live bid/ask attached
    QUOTED = "QUOTED"           # quote sent to client
    ACKNOWLEDGED = "ACKNOWLEDGED"  # client acknowledged
    CONCLUDED = "CONCLUDED"     # trade completed
    EXPIRED = "EXPIRED"         # TTL exceeded before pricing
    REJECTED = "REJECTED"       # spread too wide or business rule


# ---------------------------------------------------------------------------
# RFQ models (Pydantic for JSON serialisation)
# ---------------------------------------------------------------------------

class RFQ(BaseModel):
    rfq_id: str
    isin: str
    quantity: float
    direction: Literal["BUY", "SELL"]
    client_id: str
    state: RFQState = RFQState.RECEIVED
    created_at: str


class ClientResponse(BaseModel):
    rfq_id: str
    action: Literal["ACKNOWLEDGE", "PASS"]  # PASS = client rejects the quote


class EnrichedRFQ(BaseModel):
    rfq_id: str
    isin: str
    instrument_name: str
    quantity: float
    direction: Literal["BUY", "SELL"]
    client_id: str
    state: RFQState
    bid: Optional[float] = None
    ask: Optional[float] = None
    spread_bps: Optional[float] = None
    created_at: str
    enriched_at: Optional[str] = None
    priced_at: Optional[str] = None
    quoted_at: Optional[str] = None
    acknowledged_at: Optional[str] = None
    concluded_at: Optional[str] = None
    rejected_at: Optional[str] = None
    expired_at: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def to_nats_bytes(model: BaseModel) -> bytes:
    return model.model_dump_json().encode()


def from_nats_bytes(data: bytes, cls: Type[T]) -> T:
    return cls.model_validate_json(data)
