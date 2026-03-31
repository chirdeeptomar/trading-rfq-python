"""
Soul REST API integration for op.enrich_cached.

Provides:
  soul_getter(isin: str) -> str
      Called by enrich_cached on cache miss / TTL expiry.
      Fetches the instrument name from the Soul REST API.

  soul_mapper(cache, rfq) -> tuple[RFQ, str]
      Mapper passed to enrich_cached. Returns (rfq, instrument_name)
      so the pipeline can pass both into the state machine without
      mutating the immutable RFQ model.

Usage:
    from datetime import timedelta
    from pipeline.sources.soul_source import soul_getter, soul_mapper

    rfq_named = op.enrich_cached(
        "soul_enrich", rfq_stream, soul_getter, soul_mapper,
        ttl=timedelta(hours=24),
    )
    # rfq_named carries Stream[tuple[RFQ, str]]
"""

from __future__ import annotations

import httpx
from bytewax.operators import TTLCache

from models import RFQ

SOUL_URL = "http://localhost:8000"

_soul_client = httpx.Client(base_url=SOUL_URL, timeout=2.0)


def soul_getter(isin: str) -> str:
    """
    Fetch instrument name for an ISIN from Soul.
    Called by enrich_cached on every cache miss or TTL expiry.
    """
    try:
        resp = _soul_client.get(
            "/api/tables/instruments/rows",
            params={"_filters": f"isin:{isin}", "_limit": "1"},
        )
        resp.raise_for_status()
        data: list[dict[str, str]] = resp.json().get("data", [])
        name = data[0]["name"] if data else f"Unknown ({isin})"
        print(f"[SoulSource] Fetched '{name}' for {isin}")
        return name
    except (httpx.HTTPError, httpx.InvalidURL, KeyError, ValueError) as e:
        print(f"[SoulSource] Lookup failed for {isin}: {e}")
        return f"Unknown ({isin})"


def soul_mapper(cache: TTLCache[str, str], rfq: RFQ) -> tuple[RFQ, str]:
    """
    Mapper for enrich_cached. Resolves instrument name via the TTL cache
    (keyed by ISIN) and returns (rfq, instrument_name) as a pair.

    Returning a pair keeps RFQ immutable and gives the state machine
    everything it needs without a separate Soul HTTP call.
    """
    instrument_name = cache.get(rfq.isin)
    return rfq, instrument_name
