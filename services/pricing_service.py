"""
Pricing Service

Subscribes to NATS `market.data`, computes bid/ask spreads,
and publishes priced quotes to NATS `pricing.stream`.

Each instrument has a fixed spread profile (2-15 bps) to simulate
different liquidity tiers.
"""

import asyncio
import random
import signal
import sys

import nats

from instruments_config import ISINS
from models import MarketData, PricedQuote


def _build_spread_table() -> dict[str, float]:
    """Assign a fixed spread in bps to each ISIN (2–15 bps)."""
    rng = random.Random(42)  # deterministic seed for reproducibility
    return {isin: round(rng.uniform(2.0, 15.0), 2) for isin in ISINS}


SPREAD_TABLE = _build_spread_table()


async def run() -> None:
    """Connect to NATS, subscribe to market.data, and publish priced quotes."""
    nc = await nats.connect("nats://localhost:4222")
    print("Pricing service connected to NATS. Listening on market.data ...")

    msg_count = 0

    # One pending quote slot per ISIN — newer tick replaces older pending one
    pending: dict[str, asyncio.Task[None]] = {}

    async def _delayed_publish(quote: PricedQuote) -> None:
        nonlocal msg_count
        # Randomised delay 5ms–5s simulates varying market liquidity per tick
        await asyncio.sleep(random.uniform(0.005, 5.0))
        await nc.publish("pricing.stream", quote.to_bytes())
        msg_count += 1
        if msg_count % 1_000 == 0:
            print(f"Processed {msg_count:,} pricing messages")

    async def handler(msg: nats.aio.client.Msg) -> None:
        try:
            md = MarketData.from_bytes(msg.data)
        except (KeyError, ValueError, TypeError) as e:
            print(f"Failed to parse market data: {e}")
            return

        spread_bps = SPREAD_TABLE.get(md.isin, 5.0)
        half_spread = (md.mid_price * spread_bps) / 20_000.0

        quote = PricedQuote(
            isin=md.isin,
            bid=round(md.mid_price - half_spread, 4),
            ask=round(md.mid_price + half_spread, 4),
            spread_bps=spread_bps,
            timestamp=md.timestamp,
        )

        # Cancel any still-pending publish for this ISIN — only latest price matters
        existing = pending.get(md.isin)
        if existing and not existing.done():
            existing.cancel()
        pending[md.isin] = asyncio.create_task(_delayed_publish(quote))

    sub = await nc.subscribe(
        "market.data",
        cb=handler,
        pending_msgs_limit=131_072,   # 2× NATS default; absorbs bursts
        pending_bytes_limit=64 * 1024 * 1024,  # 64 MB
    )

    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, _signal_handler)
    loop.add_signal_handler(signal.SIGTERM, _signal_handler)

    try:
        await stop_event.wait()
    finally:
        print(f"Shutting down pricing service. Messages processed: {msg_count:,}")
        await sub.unsubscribe()
        await nc.drain()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    finally:
        sys.exit(0)
