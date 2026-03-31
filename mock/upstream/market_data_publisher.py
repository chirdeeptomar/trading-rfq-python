"""
Market Data Publisher

Publishes mock market data for 100 instruments to NATS subject `market.data`.

Each instrument publishes independently on its own cadence (100ms ± jitter),
matching the behaviour of a real bond market data feed where each instrument
ticks at its own rate. All instruments publish concurrently via asyncio tasks
so no instrument is delayed by another.
"""

import asyncio
import random
import signal
import sys
from datetime import datetime, timezone

import nats

from instruments_config import ISINS
from models import MarketData

# Realistic bond market tick interval per instrument: ~100ms ± 20ms jitter
_TICK_INTERVAL_MS = 100
_JITTER_MS = 20


def _now_iso() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


async def _publish_instrument(
    nc: nats.aio.client.Client,
    isin: str,
    price: float,
    stop_event: asyncio.Event,
    counters: dict[str, int],
) -> None:
    """Continuously publish ticks for a single instrument until stopped."""
    while not stop_event.is_set():
        # Gaussian random walk
        price = max(10.0, price + random.gauss(0, 0.05))

        md = MarketData(
            isin=isin,
            mid_price=round(price, 4),
            timestamp=_now_iso(),
        )
        try:
            await nc.publish("market.data", md.to_bytes())
            counters["total"] += 1
        except Exception:
            pass  # connection may be draining on shutdown

        # Tick interval with jitter — yields to event loop between publishes
        interval = (_TICK_INTERVAL_MS + random.randint(-_JITTER_MS, _JITTER_MS)) / 1000.0
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def run() -> None:
    nc = await nats.connect("nats://localhost:4222")
    print(
        f"Connected to NATS. Publishing {len(ISINS)} instruments "
        f"at ~{_TICK_INTERVAL_MS}ms/tick each ..."
    )

    # Stagger initial prices across a realistic range
    prices = {isin: random.uniform(80.0, 120.0) for isin in ISINS}
    counters: dict[str, int] = {"total": 0}
    stop_event = asyncio.Event()

    event_loop = asyncio.get_running_loop()
    event_loop.add_signal_handler(signal.SIGINT, stop_event.set)
    event_loop.add_signal_handler(signal.SIGTERM, stop_event.set)

    # Stagger startup so all 100 instruments don't fire at t=0
    tasks: list[asyncio.Task[None]] = []
    for i, isin in enumerate(ISINS):
        await asyncio.sleep(0.001)  # 1ms stagger between instrument launches
        task = asyncio.create_task(
            _publish_instrument(nc, isin, prices[isin], stop_event, counters)
        )
        tasks.append(task)

    # Progress reporter
    async def _reporter() -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                print(f"Published {counters['total']:,} market data messages")

    reporter = asyncio.create_task(_reporter())

    await stop_event.wait()
    reporter.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    print(f"Shutting down. Total messages published: {counters['total']:,}")
    await nc.drain()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    finally:
        sys.exit(0)
