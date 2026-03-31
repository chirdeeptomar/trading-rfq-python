"""
Mock RFQ Publisher

Simulates the full client-side RFQ lifecycle from a trading venue:

  1. Publishes a new RFQ (RECEIVED) to `rfq.new` at ~1 per second.
  2. After a realistic delay (1–4s), sends a client response to
     `rfq.client_response`:
       - 70% chance: ACKNOWLEDGE  → pipeline drives to CONCLUDED
       - 20% chance: PASS         → pipeline drives to REJECTED
       - 10% chance: no response  → pipeline TTL drives to EXPIRED

This mirrors how a real venue connection works: the bank quotes,
the client either hits/lifts (ACK), passes, or goes silent.
"""

import argparse
import asyncio
import random
import signal
import sys
import uuid
from datetime import datetime, timezone
from typing import Literal

import nats
from nats.aio.client import Client as NatsClient

from instruments_config import ISINS
from models import RFQ, ClientResponse, RFQState, to_nats_bytes

_CLIENT_IDS = [f"CLIENT_{i:03d}" for i in range(1, 11)]
_QUANTITIES = [100_000, 250_000, 500_000, 1_000_000, 2_000_000, 5_000_000]
_DIRECTIONS: list[Literal["BUY", "SELL"]] = ["BUY", "SELL"]

# Probability weights for client behaviour after receiving a quote
_RESPONSE_WEIGHTS = [
    ("ACKNOWLEDGE", 0.70),  # client hits/lifts the quote → CONCLUDED
    ("PASS", 0.20),  # client rejects the quote    → REJECTED
    ("NO_RESPONSE", 0.10),  # client goes silent          → EXPIRED (TTL)
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pick_response() -> str:
    r = random.random()
    cumulative = 0.0
    for action, weight in _RESPONSE_WEIGHTS:
        cumulative += weight
        if r < cumulative:
            return action
    return "ACKNOWLEDGE"


async def _send_client_response(
    nc: NatsClient,
    rfq_id: str,
    stop_event: asyncio.Event,
) -> None:
    """Wait a realistic delay then send the client's response."""
    # Typical quote lifetime on a bond venue: 1–4 seconds
    delay = random.uniform(0.5, 2.0)
    action = _pick_response()

    if action == "NO_RESPONSE":
        # Simulate a silent client — the pipeline TTL will expire the RFQ
        return

    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay)
        # stop_event fired before the delay — don't send anything
        return
    except asyncio.TimeoutError:
        pass

    if stop_event.is_set():
        return

    response = ClientResponse(rfq_id=rfq_id, action=action)  # type: ignore[arg-type]
    await nc.publish("rfq.client_response", to_nats_bytes(response))
    print(f"[client response] {rfq_id[:8]}... → {action} (after {delay:.1f}s)")


async def run(rate_seconds: float) -> None:
    nc = await nats.connect("nats://localhost:4222")
    print(f"RFQ publisher connected. Sending 1 RFQ every {rate_seconds}s ...")

    count = 0
    stop_event = asyncio.Event()
    pending_responses: list[asyncio.Task[None]] = []

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)

    try:
        while not stop_event.is_set():
            rfq_id = str(uuid.uuid4())
            rfq = RFQ(
                rfq_id=rfq_id,
                isin=random.choice(ISINS),
                quantity=float(random.choice(_QUANTITIES)),
                direction=random.choice(_DIRECTIONS),
                client_id=random.choice(_CLIENT_IDS),
                state=RFQState.RECEIVED,
                created_at=_now_iso(),
            )

            await nc.publish("rfq.new", to_nats_bytes(rfq))
            count += 1
            print(f"[RFQ #{count}] {rfq.rfq_id} | {rfq.isin} | {rfq.direction} {rfq.quantity:,.0f}")

            task = asyncio.create_task(_send_client_response(nc, rfq_id, stop_event))
            pending_responses.append(task)
            pending_responses = [t for t in pending_responses if not t.done()]

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=rate_seconds)
            except asyncio.TimeoutError:
                pass

    finally:
        # Wait for any in-flight client responses to drain
        if pending_responses:
            await asyncio.gather(*pending_responses, return_exceptions=True)
        print(f"RFQ publisher stopped. Total RFQs sent: {count}")
        await nc.drain()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock RFQ publisher")
    parser.add_argument(
        "--rate", type=float, default=1.0, help="Seconds between RFQs (default: 1.0)"
    )
    args = parser.parse_args()

    try:
        asyncio.run(run(args.rate))
    except KeyboardInterrupt:
        pass
    finally:
        sys.exit(0)
