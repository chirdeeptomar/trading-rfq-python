"""
NATS Source for Bytewax

Bridges async NATS-py into Bytewax's synchronous next_batch() API.
A daemon thread runs its own asyncio event loop for NATS I/O; messages
are pushed into a SimpleQueue that next_batch() drains without blocking.

Only worker_index == 0 creates a real subscription; all other workers
get an EmptyPartition that yields immediately.
"""

from __future__ import annotations

import asyncio
import queue
import threading
import time

import nats
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from nats.aio.msg import Msg as NatsMsg

NATS_URL = "nats://localhost:4222"


class _NatsSourcePartition(StatelessSourcePartition[bytes]):
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
        time.sleep(0)
        return []

    def close(self) -> None:
        pass


class NatsSource(DynamicSource[bytes]):
    """Bytewax DynamicSource that subscribes to a single NATS subject."""

    def __init__(self, subject: str) -> None:
        self._subject = subject

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSourcePartition[bytes]:
        if worker_index == 0:
            return _NatsSourcePartition(self._subject)
        return _EmptyPartition()
