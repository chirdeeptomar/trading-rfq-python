"""
WebSocket Sink for Bytewax

Runs a websockets server in a daemon thread (port configurable).
write_batch() calls websockets.broadcast() — synchronous — which fits
Bytewax's synchronous sink API perfectly.

Only worker_index == 0 starts the real server; all other workers get a
no-op partition.
"""

from __future__ import annotations

import asyncio
import threading

import websockets
import websockets.asyncio.server
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from models import EnrichedRFQ


class _WebSocketSinkPartition(StatelessSinkPartition[EnrichedRFQ]):
    def __init__(self, port: int) -> None:
        self._connections: set[websockets.asyncio.server.ServerConnection] = set()
        self._loop = asyncio.new_event_loop()
        self._port = port
        self._server_ready = threading.Event()
        self._thread = threading.Thread(
            target=self._run_server,
            name="ws-sink-server",
            daemon=True,
        )
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
            websockets.broadcast(self._connections, enriched.model_dump_json())

    def close(self) -> None:
        pass


class _NoOpSinkPartition(StatelessSinkPartition[EnrichedRFQ]):
    def write_batch(self, items: list[EnrichedRFQ]) -> None:
        pass

    def close(self) -> None:
        pass


class WebSocketSink(DynamicSink[EnrichedRFQ]):
    """Bytewax DynamicSink that broadcasts EnrichedRFQs over WebSocket."""

    def __init__(self, port: int) -> None:
        self._port = port

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSinkPartition[EnrichedRFQ]:
        if worker_index == 0:
            return _WebSocketSinkPartition(self._port)
        return _NoOpSinkPartition()
