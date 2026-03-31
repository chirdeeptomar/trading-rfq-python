"""
RFQ Trading Streaming System

Individual services:
    uv run python -m mock.local.seed_db                 # seed SQLite DB (run once)
    docker compose up -d                                # start NATS + Soul
    uv run python -m mock.upstream.market_data_publisher  # market data feed (100 ISINs, 10ms)
    uv run python -m services.pricing_service           # pricing engine
    uv run python -m mock.upstream.rfq_publisher        # RFQ generator (~1/sec)
    uv run python -m bytewax.run bytewax_pipeline:flow  # Bytewax streaming pipeline
    uv run uvicorn websocket_server:app --port 9000     # WebSocket API

Or run everything at once:
    ./run_all.sh
"""


def main() -> None:
    print(__doc__)


if __name__ == "__main__":
    main()
