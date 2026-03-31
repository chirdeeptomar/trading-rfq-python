"""
RFQ Trading Streaming System — Pipeline Entry Point

Runs the Bytewax pipeline directly. This is the canonical way to start
the pipeline:

    uv run python main.py

Individual services:
    uv run python -m mock.local.seed_db
    docker compose up -d
    uv run python -m mock.upstream.market_data_publisher
    uv run python -m services.pricing_service
    uv run python -m mock.upstream.rfq_publisher
    uv run python main.py
    uv run uvicorn websocket_server:app --port 9000

Or run everything at once:
    ./run_all.sh
"""

from bytewax.run import cli_main

from pipeline.pipeline import flow

if __name__ == "__main__":
    cli_main(flow)
