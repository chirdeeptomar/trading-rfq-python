"""
Seed the SQLite instruments database.
Run this BEFORE starting docker-compose (Soul needs the DB to exist).

Usage:
    python -m mock.local.seed_db
    python -m mock.local.seed_db --db-path /path/to/instruments.db
"""

import argparse
import sqlite3
from pathlib import Path

from instruments_config import INSTRUMENTS


def seed(db_path: str) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            isin TEXT PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)

    rows = list(INSTRUMENTS.items())
    cur.executemany(
        "INSERT OR REPLACE INTO instruments (isin, name) VALUES (?, ?)",
        rows,
    )

    con.commit()
    con.close()

    print(f"Seeded {len(rows)} instruments into {db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed instruments SQLite database")
    parser.add_argument("--db-path", default="instruments.db", help="Path to SQLite DB file")
    args = parser.parse_args()
    seed(args.db_path)
