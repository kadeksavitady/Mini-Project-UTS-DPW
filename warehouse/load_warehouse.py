"""Idempotently upsert validated batch and streaming orders into DuckDB."""

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


COLUMNS = [
    "order_id",
    "customer_id",
    "order_ts",
    "product_category",
    "quantity",
    "unit_price",
    "discount_pct",
    "payment_method",
    "status",
]


def load_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=COLUMNS)
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                records.append(json.loads(line))
    return pd.DataFrame(records, columns=COLUMNS)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", default="data/processed/valid_orders.parquet")
    parser.add_argument("--stream", default="data/stream/valid_orders.jsonl")
    parser.add_argument("--db", default="warehouse/orders.duckdb")
    args = parser.parse_args()

    batch_path = Path(args.batch)
    if not batch_path.exists():
        parser.error(f"Validated batch file does not exist: {batch_path}")
    batch = pd.read_parquet(batch_path)
    stream = load_jsonl(Path(args.stream))
    incoming = pd.concat([batch, stream], ignore_index=True)
    if incoming.empty:
        parser.error("No validated batch or streaming records were found")
    incoming = incoming[COLUMNS].drop_duplicates(subset=["order_id"], keep="last")
    incoming["order_ts"] = pd.to_datetime(incoming["order_ts"], utc=True)
    incoming["order_ts"] = incoming["order_ts"].dt.tz_localize(None)

    database_path = Path(args.db)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_orders (
                order_id VARCHAR PRIMARY KEY,
                customer_id VARCHAR NOT NULL,
                order_ts TIMESTAMP NOT NULL,
                product_category VARCHAR NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price DOUBLE NOT NULL,
                discount_pct DOUBLE NOT NULL,
                payment_method VARCHAR NOT NULL,
                status VARCHAR NOT NULL
            )
            """
        )
        connection.register("incoming_orders_df", incoming)
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                "CREATE OR REPLACE TEMP TABLE incoming_orders AS "
                "SELECT * FROM incoming_orders_df"
            )
            connection.execute(
                """
                DELETE FROM raw_orders
                WHERE order_id IN (SELECT order_id FROM incoming_orders)
                """
            )
            connection.execute(
                """
                INSERT INTO raw_orders
                SELECT order_id, customer_id, order_ts, product_category,
                       quantity, unit_price, discount_pct, payment_method, status
                FROM incoming_orders
                """
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.unregister("incoming_orders_df")

        total = connection.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
        print(f"incoming_unique={len(incoming)} warehouse_total={total}")
        print(f"database={database_path}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()

