"""Paginated local-CSV adapter that simulates a REST API batch extractor."""

import argparse
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd


def get_page(
    source: pd.DataFrame,
    page: int,
    page_size: int,
    retries: int,
    simulated_failures: int,
) -> pd.DataFrame:
    start = page * page_size
    for attempt in range(retries):
        try:
            if attempt < simulated_failures:
                raise TimeoutError(f"Simulated temporary error on page {page}")
            return source.iloc[start : start + page_size].copy()
        except TimeoutError:
            if attempt == retries - 1:
                raise
            time.sleep(min(2**attempt, 8))
    raise RuntimeError("Page retrieval exhausted retries without a result")


def read_watermark(path: Path) -> pd.Timestamp | None:
    if not path.exists():
        return None
    state = json.loads(path.read_text(encoding="utf-8"))
    value = state.get("watermark")
    return pd.Timestamp(value) if value else None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/orders.csv")
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--checkpoint", default="data/state/watermark.json")
    parser.add_argument("--page-size", type=int, default=10)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--simulate-failures", type=int, default=0)
    parser.add_argument("--lookback-hours", type=int, default=24)
    args = parser.parse_args()
    if args.page_size < 1 or args.retries < 1 or args.simulate_failures < 0:
        parser.error("page-size/retries must be positive and simulated-failures non-negative")

    source_path = Path(args.source)
    output_dir = Path(args.out)
    checkpoint_path = Path(args.checkpoint)
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    source = pd.read_csv(source_path, dtype={"order_id": "string", "customer_id": "string"})
    previous_watermark = read_watermark(checkpoint_path)
    chunks: list[pd.DataFrame] = []
    page = 0
    rows_read = 0
    while True:
        chunk = get_page(
            source,
            page,
            args.page_size,
            args.retries,
            args.simulate_failures,
        )
        if chunk.empty:
            break
        rows_read += len(chunk)
        chunks.append(chunk)
        print(f"page={page} rows={len(chunk)}")
        page += 1

    extracted = pd.concat(chunks, ignore_index=True) if chunks else source.iloc[0:0].copy()
    parsed_ts = pd.to_datetime(extracted["order_ts"], errors="coerce", utc=True)
    if previous_watermark is not None:
        threshold = previous_watermark - pd.Timedelta(hours=args.lookback_hours)
        keep = parsed_ts.isna() | (parsed_ts >= threshold)
        extracted = extracted.loc[keep].copy()

    target = output_dir / "orders.parquet"
    previous_ids: set[str] = set()
    if target.exists():
        existing = pd.read_parquet(target)
        if "order_id" in existing.columns:
            previous_ids = set(existing["order_id"].dropna().astype(str))
        extracted = pd.concat([existing, extracted], ignore_index=True)
    if "order_id" in extracted.columns:
        extracted = extracted.drop_duplicates(subset=["order_id"], keep="last")
        rows_added = sum(
            1
            for order_id in extracted["order_id"].dropna().astype(str)
            if order_id not in previous_ids
        )
    else:
        rows_added = 0
    if "order_ts" in extracted.columns:
        extracted["_parsed_ts"] = pd.to_datetime(
            extracted["order_ts"], errors="coerce", utc=True
        )
        extracted = extracted.sort_values("_parsed_ts", na_position="first")
        maximum_ts = extracted["_parsed_ts"].max()
        extracted = extracted.drop(columns=["_parsed_ts"])
    else:
        maximum_ts = pd.NaT

    temporary = target.with_suffix(".parquet.tmp")
    extracted.to_parquet(temporary, index=False)
    temporary.replace(target)

    if pd.notna(maximum_ts):
        checkpoint_path.write_text(
            json.dumps({"watermark": maximum_ts.isoformat()}, indent=2),
            encoding="utf-8",
        )

    print(
        f"rows_read={rows_read} rows_added={rows_added} rows_in_raw={len(extracted)} "
        f"watermark={maximum_ts if pd.notna(maximum_ts) else 'unchanged'}"
    )
    print(f"raw_parquet={target}")


if __name__ == "__main__":
    main()
