"""Transform valid orders and measure Pandas/Polars and skew workloads."""

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Callable

import pandas as pd
import polars as pl


def measure(function: Callable[[], object], repeats: int) -> tuple[float, object]:
    durations: list[float] = []
    result: object = None
    for _ in range(repeats):
        started = time.perf_counter()
        result = function()
        durations.append(time.perf_counter() - started)
    return statistics.median(durations), result


def pandas_transform(path: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    frame["order_ts"] = pd.to_datetime(frame["order_ts"], utc=True)
    frame["gross_amount"] = frame["quantity"] * frame["unit_price"]
    frame["net_amount"] = frame["gross_amount"] * (1 - frame["discount_pct"])
    return frame


def polars_transform(path: str) -> pl.DataFrame:
    return (
        pl.read_parquet(path)
        .with_columns(
            pl.col("order_ts").str.to_datetime(time_zone="UTC", strict=False),
            (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
            (
                pl.col("quantity")
                * pl.col("unit_price")
                * (1 - pl.col("discount_pct"))
            ).alias("net_amount"),
        )
    )


def lazy_polars_transform(path: str) -> pl.DataFrame:
    return (
        pl.scan_parquet(path)
        .with_columns(
            (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
            (
                pl.col("quantity")
                * pl.col("unit_price")
                * (1 - pl.col("discount_pct"))
            ).alias("net_amount"),
        )
        .select("order_id", "customer_id", "order_ts", "gross_amount", "net_amount")
        .collect()
    )


def skew_experiment(frame: pl.DataFrame, factor: int) -> dict[str, object]:
    if frame.is_empty():
        return {"rows_original": 0, "rows_skewed": 0, "groups": []}

    dominant_customer = frame.get_column("customer_id")[0]
    dominant_rows = frame.filter(pl.col("customer_id") == dominant_customer)
    skewed = pl.concat(
        [frame] + [dominant_rows] * (factor - 1),
        how="vertical",
    )
    original_counts = frame.group_by("customer_id").len()
    skew_counts = skewed.group_by("customer_id").len()
    original_seconds, _ = measure(
        lambda: frame.group_by("customer_id").agg(pl.col("net_amount").sum()),
        3,
    )
    skew_seconds, _ = measure(
        lambda: skewed.group_by("customer_id").agg(pl.col("net_amount").sum()),
        3,
    )
    return {
        "rows_original": frame.height,
        "rows_skewed": skewed.height,
        "dominant_customer_id": dominant_customer,
        "original_group_sizes": original_counts.to_dicts(),
        "skewed_group_sizes": skew_counts.to_dicts(),
        "group_by_median_seconds": {
            "original": original_seconds,
            "skewed": skew_seconds,
        },
        "note": "Waktu pada dataset kecil adalah indikatif; tidak membuktikan OOM.",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/processed/valid_orders.parquet")
    parser.add_argument(
        "--out", default="data/processed/transformed_orders.parquet"
    )
    parser.add_argument("--benchmark-out", default="reports/benchmark.json")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--skew-factor", type=int, default=100)
    args = parser.parse_args()
    if args.repeats < 1 or args.skew_factor < 1:
        parser.error("repeats and skew-factor must be positive")

    output_path = Path(args.out)
    benchmark_path = Path(args.benchmark_out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    benchmark_path.parent.mkdir(parents=True, exist_ok=True)

    pandas_seconds, pandas_result = measure(
        lambda: pandas_transform(args.input), args.repeats
    )
    polars_seconds, polars_result = measure(
        lambda: polars_transform(args.input), args.repeats
    )
    lazy_seconds, lazy_result = measure(
        lambda: lazy_polars_transform(args.input), args.repeats
    )

    # Persist the Polars eager result; benchmark inputs and transformations match.
    polars_result.write_parquet(output_path)
    skew_result = skew_experiment(polars_result, args.skew_factor)
    result = {
        "input": args.input,
        "row_count": len(pandas_result),
        "repeats": args.repeats,
        "timing_median_seconds": {
            "pandas_eager": pandas_seconds,
            "polars_eager": polars_seconds,
            "polars_lazy": lazy_seconds,
        },
        "skew_experiment": skew_result,
        "caveat": (
            "Pengukuran dipengaruhi ukuran data, cache, perangkat keras, versi "
            "library, dan overhead startup; jangan menggeneralisasi dari dataset kecil."
        ),
    }
    benchmark_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"transformed_rows={len(polars_result)} output={output_path}")
    print(f"benchmark={benchmark_path}")


if __name__ == "__main__":
    main()
