"""Validate raw batch records and quarantine invalid records with reasons."""

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from contracts import validate_record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw/orders.parquet")
    parser.add_argument("--valid-out", default="data/processed/valid_orders.parquet")
    parser.add_argument("--rejected-out", default="data/rejected/batch_rejected.jsonl")
    args = parser.parse_args()

    frame = pd.read_parquet(args.input)
    valid: list[dict[str, object]] = []
    rejected: list[dict[str, object]] = []
    for row in frame.to_dict(orient="records"):
        payload = {key: _json_safe(value) for key, value in row.items()}
        normalized, errors = validate_record(payload)
        if normalized is not None:
            valid.append(normalized)
        else:
            rejected.append(
                {
                    "order_id": payload.get("order_id"),
                    "reasons": errors,
                    "payload": payload,
                }
            )

    valid_path = Path(args.valid_out)
    rejected_path = Path(args.rejected_out)
    valid_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(valid).to_parquet(valid_path, index=False)
    with rejected_path.open("w", encoding="utf-8") as stream:
        for record in rejected:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"read={len(frame)} valid={len(valid)} rejected={len(rejected)}")
    print(f"valid_parquet={valid_path}")
    print(f"rejected_jsonl={rejected_path}")


def _json_safe(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


if __name__ == "__main__":
    main()

