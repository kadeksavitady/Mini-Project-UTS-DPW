"""Publish CSV order records to the Kafka orders topic."""

import argparse
import csv
import json

from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/orders.csv")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--duplicate-first", action="store_true")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("limit must be at least 1")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )
    sent = 0
    try:
        with open(args.source, newline="", encoding="utf-8-sig") as source:
            records = csv.DictReader(source)
            first_record: dict[str, str] | None = None
            for record in records:
                if first_record is None:
                    first_record = record
                producer.send(
                    args.topic,
                    key=record.get("order_id") or "",
                    value=record,
                )
                sent += 1
                if sent >= args.limit:
                    break
            if args.duplicate_first and first_record is not None:
                producer.send(
                    args.topic,
                    key=first_record.get("order_id") or "",
                    value=first_record,
                )
                sent += 1
        producer.flush()
    finally:
        producer.close()

    print(f"published={sent} topic={args.topic}")


if __name__ == "__main__":
    main()

