"""Consume Kafka orders, validate them, and write idempotent JSONL sinks."""

import argparse
import json
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition

from contracts import validate_record


def read_seen_order_ids(path: Path) -> set[str]:
    seen: set[str] = set()
    if not path.exists():
        return seen
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            record = json.loads(line)
            order_id = record.get("order_id")
            if order_id:
                seen.add(str(order_id))
    return seen


def read_seen_rejections(path: Path) -> set[tuple[str, int, int]]:
    seen: set[tuple[str, int, int]] = set()
    if not path.exists():
        return seen
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            record = json.loads(line)
            source = record.get("source", {})
            if {"topic", "partition", "offset"} <= source.keys():
                seen.add(
                    (
                        str(source["topic"]),
                        int(source["partition"]),
                        int(source["offset"]),
                    )
                )
    return seen


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        stream.flush()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--group-id", default="orders-consumer-demo")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--max-messages", type=int, default=20)
    parser.add_argument("--idle-polls", type=int, default=5)
    parser.add_argument("--valid-out", default="data/stream/valid_orders.jsonl")
    parser.add_argument("--rejected-out", default="data/stream/rejected_orders.jsonl")
    args = parser.parse_args()
    if args.max_messages < 1 or args.idle_polls < 1:
        parser.error("max-messages and idle-polls must be positive")

    valid_path = Path(args.valid_out)
    rejected_path = Path(args.rejected_out)
    seen_valid_ids = read_seen_order_ids(valid_path)
    seen_rejections = read_seen_rejections(rejected_path)
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        group_id=args.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
    )

    consumed = 0
    idle = 0
    try:
        while consumed < args.max_messages and idle < args.idle_polls:
            batches = consumer.poll(timeout_ms=1000, max_records=args.max_messages - consumed)
            messages = [message for records in batches.values() for message in records]
            if not messages:
                idle += 1
                continue
            idle = 0
            for message in messages:
                payload = message.value
                if not isinstance(payload, dict):
                    payload = {"_invalid_payload": repr(payload)}
                normalized, errors = validate_record(payload)
                if normalized is not None:
                    order_id = str(normalized["order_id"])
                    if order_id not in seen_valid_ids:
                        append_jsonl(valid_path, normalized)
                        seen_valid_ids.add(order_id)
                else:
                    source = {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                    }
                    source_key = (
                        message.topic,
                        message.partition,
                        message.offset,
                    )
                    if source_key not in seen_rejections:
                        append_jsonl(
                            rejected_path,
                            {
                                "order_id": payload.get("order_id"),
                                "reasons": errors,
                                "payload": payload,
                                "source": source,
                            },
                        )
                        seen_rejections.add(source_key)
                consumer.commit(
                    offsets={
                        TopicPartition(message.topic, message.partition):
                            OffsetAndMetadata(message.offset + 1, "")
                    }
                )
                consumed += 1
                print(
                    f"topic={message.topic} partition={message.partition} "
                    f"offset={message.offset} valid={normalized is not None}"
                )
                if consumed >= args.max_messages:
                    break
    except KafkaError:
        raise
    finally:
        consumer.close()

    print(f"consumed={consumed} group_id={args.group_id}")


if __name__ == "__main__":
    main()
