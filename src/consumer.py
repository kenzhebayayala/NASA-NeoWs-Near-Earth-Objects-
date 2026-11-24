"""Step 2 (Person 2): read all messages from Kafka topic neo.raw.

Deserializes JSON, stops when EXPECTED messages are read or on timeout,
prints how many were consumed, saves them to data/consumed.csv.

Run:  python src/consumer.py
"""
import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "neo.raw")
EXPECTED = int(os.getenv("EXPECTED_MESSAGES", "203"))
TIMEOUT_MS = 10000          # 10 sec without new messages -> stop
OUT_CSV = ROOT / "data" / "consumed.csv"


def consume_all():
    """Read messages until EXPECTED reached or timeout. Returns list of dicts."""
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="earliest",     # read from the beginning
            enable_auto_commit=False,         # do not save offsets -> rerun gives same result
            consumer_timeout_ms=TIMEOUT_MS,
        )
    except KafkaError:
        raise SystemExit(
            f"Cannot connect to Kafka at {BOOTSTRAP}. Start it first: docker compose up -d"
        )

    records, bad_json = [], 0
    try:
        for msg in consumer:
            try:
                records.append(json.loads(msg.value.decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                bad_json += 1
                print(f"  bad message at offset {msg.offset}: {error}")
            if len(records) + bad_json >= EXPECTED:
                break
    except KafkaError as error:
        print("Kafka error while reading:", error)
    finally:
        consumer.close()

    print(f"Consumed: {len(records)} (bad JSON: {bad_json}, expected: {EXPECTED})")
    return records


if __name__ == "__main__":
    data = consume_all()
    OUT_CSV.parent.mkdir(exist_ok=True)
    pd.DataFrame(data).to_csv(OUT_CSV, index=False)
    print(f"Saved to {OUT_CSV}")