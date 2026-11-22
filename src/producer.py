"""Step 5 (Person 1): send every raw record to Kafka as a JSON message.

Reads data/raw_data.csv, sends one message per row to topic neo.raw,
prints how many messages were really delivered.

Run:  python src/producer.py
"""
import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

RAW_CSV = ROOT / "data" / "raw_data.csv"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "neo.raw")


def read_records(csv_path=RAW_CSV):
    """Read raw CSV as text and turn empty cells into None (JSON null)."""
    df = pd.read_csv(csv_path, dtype=str)
    return [
        {key: (None if pd.isna(value) else value) for key, value in row.items()}
        for row in df.to_dict("records")
    ]


def send_all(csv_path=RAW_CSV):
    """Send all records. Returns the number of messages delivered."""
    records = read_records(csv_path)
    print(f"Records to send: {len(records)}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
            acks="all",
            retries=3,
        )
    except Exception:
        raise SystemExit(
            f"Cannot connect to Kafka at {BOOTSTRAP}. Start it first: docker compose up -d"
        )

    futures = [producer.send(TOPIC, key=rec["record_id"], value=rec) for rec in records]
    producer.flush()

    sent, failed = 0, 0
    for future in futures:
        try:
            future.get(timeout=10)       # waits for the broker confirmation
            sent += 1
        except KafkaError as error:
            failed += 1
            print("  send error:", error)
    producer.close()

    print(f"Sent {sent} messages to topic '{TOPIC}' (failed: {failed})")
    return sent


if __name__ == "__main__":
    send_all()
