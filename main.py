"""Runs the whole pipeline in one order:

API -> raw_data.csv -> Kafka (producer) -> consumer -> validation -> cleaning -> SQLite

Run:  python main.py
Kafka must be running first:  docker compose up -d
"""
from src import api_client, producer, loader

# Person 2's modules. The function names below are the agreed interface:
#   consumer.consume(expected)  -> DataFrame with all received messages
#   validator.validate(df)      -> (valid_df, invalid_df)
#   cleaner.clean(valid_df)     -> clean_df (columns as in src/loader.py COLUMNS)
try:
    from src import consumer, validator, cleaner
except ImportError as error:
    raise SystemExit(f"Person 2 modules are missing: {error}")


def main():
    print("=== 1. COLLECT from NASA API ===")
    raw_df = api_client.collect_and_save()

    print("\n=== 2. PRODUCE to Kafka ===")
    sent = producer.send_all()

    print("\n=== 3. CONSUME from Kafka ===")
    received_df = consumer.consume(expected=sent)

    print("\n=== 4. VALIDATE (before cleaning) ===")
    valid_df, invalid_df = validator.validate(received_df)

    print("\n=== 5. CLEAN ===")
    clean_df = cleaner.clean(valid_df)

    print("\n=== 6. LOAD into SQLite ===")
    result = loader.load(clean_df)

    print("\n=== SUMMARY (all numbers must be explained in the report) ===")
    print(f"collected from API : {len(raw_df)}")
    print(f"sent to Kafka      : {sent}")
    print(f"received (consumer): {len(received_df)}")
    print(f"valid / invalid    : {len(valid_df)} / {len(invalid_df)}")
    print(f"after cleaning     : {len(clean_df)}")
    print(f"saved in database  : {result['total_in_db']}")


if __name__ == "__main__":
    main()
