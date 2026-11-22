# Near-Earth Asteroids: Streaming & Validation Pipeline

Course: Data Collection & Preparation (SIS2)  
Team: Kenzhebay Ayala, Akniyet Barakhan  
Stack: NASA NeoWs API, Apache Kafka, Pandera, pandas, SQLite3

Pipeline: **NASA API → raw_data.csv → Kafka producer → Kafka consumer → Pandera validation → cleaning → SQLite**

## Project structure
```
src/        api_client.py, producer.py, consumer.py, validator.py, cleaner.py, loader.py, analysis.py
data/       raw_data.csv, asteroids.db (the .db file is not committed)
figures/    EDA plots
docs/       data_dictionary.md
report/     final report
tests/      test_idempotency.py
main.py     runs everything in one order
```

## 1. Install
```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # then put your NASA key into .env
```
Get a free key at https://api.nasa.gov (the key is never committed: `.env` is in `.gitignore`).

## 2. Data collection (NASA NeoWs)
- Endpoint: `https://api.nasa.gov/neo/rest/v1/feed`
- Parameters: `start_date`, `end_date` (YYYY-MM-DD, **7-day windows**, going back from today), `api_key`
- The script requests new windows until at least **200 records** are collected.
- Errors handled: timeout, connection error, `429` (waits and retries), `4xx` (stops with a message), `5xx` (retries).
- Nested JSON is flattened to **one row per asteroid close approach**. `record_id = neo_id + "_" + close_approach_date` is unique.
- Raw data is saved **before cleaning** to `data/raw_data.csv`.

```bash
python src/api_client.py
```

## 3. Kafka (Docker)
```bash
docker compose up -d
docker exec neo-kafka /opt/kafka/bin/kafka-topics.sh --create --topic neo.raw --bootstrap-server localhost:9092
docker exec neo-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```
Stop: `docker compose down`

Before a **second run** delete the old messages, otherwise the consumer will also read the previous run:
```bash
docker exec neo-kafka /opt/kafka/bin/kafka-topics.sh --delete --topic neo.raw --bootstrap-server localhost:9092
docker exec neo-kafka /opt/kafka/bin/kafka-topics.sh --create --topic neo.raw --bootstrap-server localhost:9092
```

## 4. Producer
```bash
python src/producer.py
```
Each row of `raw_data.csv` is sent as one JSON message (key = `record_id`). The script prints how many messages were delivered (≥ 200).

## 5. Loader (SQLite) and idempotency
```bash
python tests/test_idempotency.py
```
The table `asteroids` has a `PRIMARY KEY (record_id)` and the loader uses `INSERT OR IGNORE`, so loading the same data twice does not create duplicates.

## 6. Run everything
```bash
docker compose up -d
python main.py
```
Expected result: a summary with the counts (collected, sent, received, valid/invalid, cleaned, saved). Collected = sent = received; invalid records and duplicates explain the later decrease.
