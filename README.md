# Incremental Data Engineering Pipeline

## Architecture & Overview
This data pipeline ingests incremental transaction streams, performs robust validation, routes invalid or malformed records to a quarantine table, ensures idempotent execution via primary key deduplication (`INSERT OR REPLACE`), and statefully recomputes daily customer aggregates.

## Tech Stack
* **Python** (Core logic & scripting)
* **Pandas** (Data manipulation)
* **SQLite** (Lightweight, serverless embedded database)
* **Pytest** (Automated unit testing framework)

## Project Structure
```text
yottaflex-pipeline/
├── data/
│   └── generator.py
├── src/
│   ├── __init__.py
│   └── pipeline.py
├── tests/
│   └── test_pipeline.py
├── main.py
├── requirements.txt
└── README.md