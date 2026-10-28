import sqlite3
import pandas as pd
import json
from datetime import datetime

class DataPipeline:
    def __init__(self, db_path=":memory:"):
        self.conn = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS transactions_raw (
                    transaction_id TEXT PRIMARY KEY,
                    customer_id TEXT,
                    product_id TEXT,
                    amount REAL,
                    event_timestamp TEXT,
                    status TEXT,
                    ingestion_timestamp TEXT
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS quarantine_records (
                    transaction_id TEXT,
                    raw_data TEXT,
                    rejection_reason TEXT,
                    rejected_at TEXT
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_customer_metrics (
                    metric_date TEXT,
                    customer_id TEXT,
                    total_volume REAL,
                    transaction_count INTEGER,
                    reversal_count INTEGER,
                    last_updated_at TEXT,
                    PRIMARY KEY (metric_date, customer_id)
                );
            """)

    def validate_record(self, row):
        reasons = []
        if not row.get("transaction_id") or str(row["transaction_id"]).strip() == "":
            reasons.append("Missing transaction_id")
        if not row.get("customer_id") or str(row["customer_id"]).strip() == "":
            reasons.append("Missing customer_id")
        if pd.isna(row.get("amount")) or row.get("amount") is None:
            reasons.append("Missing amount")
        elif row.get("amount") < 0 and row.get("status") == "COMPLETED":
            reasons.append("Negative amount for COMPLETED status")
        return len(reasons) == 0, "; ".join(reasons)

    def process_batch(self, df_input, run_id=None):
        if run_id is None:
            run_id = f"RUN_{int(datetime.now().timestamp())}"
            
        valid_records = []
        quarantine_records = []
        affected_dates = set()

        for _, row in df_input.iterrows():
            row_dict = row.to_dict()
            is_valid, reason = self.validate_record(row_dict)
            
            if is_valid:
                valid_records.append((
                    row_dict["transaction_id"],
                    row_dict["customer_id"],
                    row_dict["product_id"],
                    float(row_dict["amount"]),
                    row_dict["event_timestamp"],
                    row_dict["status"],
                    datetime.now().isoformat()
                ))
                date_str = str(row_dict["event_timestamp"])[:10]
                affected_dates.add(date_str)
            else:
                quarantine_records.append((
                    str(row_dict.get("transaction_id", "")),
                    json.dumps(row_dict),
                    reason,
                    datetime.now().isoformat()
                ))

        cursor = self.conn.cursor()
        cursor.executemany("""
            INSERT OR REPLACE INTO transactions_raw 
            (transaction_id, customer_id, product_id, amount, event_timestamp, status, ingestion_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, valid_records)

        cursor.executemany("""
            INSERT INTO quarantine_records (transaction_id, raw_data, rejection_reason, rejected_at)
            VALUES (?, ?, ?, ?);
        """, quarantine_records)
        self.conn.commit()

        self.recompute_daily_metrics(affected_dates)

        return {
            "run_id": run_id,
            "input_count": len(df_input),
            "valid_count": len(valid_records),
            "quarantine_count": len(quarantine_records),
            "affected_dates": list(affected_dates)
        }

    def recompute_daily_metrics(self, dates):
        cursor = self.conn.cursor()
        for d in dates:
            cursor.execute("""
                INSERT OR REPLACE INTO daily_customer_metrics
                SELECT 
                    substr(event_timestamp, 1, 10) as metric_date,
                    customer_id,
                    SUM(CASE WHEN status = 'COMPLETED' THEN amount WHEN status = 'REFUNDED' THEN -amount ELSE 0 END) as total_volume,
                    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as transaction_count,
                    COUNT(CASE WHEN status IN ('REFUNDED', 'CANCELLED') THEN 1 END) as reversal_count,
                    ? as last_updated_at
                FROM transactions_raw
                WHERE substr(event_timestamp, 1, 10) = ?
                GROUP BY 1, 2;
            """, (datetime.now().isoformat(), d))
        self.conn.commit()