import pytest
import pandas as pd
from src.pipeline import DataPipeline

@pytest.fixture
def pipeline():
    return DataPipeline(":memory:")

def test_happy_path(pipeline):
    data = pd.DataFrame([{
        "transaction_id": "T1", "customer_id": "C1", "product_id": "P1",
        "amount": 100.0, "event_timestamp": "2026-10-28T10:00:00", "status": "COMPLETED"
    }])
    metrics = pipeline.process_batch(data)
    assert metrics["valid_count"] == 1
    assert metrics["quarantine_count"] == 0

def test_malformed_record_quarantine(pipeline):
    data = pd.DataFrame([{
        "transaction_id": "T2", "customer_id": "", "product_id": "P1",
        "amount": 100.0, "event_timestamp": "2026-10-28T10:00:00", "status": "COMPLETED"
    }])
    metrics = pipeline.process_batch(data)
    assert metrics["valid_count"] == 0
    assert metrics["quarantine_count"] == 1

def test_idempotency_rerun(pipeline):
    data = pd.DataFrame([{
        "transaction_id": "T1", "customer_id": "C1", "product_id": "P1",
        "amount": 100.0, "event_timestamp": "2026-10-28T10:00:00", "status": "COMPLETED"
    }])
    pipeline.process_batch(data)
    pipeline.process_batch(data)
    
    cursor = pipeline.conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM daily_customer_metrics")
    count = cursor.fetchone()[0]
    assert count == 1