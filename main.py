import pandas as pd
from data.generator import generate_transactions
from src.pipeline import DataPipeline

def run_main():
    print("=== Step 1: Generating Synthetic Data ===")
    generate_transactions(num_records=1000, seed=42, output_path="data/sample_input.csv")
    
    print("\n=== Step 2: Executing Pipeline ===")
    df = pd.read_csv("data/sample_input.csv")
    pipeline = DataPipeline("analytics.db")
    results = pipeline.process_batch(df)
    
    print("\n=== Step 3: Execution Metrics ===")
    print(f"Run ID: {results['run_id']}")
    print(f"Input Rows: {results['input_count']}")
    print(f"Successfully Processed: {results['valid_count']}")
    print(f"Quarantined Records: {results['quarantine_count']}")
    print(f"Recomputed Dates: {results['affected_dates']}")
    
    print("\n=== Step 4: Sample Daily Customer Metrics ===")
    cursor = pipeline.conn.cursor()
    cursor.execute("""
        SELECT metric_date, customer_id, total_volume, transaction_count, reversal_count 
        FROM daily_customer_metrics LIMIT 5
    """)
    for r in cursor.fetchall():
        print(r)

if __name__ == "__main__":
    run_main()