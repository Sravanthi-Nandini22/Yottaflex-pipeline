import os
import random
import pandas as pd
from datetime import datetime, timedelta

def generate_transactions(num_records=1000, seed=42, output_path="data/sample_input.csv"):
    random.seed(seed)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    base_time = datetime(2026, 10, 20, 10, 0, 0)
    customers = [f"CUST_{i:03d}" for i in range(1, 20)]
    products = [f"PROD_{i:03d}" for i in range(1, 10)]
    
    data = []
    
    for i in range(num_records):
        tx_id = f"TXN_{i+1:06d}"
        cust_id = random.choice(customers)
        prod_id = random.choice(products)
        
        day_offset = random.choices([0, 1, 2, 3], weights=[0.7, 0.15, 0.1, 0.05])[0]
        event_time = base_time + timedelta(days=day_offset, minutes=random.randint(0, 1440))
        
        amount = round(random.uniform(10.0, 500.0), 2)
        status = random.choices(["COMPLETED", "REFUNDED", "CANCELLED"], weights=[0.85, 0.10, 0.05])[0]
        
        # Intentional edge cases
        if i % 150 == 0:
            cust_id = ""
        elif i % 200 == 0:
            amount = -50.0
            
        data.append({
            "transaction_id": tx_id,
            "customer_id": cust_id,
            "product_id": prod_id,
            "amount": amount,
            "event_timestamp": event_time.isoformat(),
            "status": status
        })
        
        if i % 100 == 0:
            data.append(data[-1].copy())
            
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"Generated {len(df)} records at {output_path}")

if __name__ == "__main__":
    generate_transactions(num_records=1000)