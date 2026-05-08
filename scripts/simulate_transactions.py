import pandas as pd
import os, time, logging
from datetime import datetime

SOURCE_CSV   = r'data/raw/bank_transactions.csv'
LANDING_ZONE = r'data/landing'   
BATCH_SIZE   = 500               
SLEEP_SECONDS = 5               

def simulate():
    os.makedirs(LANDING_ZONE, exist_ok=True)
   
    df = pd.read_csv(SOURCE_CSV)
    print(f"Loaded {len(df)} rows")
    batch_num = 1
    start_idx = 0

    while start_idx < len(df):
        batch = df.iloc[start_idx : start_idx + BATCH_SIZE]

       
        ts   = datetime.now().strftime('%Y%m%d_%H%M%S')
        path = f'{LANDING_ZONE}/transactions_batch_{batch_num:04d}_{ts}.csv'

        batch.to_csv(path, index=False)
        print(f'Batch {batch_num:04d} sent → {path}')

        start_idx += BATCH_SIZE
        batch_num += 1
        time.sleep(SLEEP_SECONDS)   

simulate()