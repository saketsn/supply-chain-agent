import pandas as pd
import sqlite3
import os

# Paths
CSV_FILE = "data/raw/DataCoSupplyChainDataset.csv"
DB_FILE = "data/processed/supply_chain.db"

def clean_column_names(df):
    """Standardize column names for SQL compatibility."""
    df.columns = [c.lower().replace(' ', '_').replace('(', '').replace(')', '') for c in df.columns]
    return df

def ingest_data():
    if not os.path.exists(CSV_FILE):
        print(f"XX Error: {CSV_FILE} not found. Please download it from Kaggle.")
        return

    print("== Starting ingestion...")
    conn = sqlite3.connect(DB_FILE)
    
    # Process in chunks of 20,000 rows to save RAM
    chunk_iter = pd.read_csv(CSV_FILE, encoding='ISO-8859-1', chunksize=20000)
    
    first_chunk = True
    for chunk in chunk_iter:
        chunk = clean_column_names(chunk)
        
        # Feature Engineering: Calculate Lead Time Variance
        # Actual Days - Scheduled Days
        if 'days_for_shipping_real' in chunk.columns:
            chunk['lead_time_variance'] = chunk['days_for_shipping_real'] - chunk['days_for_shipment_scheduled']
        
        # Write to SQL
        if first_chunk:
            chunk.to_sql('orders', conn, if_exists='replace', index=False)
            first_chunk = False
        else:
            chunk.to_sql('orders', conn, if_exists='append', index=False)
        print(f" Processed a chunk... total rows so far: {pd.read_sql('SELECT COUNT(*) FROM orders', conn).iloc[0,0]}")

    # Create an Index on 'order_region' for lightning-fast queries
    conn.execute("CREATE INDEX idx_region ON orders(order_region)")
    conn.close()
    print(f"== Success! Database created at {DB_FILE}")

if __name__ == "__main__":
    ingest_data()