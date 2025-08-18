"""
fetch_db.py
Loads tabular data from SQLite, CSV, or Parquet.
"""

import pandas as pd
import sqlite3
import logging

def fetch_db_data(db_path="data/env.db", table="air_quality", csv_path=None, parquet_path=None):
    """Load data from SQLite, CSV, or Parquet."""
    try:
        if csv_path:
            return pd.read_csv(csv_path)
        if parquet_path:
            return pd.read_parquet(parquet_path)
        con = sqlite3.connect(db_path)
        df = pd.read_sql(f"SELECT * FROM {table}", con)
        con.close()
        return df
    except Exception as e:
        logging.error(f"DB fetch failed: {e}")
        return pd.DataFrame()
