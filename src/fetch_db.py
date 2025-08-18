"""
fetch_db.py
Loads tabular data from SQLite, CSV, or Parquet.
"""

import pandas as pd
import sqlite3
import logging

def fetch_db_data(db_path="data/env.db", table="air_quality"): 
    """Load data from SQLite database."""
    try:
        con = sqlite3.connect(db_path)
        df = pd.read_sql(f"SELECT * FROM {table}", con)
        con.close()
        return df
    except Exception as e:
        logging.error(f"DB fetch failed: {e}")
        return pd.DataFrame()
