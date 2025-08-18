"""
main.py
Orchestrates the pipeline; supports CLI args via argparse.
"""

import argparse
import logging
import json
import pandas as pd
from src.fetch_api import fetch_api_data
from src.fetch_db import fetch_db_data
from src.transform import transform_data

logging.basicConfig(filename='logs/pipeline.log', level=logging.INFO, format='%(message)s')

def main():
    parser = argparse.ArgumentParser(description="EcoAnalytics Data Pipeline")
    parser.add_argument('--offline', action='store_true', help='Use synthetic data')
    args = parser.parse_args()

    api_data = fetch_api_data(offline=args.offline)
    db_data = fetch_db_data()
    final_df = transform_data(api_data, db_data)
    final_df.to_parquet("data/final_enriched.parquet")
    print("✅ Pipeline complete. Output saved to data/final_enriched.parquet")

if __name__ == "__main__":
    main()
