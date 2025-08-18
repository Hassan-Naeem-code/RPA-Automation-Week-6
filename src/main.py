"""
main.py
Orchestrates the pipeline; supports CLI args via argparse.
"""


import argparse
import logging
import json
import pandas as pd
from logging.handlers import RotatingFileHandler
from src.fetch_api import fetch_api_data
from src.fetch_db import fetch_db_data
from src.transform import transform_data

# JSON logging with rotation
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "level": record.levelname,
            "time": self.formatTime(record),
            "message": record.getMessage(),
            "name": record.name,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

handler = RotatingFileHandler('logs/pipeline.log', maxBytes=1000000, backupCount=3)
handler.setFormatter(JsonFormatter())
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def main():
    parser = argparse.ArgumentParser(description="EcoAnalytics Data Pipeline")
    parser.add_argument('--offline', action='store_true', help='Use synthetic data')
    args = parser.parse_args()

    logger.info("Pipeline started", extra={"step": "start"})
    api_data = fetch_api_data(offline=args.offline)
    db_data = fetch_db_data()
    final_df = transform_data(api_data, db_data)
    final_df.to_parquet("data/final_enriched.parquet")
    logger.info("Pipeline complete", extra={"step": "end", "output": "data/final_enriched.parquet"})
    print("✅ Pipeline complete. Output saved to data/final_enriched.parquet")

if __name__ == "__main__":
    main()
