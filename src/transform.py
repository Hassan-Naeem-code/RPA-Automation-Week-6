"""
transform.py
Joins, filters, aggregates, and calculates KPIs.
"""

import pandas as pd
import logging

def transform_data(api_data, db_data):
    """Merge API and DB data, clean, and calculate KPIs."""
    try:
        df_api = pd.DataFrame(api_data)
        df = pd.merge(df_api, db_data, on=["station_id", "date"], how="outer")
        df = df.dropna()
        # Example KPI: average AQI
        avg_aqi = df["aqi"].mean()
        df["avg_aqi"] = avg_aqi
        return df
    except Exception as e:
        logging.error(f"Transform failed: {e}")
        return pd.DataFrame()
