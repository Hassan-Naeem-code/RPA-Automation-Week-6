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
        if not df_api.empty and db_data.empty:
            df = df_api.copy()
        elif df_api.empty and not db_data.empty:
            df = db_data.copy()
        else:
            df = pd.merge(df_api, db_data, on=["station_id", "date"], how="outer", suffixes=("_api", "_db"))
            # If both sources have 'aqi', combine into one
            if "aqi_api" in df.columns and "aqi_db" in df.columns:
                df["aqi"] = df["aqi_api"].combine_first(df["aqi_db"])
            elif "aqi_api" in df.columns:
                df["aqi"] = df["aqi_api"]
            elif "aqi_db" in df.columns:
                df["aqi"] = df["aqi_db"]
            # Same for co2_ppm
            if "co2_ppm_api" in df.columns and "co2_ppm_db" in df.columns:
                df["co2_ppm"] = df["co2_ppm_api"].combine_first(df["co2_ppm_db"])
            elif "co2_ppm_api" in df.columns:
                df["co2_ppm"] = df["co2_ppm_api"]
            elif "co2_ppm_db" in df.columns:
                df["co2_ppm"] = df["co2_ppm_db"]
        # Ensure required columns exist
        for col in ["station_id", "date", "aqi", "co2_ppm"]:
            if col not in df.columns:
                df[col] = pd.Series(dtype=object)
        # Only drop rows where both AQI and CO2 are missing, not all NA
        if "aqi" in df.columns and "co2_ppm" in df.columns:
            df = df.dropna(subset=["aqi", "co2_ppm"], how="all")
        # Ensure required columns exist
        for col in ["aqi", "co2_ppm"]:
            if col not in df.columns:
                df[col] = pd.Series(dtype=float)
        # KPI: average AQI
        if not df["aqi"].empty:
            avg_aqi = df["aqi"].mean()
        else:
            avg_aqi = float('nan')
        df["avg_aqi"] = avg_aqi
        # KPI: AQI delta (difference from previous day per station)
        if not df["aqi"].empty:
            df = df.sort_values(["station_id", "date"])
            df["aqi_delta"] = df.groupby("station_id")["aqi"].diff()
        else:
            df["aqi_delta"] = pd.Series(dtype=float)
        # KPI: CO2 reduction (difference from previous day per station)
        if not df["co2_ppm"].empty:
            df["co2_delta"] = df.groupby("station_id")["co2_ppm"].diff()
        else:
            df["co2_delta"] = pd.Series(dtype=float)
        # KPI: daily average AQI
        if not df["aqi"].empty and "date" in df.columns:
            daily_avg = df.groupby("date")["aqi"].mean().rename("daily_avg_aqi")
            df = df.merge(daily_avg, on="date", how="left")
        else:
            df["daily_avg_aqi"] = pd.Series(dtype=float)
        return df
    except Exception as e:
        logging.error(f"Transform failed: {e}")
        return pd.DataFrame({"avg_aqi": [], "aqi_delta": [], "co2_delta": [], "daily_avg_aqi": []})
