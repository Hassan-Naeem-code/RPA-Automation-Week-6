"""
fetch_api.py
Retrieves external climate/sustainability data via API or simulates data if offline.
"""

import os
import requests
import logging
import time

def fetch_api_data(offline=False, retries=3, delay=2, latitude=40.7128, longitude=-74.0060):
    """Fetch data from Open-Meteo API or simulate if offline. Retries on failure."""
    if offline:
        # Simulate API data
        return [
            {"station_id": "ST001", "date": "2025-08-01", "aqi": 75, "co2_ppm": 400.2},
            {"station_id": "ST001", "date": "2025-08-02", "aqi": 80, "co2_ppm": 399.8}
        ]
    api_url = os.getenv("API_URL")
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,co2,pm10,pm2_5",
        "start_date": "2025-08-01",
        "end_date": "2025-08-07",
        "timezone": "auto"
    }
    for attempt in range(retries):
        try:
            response = requests.get(api_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            # Parse Open-Meteo response to match expected format
            result = []
            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            pm10 = hourly.get("pm10", [None]*len(times))
            co2 = hourly.get("co2", [None]*len(times))
            for i, t in enumerate(times):
                result.append({
                    "station_id": f"METEO_{latitude}_{longitude}",
                    "date": t.split("T")[0],
                    "aqi": pm10[i] if pm10[i] is not None else 0,
                    "co2_ppm": co2[i] if co2[i] is not None else 0
                })
            return result
        except Exception as e:
            logging.error(f"API fetch attempt {attempt+1} failed: {e}")
            time.sleep(delay)
    logging.error("API fetch failed after retries.")
    return []
