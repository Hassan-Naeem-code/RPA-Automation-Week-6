"""
fetch_api.py
Retrieves external climate/sustainability data via API or simulates data if offline.
"""

import os
import requests
import logging

def fetch_api_data(offline=False):
    """Fetch data from external API or simulate if offline."""
    if offline:
        # Simulate API data
        return [{"station_id": "ST001", "date": "2025-08-01", "aqi": 75, "co2_ppm": 400.2}]
    try:
        # Example API call (replace with real endpoint)
        response = requests.get(os.getenv("API_URL"), timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"API fetch failed: {e}")
        return []
