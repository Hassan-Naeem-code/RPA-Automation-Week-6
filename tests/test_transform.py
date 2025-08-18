"""
test_transform.py
Unit test for KPI calculation in transform.py
"""

import pandas as pd
from src.transform import transform_data

def test_avg_aqi():
    api_data = [{"station_id": "ST001", "date": "2025-08-01", "aqi": 100, "co2_ppm": 400.0}]
    db_data = pd.DataFrame([{"station_id": "ST001", "date": "2025-08-01", "aqi": 100, "co2_ppm": 400.0}])
    result = transform_data(api_data, db_data)
    assert "avg_aqi" in result.columns
    assert result["avg_aqi"].iloc[0] == 100
