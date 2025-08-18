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

def test_aqi_delta():
    api_data = [
        {"station_id": "ST001", "date": "2025-08-01", "aqi": 100, "co2_ppm": 400.0},
        {"station_id": "ST001", "date": "2025-08-02", "aqi": 110, "co2_ppm": 399.0}
    ]
    db_data = pd.DataFrame([])
    result = transform_data(api_data, db_data)
    assert "aqi_delta" in result.columns
    assert result[result["date"] == "2025-08-02"]["aqi_delta"].iloc[0] == 10

def test_daily_avg_aqi():
    api_data = [
        {"station_id": "ST001", "date": "2025-08-01", "aqi": 100, "co2_ppm": 400.0},
        {"station_id": "ST002", "date": "2025-08-01", "aqi": 200, "co2_ppm": 410.0}
    ]
    db_data = pd.DataFrame([])
    result = transform_data(api_data, db_data)
    assert "daily_avg_aqi" in result.columns
    assert all(result[result["date"] == "2025-08-01"]["daily_avg_aqi"] == 150)
