# EcoAnalytics Automation Pipeline

## Overview
Automates environmental data retrieval, transformation, and storage for analysis.

## Setup
1. Clone the repo
2. Install dependencies: `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and set your API endpoint
4. (Optional) Generate synthetic data: `python scripts/generate_fake_data.py`

## Usage
Run the pipeline:
```bash
python src/main.py --offline  # Uses synthetic data
python src/main.py            # Uses live API
```

## Outputs
- Final dataset: `data/final_enriched.parquet`
- Logs: `logs/pipeline.log`

## Testing
Run unit tests:
```bash
pytest tests/
```
