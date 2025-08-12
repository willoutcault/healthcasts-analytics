# Analytics Utils

A shared Python package for pulling metrics from PostgreSQL and MySQL.

## Installation
```bash
git clone https://github.com/YOUR-ORG/analytics-utils.git
cd analytics-utils
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Setup
1. Copy `examples/config_sample.py` → `analytics_utils/config.py`
2. Fill in your real PostgreSQL and MySQL credentials.

## Usage
```python
from analytics_utils import db_utils as db

df = db.run_email_engagement_query([1234])
print(df.head())
```
