### Setup Virtual Environment
python -m venv .venv

.venv\Scripts\pip install -r requirements.txt

### Activate Virtual Environment
#### Windows
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process

.venv\Scripts\activate

#### Mac
source .venv/bin/activate

### Dash UI (local)

```bash
pip install -r dash_app/requirements-dash.txt
copy dash_app\.env.example .env  # then fill values
python dash_app/app.py
```

App runs at http://127.0.0.1:8051