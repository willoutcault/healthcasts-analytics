### Setup Virtual Environment
python -m venv .venv                            # create the virtual environment
.venv\Scripts\pip install -r requirements.txt

### Activate Virtual Environment
#### Windows
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
.venv\Scripts\activate
#### Mac
source .venv/bin/activate