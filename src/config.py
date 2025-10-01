from pathlib import Path

# --- Configuration ---
DATA_DIR = Path("data")
FAKE_CSVS_DIR = DATA_DIR / "fake_csvs"
FAKE_JSONS_DIR = DATA_DIR / "fake_jsons"
PARQUETS_DIR = DATA_DIR / "parquets"
SQLITE_DB_PATH = DATA_DIR / "example.db"

# Ensure directories exist
FAKE_CSVS_DIR.mkdir(parents=True, exist_ok=True)
FAKE_JSONS_DIR.mkdir(parents=True, exist_ok=True)
PARQUETS_DIR.mkdir(parents=True, exist_ok=True)
