#!/usr/bin/env python3
import json
import os
from pathlib import Path
from supabase import Client, create_client

ENV_FILE_PATH = Path(__file__).with_name(".env.supabase")


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


load_env_file(ENV_FILE_PATH)

SUPABASE_URL = os.getenv("SUPABASE_URL", "YOUR_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "YOUR_SERVICE_ROLE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "seller_risk_scores")

if SUPABASE_URL == "YOUR_URL" or SUPABASE_SERVICE_ROLE_KEY == "YOUR_SERVICE_ROLE_KEY":
    raise ValueError(
        "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env.supabase "
        "or environment variables before running."
    )

# Load sample_data from the JSON file you provided.
INPUT_JSON_PATH = Path(__file__).with_name("v1_output")
with INPUT_JSON_PATH.open("r", encoding="utf-8") as f:
    sample_data = json.load(f)

if not isinstance(sample_data, list):
    raise ValueError("v1_output must contain a JSON array of records.")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Insert into Supabase
try:
    response = supabase.table(SUPABASE_TABLE).insert(sample_data).execute()
    print(f"Inserted {len(sample_data)} records into {SUPABASE_TABLE}.")
    if getattr(response, "data", None) is not None:
        print(response.data)
except Exception as e:
    print(f"Error inserting data: {e}")
