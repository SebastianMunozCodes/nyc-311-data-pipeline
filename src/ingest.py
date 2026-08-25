import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_FILE = BASE_DIR / "data" / "raw" / "nyc_311_raw.csv"

EXPECTED_COLUMNS = [
    "Unique Key",
    "Created Date",
    "Closed Date",
    "Agency",
    "Problem (formerly Complaint Type)",
    "Status",
    "Borough",
]

def load_raw_data():
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Raw data file not found: {RAW_FILE}")

    df = pd.read_csv(
        RAW_FILE,
        dtype={
            "Incident Zip": "string",
            "Taxi Company Borough": "string",
        }
    )

    return df

def validate_columns(df):
    missing_columns = [
        column
        for column in EXPECTED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing expected columns: {missing_columns}"
        )

    return True

if __name__ == "__main__":
    df = load_raw_data()
    validate_columns(df)

    print(df.shape)
    print("Column validation passed.")
