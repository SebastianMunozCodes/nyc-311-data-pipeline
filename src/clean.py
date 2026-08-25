import pandas as pd
from pathlib import Path
from ingest import load_raw_data


def convert_dates(df):
    columns_to_convert = ["Created Date", "Closed Date", "Resolution Action Updated Date"]

    df[columns_to_convert] = df[columns_to_convert].apply(
        lambda col: pd.to_datetime(
            col,
            format="%m/%d/%Y %I:%M:%S %p"
        )
    )
    return df

def clean_complaint_types(df):
    complaint_col = "Problem (formerly Complaint Type)"

    replacements = {
        "ELEVATOR": "Elevator",
        "PLUMBING": "Plumbing",
    }

    df[complaint_col] = df[complaint_col].replace(replacements)

    return df

def create_derived_columns(df):
    df["request_year"] = df["Created Date"].dt.year
    df["request_month"] = df["Created Date"].dt.month_name()
    df["request_day_of_week"] = df["Created Date"].dt.day_name()
    df["resolution_time"] = df["Closed Date"] - df["Created Date"]

    return df

def save_cleaned_data(df):
    base_dir = Path(__file__).resolve().parent.parent
    processed_dir = base_dir / "data" / "processed"

    csv_path = processed_dir / "nyc_311_cleaned.csv"
    parquet_path = processed_dir / "nyc_311_cleaned.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print(f"Saved cleaned CSV to: {csv_path}")
    print(f"Saved cleaned Parquet to: {parquet_path}")


if __name__ == "__main__":
    df = load_raw_data()

    df = convert_dates(df)
    df = clean_complaint_types(df)
    df = create_derived_columns(df)
    
    save_cleaned_data(df)

    df.info()