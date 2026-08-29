from pathlib import Path

from pyspark.sql import SparkSession

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

def load_raw_data(spark):
    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"Raw data file not found: {RAW_FILE}"
        )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(RAW_FILE))
    )

    df = (
        df
        .withColumn(
            "Incident Zip", 
            df["Incident Zip"].cast("string")
        )
        .withColumn(
            "Taxi Company Borough", 
            df["Taxi Company Borough"].cast("string")
        )
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
    spark = (
        SparkSession.builder
        .appName("NYC311Pipeline")
        .getOrCreate()
    )

    df = load_raw_data(spark)
    validate_columns(df)

    print(df.count())
    print(len(df.columns))
    print("Column validation passed.")
