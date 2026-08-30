from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark_ingest import BASE_DIR, load_raw_data

PROCESSED_FILE = BASE_DIR / "data" / "processed" / "nyc_311_cleaned.parquet"

PLACEHOLDER_COLUMNS = [
    "Problem Detail (formerly Descriptor)",
    "Additional Details",
    "Facility Type",
    "Resolution Description",
    "Park Facility Name",
    "Location Type",
    "Road Ramp",
    "Bridge Highway Segment",
]

spark = (
    SparkSession.builder
    .appName("NYC311Pipeline")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)


def convert_dates(df):
    converted_df = df.withColumns({
        "Created Date": F.to_timestamp(
            F.col("Created Date"),
            "MM/dd/yyyy hh:mm:ss a"
        ),
        "Closed Date": F.to_timestamp(
            F.col("Closed Date"),
            "MM/dd/yyyy hh:mm:ss a"
        ),
        "Resolution Action Updated Date": F.to_timestamp(
            F.col("Resolution Action Updated Date"),
            "MM/dd/yyyy hh:mm:ss a"
        ),
    })

    return converted_df


def normalize_missing_placeholders(df):
    for column_name in PLACEHOLDER_COLUMNS:
        df = df.withColumn(
            column_name,
            F.when(
                F.col(column_name) == "N/A",
                F.lit(None)
            ).otherwise(
                F.col(column_name)
            )
        )

    return df


def handle_invalid_date_ordering(df):
    cleaned_df = df.withColumn(
        "Invalid Date Order",
        F.when(
            F.col("Created Date") > F.col("Closed Date"),
            True
        ).otherwise(False)
    )

    return cleaned_df


def standardize_complaint_types(df):
    cleaned_df = df.withColumn(
        "Problem (formerly Complaint Type)",
        F.when(
            F.col("Problem (formerly Complaint Type)") == "ELEVATOR",
            "Elevator"
        ).when(
            F.col("Problem (formerly Complaint Type)") == "PLUMBING",
            "Plumbing"
        ).otherwise(
            F.col("Problem (formerly Complaint Type)")
        )
    )

    return cleaned_df


def validate_cleaned_data(df):
    print("Validating cleaned data...")

    invalid_date_count = (
        df
        .filter(F.col("Invalid Date Order") == True)
        .count()
    )

    if invalid_date_count != 40:
        raise ValueError(
            f"Expected 40 invalid date order records, found {invalid_date_count}"
        )

    remaining_placeholders = 0

    for column_name in PLACEHOLDER_COLUMNS:
        remaining_placeholders += (
            df
            .filter(F.col(column_name) == "N/A")
            .count()
        )

    if remaining_placeholders != 0:
        raise ValueError(
            f"Found {remaining_placeholders} remaining N/A placeholders"
        )

    remaining_complaint_variants = (
        df
        .filter(
            F.col("Problem (formerly Complaint Type)").isin(
                "ELEVATOR",
                "PLUMBING"
            )
        )
        .count()
    )

    if remaining_complaint_variants != 0:
        raise ValueError(
            f"Found {remaining_complaint_variants} unstandardized complaint types"
        )

    print("Cleaned data validation passed.")


def save_cleaned_data(df):
    df.write.mode("overwrite").parquet(str(PROCESSED_FILE))


if __name__ == "__main__":
    df = load_raw_data(spark)

    df = convert_dates(df)
    df = normalize_missing_placeholders(df)
    df = handle_invalid_date_ordering(df)
    df = standardize_complaint_types(df)

    validate_cleaned_data(df)
    save_cleaned_data(df)

    print(f"Cleaned data saved to: {PROCESSED_FILE}")

