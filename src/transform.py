from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_FILE = BASE_DIR / "data" / "processed" / "nyc_311_cleaned.parquet"
TRANSFORMED_FILE = BASE_DIR / "data" / "transformed" / "nyc_311_transformed.parquet"

COMPLAINTS_SUMMARY_FILE = BASE_DIR / "data" / "transformed" / "complaints_summary.parquet"
BOROUGH_SUMMARY_FILE = BASE_DIR / "data" / "transformed" / "borough_summary.parquet"
AGENCY_SUMMARY_FILE = BASE_DIR / "data" / "transformed" / "agency_summary.parquet"
DATE_SUMMARY_FILE = BASE_DIR / "data" / "transformed" / "date_summary.parquet"

EXPECTED_TRANSFORM_COLUMNS = [
    "request_date",
    "request_year",
    "request_month",
    "request_day_of_week",
    "request_hour",
    "resolution_time_hours"
]

def add_request_date_features(df):
    request_dates = df.withColumns({
        "request_date": F.to_date("Created Date"),
        "request_year": F.year("Created Date"),
        "request_month": F.month("Created Date"),
        "request_day_of_week": F.date_format("Created Date", "EEEE"),
        "request_hour": F.hour("Created Date")
    })

    return request_dates

def add_resolution_time(df):
    resolution_time_hours = df.withColumn(
        "resolution_time_hours",
        F.when(
            (F.col("Status") == "Closed") &
            (F.col("Closed Date").isNotNull()) &
            (F.col("Invalid Date Order") == False),
            F.round((F.unix_timestamp("Closed Date") - F.unix_timestamp("Created Date")) / 3600, 2)
        ).otherwise(
            F.lit(None)
        )
    )

    return resolution_time_hours

def validate_transformed_data(df, expected_row_count):
    missing_columns = [
        column
        for column in EXPECTED_TRANSFORM_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing expected columns: {missing_columns}"
        )

    transformed_row_count = df.count()
    if transformed_row_count != expected_row_count:
        raise ValueError(
            f"Transformed row count {transformed_row_count} "
            f"does not match cleaned row count {expected_row_count}"
        )

    invalid_resolution_time_count = df.filter(
        (F.col("Invalid Date Order") == True) &
        (F.col("resolution_time_hours").isNotNull())
    ).count()

    if invalid_resolution_time_count != 0:
        raise ValueError(
            f"Found {invalid_resolution_time_count} invalid date-order records "
            f"with non-null resolution times"
        )

    unresolved_with_resolution_time_count = df.filter(
        (F.col("Closed Date").isNull()) &
        (F.col("resolution_time_hours").isNotNull())
    ).count()

    if unresolved_with_resolution_time_count != 0:
        raise ValueError(
            f"Found {unresolved_with_resolution_time_count} unresolved records "
            f"with non-null resolution times"
        )

    non_closed_with_resolution_time_count = df.filter(
        (F.col("Status") != "Closed") &
        (F.col("resolution_time_hours").isNotNull())
    ).count()

    if non_closed_with_resolution_time_count != 0:
        raise ValueError(
            f"Found {non_closed_with_resolution_time_count} non-closed records "
            f"with non-null resolution times"
        )

    negative_resolution_time_count = df.filter(
        F.col("resolution_time_hours") < 0
    ).count()

    if negative_resolution_time_count > 0:
        raise ValueError(
            f"Found {negative_resolution_time_count} resolution time records "
            f"with negative resolution times"
        )

    print("Transformed data validation passed.")

def save_transformed_data(df):
    df.write.mode("overwrite").parquet(str(TRANSFORMED_FILE))

def complaints_summary(df):
    complaints_df = (
        df
        .groupBy("Problem (formerly Complaint Type)")
        .agg(
            F.count("*").alias("Request Count"),
            F.round(F.avg("resolution_time_hours"), 2).alias("Average Resolution Time Hours")
        )
        .orderBy("Request Count", ascending=False)
    )

    return complaints_df


def borough_summary(df):
    borough_df = (
        df
        .groupBy("Borough")
        .agg(
            F.count("*").alias("Request Count"),
            F.round(F.avg("resolution_time_hours"), 2).alias("Average Resolution Time Hours")
        )
        .orderBy("Request Count", ascending=False)
    )

    return borough_df

def agency_summary(df):
    agency_df = (
        df
        .groupBy("Agency")
        .agg(
            F.count("*").alias("Request Count"),
            F.round(F.avg("resolution_time_hours"), 2).alias("Average Resolution Time Hours")
        )
        .orderBy("Request Count", ascending=False)
    )

    return agency_df

def date_summary(df):
    date_df = (
        df
        .groupBy("request_date")
        .count()
        .withColumnRenamed("count", "Request Count")
        .orderBy("request_date", ascending=True)
    )

    return date_df


def save_summary(df, output_path):
    df.write.mode("overwrite").parquet(str(output_path))

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("NYC311Pipeline")
        .getOrCreate()
    )

    df = spark.read.parquet(str(PROCESSED_FILE))

    expected_row_count = df.count()

    df = add_request_date_features(df)
    df = add_resolution_time(df)

    validate_transformed_data(df, expected_row_count)
    save_transformed_data(df)

    complaints_df = complaints_summary(df)
    borough_df = borough_summary(df)
    agency_df = agency_summary(df)
    date_df = date_summary(df)

    save_summary(complaints_df, COMPLAINTS_SUMMARY_FILE)
    save_summary(borough_df, BOROUGH_SUMMARY_FILE)
    save_summary(agency_df, AGENCY_SUMMARY_FILE)
    save_summary(date_df, DATE_SUMMARY_FILE)

    print("Transformation pipeline completed successfully.")