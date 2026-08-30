from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

from spark_ingest import load_raw_data

spark = (
    SparkSession.builder
    .appName("NYC311Pipeline")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

df = load_raw_data(spark)

df.cache()
df.count()

date_range = (
    df
    .select(
        F.to_timestamp(
            "Created Date",
            "MM/dd/yyyy hh:mm:ss a"
        ).alias("created_date_timestamp")
    )
    .agg(
        F.min("created_date_timestamp").alias("earliest_date"),
        F.max("created_date_timestamp").alias("latest_date")
    )
    .first()
)

earliest_date = date_range["earliest_date"]
latest_date = date_range["latest_date"]

CORE_FIELDS = [
    "Unique Key",
    "Created Date",
    "Agency",
    "Problem (formerly Complaint Type)",
    "Status",
    "Borough",
]

STATUS_CHECK_COLUMNS = [
    "Unique Key",
    "Created Date",
    "Closed Date",
    "Agency",
    "Problem (formerly Complaint Type)",
    "Status",
    "Borough",
]   
    
def inspect_structure(df):
    print(f"Row count: {df.count()}")
    print(f"Column count: {len(df.columns)}")
    print()

    print("Schema:")
    df.printSchema()
    print()

    print(f"Earliest created date: {earliest_date}")
    print(f"Latest created date: {latest_date}")
    print()

def inspect_missing_values(df):
    total_rows = df.count()

    null_expressions = []

    for field in df.schema.fields:
        column_name = field.name

        if isinstance(field.dataType, StringType):
            missing_condition = (
                F.col(column_name).isNull() |
                (F.trim(F.col(column_name)) == "")
            )
        else:
            missing_condition = F.col(column_name).isNull()

        null_expressions.append(
            F.sum(
                missing_condition.cast("int")
            ).alias(column_name)
        )

    null_counts_row = df.select(
        *null_expressions
    ).first()

    null_summary = []

    for column_name in df.columns:
        null_count = null_counts_row[column_name]
        null_percentage = round(
            (null_count / total_rows) * 100, 2
        )

        null_summary.append(
            (column_name, null_count, null_percentage)
        )

    null_counts_df = df.sparkSession.createDataFrame(
        null_summary,
        ["Column", "Null Count", "Null Percentage"]
    )

    print("Null count and null percentage for every column:")
    null_counts_df.show(len(df.columns), truncate=False)
    print()

    core_fields_df = null_counts_df.filter(
        F.col("Column").isin(CORE_FIELDS)
    )

    print("Core Field Completeness:")
    core_fields_df.show(len(CORE_FIELDS), truncate=False)
    print()

def inspect_missing_placeholders(df):
    placeholder_values = [
        "N/A",
        "NA",
        "NULL",
        "None",
        "NaN",
    ]

    columns_to_check = [
        "Problem Detail (formerly Descriptor)",
        "Additional Details",
        "Facility Type",
        "Resolution Description",
        "Park Facility Name",
    ]

    for column_name in columns_to_check:
        placeholder_counts = (
            df
            .filter(F.col(column_name).isin(placeholder_values))
            .groupBy(column_name)
            .count()
            .orderBy(F.col("count").desc())
        )

        total_placeholder_count = placeholder_counts.agg(
            F.sum("count").alias("total")
        ).collect()[0]["total"]

        if total_placeholder_count is None:
            total_placeholder_count = 0

        print(f"Placeholder values in {column_name}: {total_placeholder_count}")

        if total_placeholder_count > 0:
            placeholder_counts.show(truncate=False)

        print()

def inspect_duplicates(df):
    duplicate_counts = df.groupBy(*df.columns).count()
    full_duplicates = (
        duplicate_counts
        .filter(F.col("count") > 1)
        .withColumn("duplicate_rows", F.col("count") - 1)
    )

    total_duplicate_rows = (
        full_duplicates
        .agg(F.sum("duplicate_rows").alias("total_duplicates"))
        .collect()[0]["total_duplicates"]
    )

    if total_duplicate_rows is None:
        total_duplicate_rows = 0

    print(f"Full-row duplicates: {total_duplicate_rows}")

    duplicate_uk_counts = df.groupBy("Unique Key").count()
    uk_duplicates = (
        duplicate_uk_counts
        .filter(F.col("count") > 1)
        .withColumn("duplicate_keys", F.col("count") - 1)
    )

    total_duplicate_keys = (
        uk_duplicates
        .agg(F.sum("duplicate_keys").alias("total_duplicate_keys"))
        .collect()[0]["total_duplicate_keys"]
    )

    if total_duplicate_keys is None:
        total_duplicate_keys = 0

    print(f"Duplicate Unique Keys: {total_duplicate_keys}")
    print()

def inspect_status_quality(df):
    status_counts = (
        df.groupBy("Status")
        .count()
        .withColumnRenamed("count", "Count")
        .orderBy("Count", ascending=False)
    )

    print("Status Counts:")
    status_counts.show(truncate=False)

    missing_closed_by_status = (
        df
        .filter(F.col("Closed Date").isNull())
        .groupBy("Status")
        .count()
        .withColumnRenamed("count", "Missing Closed Date Count")
        .orderBy("Missing Closed Date Count", ascending=False)
    )

    print("Missing Closed Date by status:")
    missing_closed_by_status.show(truncate=False)

    closed_missing_closed = (
        df
        .filter(
            (F.col("Status") == "Closed") &
            (F.col("Closed Date").isNull())
        )
        .select(*STATUS_CHECK_COLUMNS)
    )

    print("Closed + missing Closed Date:")
    closed_missing_closed.show(vertical=True, truncate=False)

    non_closed_with_closed_date = (
        df
        .filter(
            (F.col("Status") != "Closed") &
            (F.col("Closed Date").isNotNull())
        )
        .select(*STATUS_CHECK_COLUMNS)
    )

    print(
        f"Non-Closed + Closed Date count: "
        f"{non_closed_with_closed_date.count()}"
    )

    print("Non-Closed + Closed Date sample:")
    non_closed_with_closed_date.show(truncate=False)

    print("Non-Closed + Closed Date by Status:")
    non_closed_with_closed_date.groupBy("Status").count().withColumnRenamed(
        "count", "Count"
    ).show()

    temp_df = (
        df
        .withColumn(
            "created_date_timestamp",
            F.to_timestamp(
                "Created Date",
                "MM/dd/yyyy hh:mm:ss a"
            )
        )
        .withColumn(
            "closed_date_timestamp",
            F.to_timestamp(
                "Closed Date",
                "MM/dd/yyyy hh:mm:ss a"
            )
        )
    )

    invalid_date_records = (
        temp_df
        .filter(
            F.col("created_date_timestamp") > F.col("closed_date_timestamp")
        )
    )

    invalid_date_summary = (
        invalid_date_records
        .select(*STATUS_CHECK_COLUMNS)
    )

    print("Cases where Created Date > Closed Date:")
    invalid_date_summary.show(
        invalid_date_summary.count(),
        truncate=False
    )

    print("Created Date > Closed Date by Status:")
    invalid_date_summary.groupBy("Status").count().withColumnRenamed(
        "count", "Count"
    ).show()

    matching_resolution_dates = (
        invalid_date_records
        .filter(
            F.col("Closed Date") == F.col("Resolution Action Updated Date")
        )
    )

    matching_resolution_dates_count = (matching_resolution_dates.count())
    print(f"Created Date > Closed Date records where Closed Date == Resolution Action Updated Date: {matching_resolution_dates_count}")

    invalid_date_details = (
        invalid_date_records
        .select(
            "Unique Key",
            "Created Date",
            "Closed Date",
            "Resolution Action Updated Date",
            "Agency",
            "Problem (formerly Complaint Type)",
            "Status",
            "Borough"
        )
    )

    print("Additional details for Created Date > Closed Date records:")
    invalid_date_details.show(invalid_date_details.count(), truncate=False)

def inspect_geography(df):
    borough_counts = (
        df.groupBy("Borough")
        .count()
        .withColumnRenamed("count", "Count")
        .orderBy("Count", ascending=False)
    )

    print("Borough Counts:")
    borough_counts.show()

    missing_zip_count = df.filter(
        F.col("Incident Zip").isNull()
    )

    print(f"Total missing Incident Zip: {missing_zip_count.count()}")

    missing_zip_by_borough = (
        df
        .filter(F.col("Incident Zip").isNull())
        .groupBy("Borough")
        .count()
        .withColumnRenamed("count", "Missing Incident Zip")
        .orderBy("Missing Incident Zip", ascending=False)
    )

    print("Missing Incident Zip by Borough:")
    missing_zip_by_borough.show(truncate=False)

    unspecified_borough = (
        df.filter(
            F.col("Borough") == "Unspecified"
        )
        .select(
            "Incident Zip", 
            "City", 
            "Incident Address", 
            "Latitude", 
            "Longitude", 
            "Location"
        )
    )

    print("Location Data for Unspecified Borough Records:")
    unspecified_borough.show(truncate=False)
    print()

    missing_coordinates_by_borough = (
        df
        .filter(
            (F.col("Latitude").isNull()) &
            (F.col("Longitude").isNull())
        )
        .groupBy("Borough")
        .count()
        .withColumnRenamed("count", "Count")
        .orderBy("Count", ascending=False)
    )
    print("Missing coordinates by borough:")
    missing_coordinates_by_borough.show(truncate=False)

    missing_coordinates = (
        df
        .filter(
            (F.col("Latitude").isNull()) &
            (F.col("Longitude").isNull())
        )
        .select(
            "Unique Key",
            "Borough",
            "Incident Zip",
            "City",
            "Incident Address",
            "Latitude",
            "Longitude",
            "Location"
        )
    ) 

    print("Missing coordinate records:")
    missing_coordinates.show(truncate=False)
    print()

    coordinate_mismatches = (
        df
        .filter(
            (F.col("Latitude").isNotNull() & F.col("Longitude").isNull()) |
            (F.col("Latitude").isNull() & F.col("Longitude").isNotNull())
        )
        .select(
            "Unique Key", 
            "Borough", 
            "Incident Zip", 
            "Latitude", 
            "Longitude", 
            "Location"
        )
    )

    coordinate_mismatch_count = coordinate_mismatches.count()

    print(f"Coordinate mismatch count: {coordinate_mismatch_count}")

    if coordinate_mismatch_count > 0:
        coordinate_mismatches.show(truncate=False)

def inspect_complaints(df):
    distinct_complaint_count = (
        df
        .select(
            F.count_distinct("Problem (formerly Complaint Type)").alias("distinct_complaint_count")
        )
        .collect()[0]["distinct_complaint_count"]
    )

    print(f"Distinct complaint counts: {distinct_complaint_count}")
    print()

    top_complaint_types = (
        df
        .groupBy("Problem (formerly Complaint Type)")
        .count()
        .withColumnRenamed("count", "Number of Complaints")
        .orderBy("Number of Complaints", ascending=False)
    )

    print(f"Complaint Types Ranked by Number of Requests from {earliest_date} to {latest_date}:")
    top_complaint_types.show(top_complaint_types.count(), truncate=False)

    null_complaints = (
        df
        .filter(
            (F.col("Problem (formerly Complaint Type)").isNull()) |
            (F.trim(F.col("Problem (formerly Complaint Type)")) == "")
        )
        .select(*CORE_FIELDS)
    )

    null_complaints_count = null_complaints.count()

    print(f"Null or empty string complaints count: {null_complaints_count}")

    if null_complaints_count > 0:
        null_complaints.show(truncate=False)


def inspect_agencies(df):
    distinct_agency_count = (
        df
        .select(
            F.count_distinct("Agency").alias("distinct_agency_count")
        )
        .collect()[0]["distinct_agency_count"]
    )

    print(f"Distinct agency count: {distinct_agency_count}")
    print()

    agency_counts = (
        df
        .groupBy("Agency")
        .count()
        .withColumnRenamed("count", "Number of Requests")
        .orderBy(F.col("Number of Requests").desc())
    )

    print("Agencies Ranked by Number of Requests:")
    agency_counts.show(truncate=False)
    print()

    missing_agencies = (
        df
        .filter(
            (F.col("Agency").isNull()) |
            (F.trim(F.col("Agency")) == "")
        )
        .select(*CORE_FIELDS)
    )

    missing_agency_count = missing_agencies.count()

    print(f"Null or empty agency count: {missing_agency_count}")

    if missing_agency_count > 0:
        missing_agencies.show(truncate=False)

    print()

    agency_name_consistency = (
        df
        .groupBy("Agency")
        .agg(
            F.count_distinct("Agency Name").alias("distinct_agency_names"),
            F.collect_set("Agency Name").alias("agency_names")
        )
        .filter(F.col("distinct_agency_names") > 1)
        .orderBy("Agency")
    )

    inconsistent_agency_count = agency_name_consistency.count()

    print(f"Agencies mapped to multiple agency names: {inconsistent_agency_count}")

    if inconsistent_agency_count > 0:
        agency_name_consistency.show(truncate=False)

    print()

if __name__ == "__main__":
    #inspect_structure(df)
    #inspect_missing_values(df)
    #inspect_missing_placeholders(df)
    #inspect_duplicates(df)
    inspect_status_quality(df)
    #inspect_geography(df)
    #inspect_complaints(df)
    #inspect_agencies(df)