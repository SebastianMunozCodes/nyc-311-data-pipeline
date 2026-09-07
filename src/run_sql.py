from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent.parent
SQL_FILE = BASE_DIR / "sql" / "analysis.sql"

QUERY_TITLES = [
    "Top Complaint Types Within Each Borough",
    "High-Volume and Slow-Resolution Complaint Types",
    "Agency Workload vs Resolution Speed",
    "Borough Resolution Coverage and Speed",
    "Peak Hours for 311 Activity",
    "Day of Week and Weekday/Weekend Pattern",
    "Daily Spike vs the Average Day",
    "Peak Hour By Complaint Type",
    "Complaint Resolution Differences Across Boroughs",
]

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("NYC311Analysis")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW nyc_311
        USING PARQUET
        OPTIONS (
            path 'data/transformed/nyc_311_transformed.parquet'
        )
    """)

    with open(SQL_FILE, "r") as file:
        sql_script = file.read()

    sql_queries = [
        query.strip()
        for query in sql_script.split(";")
        if query.strip()
    ]

    for query_number, query in enumerate(sql_queries, start=1):
        result = spark.sql(query)

        if result.columns:
            title = (
                QUERY_TITLES[query_number - 1]
                if query_number <= len(QUERY_TITLES)
                else f"Query {query_number}"
            )

            print()
            print("=" * 80)
            print(f"Query {query_number}: {title}")
            print("=" * 80)

            result.show(100, truncate=False)

    spark.stop()