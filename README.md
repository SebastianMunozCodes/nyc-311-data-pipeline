# NYC 311 Data Engineering Pipeline

A data engineering portfolio project that builds an end-to-end PySpark pipeline for ingesting, validating, cleaning, transforming, and analyzing NYC 311 service request data.

The project uses a historical NYC 311 dataset to demonstrate practical data engineering concepts including Spark DataFrames, data quality validation, Parquet storage, transformations, analytical summaries, SQL, and Databricks. :contentReference[oaicite:0]{index=0}

## Table of Contents

- [Project Overview](#project-overview)
- [Pipeline Architecture](#pipeline-architecture)
- [Current Project Status](#current-project-status)
- [Data Quality Findings](#data-quality-findings)
- [PySpark Ingestion](#pyspark-ingestion)
- [PySpark Inspection](#pyspark-inspection)
- [PySpark Cleaning](#pyspark-cleaning)
- [PySpark Transformation](#pyspark-transformation)
- [Transformation Outputs](#transformation-outputs)
- [Why Parquet?](#why-parquet)
- [SQL Analysis](#sql-analysis)
- [Databricks](#databricks)
- [Project Structure](#project-structure)
- [Technologies](#technologies)
- [Requirements](#requirements)
- [Data Source](#data-source)
- [Project Goal](#project-goal)

## Project Overview

This project builds an end-to-end data engineering pipeline using NYC 311 service request data.

The pipeline begins with raw CSV data from NYC Open Data and processes it locally using PySpark. Data quality issues are identified through a dedicated inspection stage before cleaning rules are applied.

The cleaned dataset is stored in Parquet format, transformed into analysis-ready data, and used to produce summary datasets for complaint types, boroughs, agencies, and request dates.

### Dataset Scope

- **Source:** NYC Open Data
- **Dataset:** 311 Service Requests from 2020 to Present
- **Date range:** November 27, 2025 through January 2, 2026
- **Rows:** 381,228
- **Raw columns:** 44
- **Coverage:** All five NYC boroughs

A historical date range was intentionally selected so the project could work with a mostly completed set of service requests rather than continuously changing current data. :contentReference[oaicite:1]{index=1}

## Pipeline Architecture

```text
NYC Open Data
      ↓
Raw CSV
      ↓
PySpark Ingestion
      ↓
Schema & Column Validation
      ↓
PySpark Data Quality Inspection
      ↓
PySpark Cleaning
      ↓
Cleaned Parquet
      ↓
PySpark Transformation
      ↓
Transformed Parquet
      ↓
Summary Datasets
      ↓
SQL Analysis
      ↓
Databricks
```

PySpark is the primary processing engine throughout the pipeline.

## Current Project Status

### Completed

- Project structure and virtual environment
- NYC Open Data acquisition
- Local PySpark configuration
- Raw CSV ingestion with PySpark
- Raw file existence validation
- Expected column validation
- Spark schema inspection
- Dataset row and column validation
- Date range validation
- Missing-value analysis
- Missing-value placeholder detection
- Full-row duplicate detection
- Unique Key duplicate validation
- Status quality analysis
- Closed Date consistency analysis
- Invalid date-order detection
- Borough and geographic data inspection
- ZIP-code completeness analysis
- Coordinate completeness analysis
- Complaint type consistency analysis
- Agency consistency analysis
- PySpark cleaning pipeline
- Datetime conversion to Spark timestamp types
- Missing-value placeholder normalization
- Complaint type capitalization standardization
- Invalid date-order flagging
- Post-cleaning validation
- Cleaned Parquet output
- PySpark transformation pipeline
- Request date feature creation
- Resolution-time calculation
- Transformation validation
- Transformed Parquet output
- Complaint summary dataset
- Borough summary dataset
- Agency summary dataset
- Date summary dataset
- Git-tracked data directory structure using `.gitkeep`

### In Progress

- SQL analysis layer

### Planned

- Build analytical SQL queries
- Use CTEs, joins, aggregations, ranking, and window functions
- Analyze complaint volume and resolution-time patterns
- Introduce Databricks into the workflow
- Recreate meaningful parts of the local Spark workflow in Databricks
- Document final analytical findings

## Data Quality Findings

The inspection stage identified several data-quality issues before cleaning logic was implemented.

This separation ensures that cleaning decisions are based on observed properties of the dataset rather than assumptions.

### Missing Values

Missing data is not automatically removed.

Many NYC 311 fields are optional or only apply to specific types of service requests. Missing values are therefore preserved unless there is a reliable reason to modify them.

The inspection stage also found that NYC 311 uses the literal value:

```text
N/A
```

as a missing-data placeholder in several string columns.

These values are normalized to Spark `NULL` values during cleaning. :contentReference[oaicite:2]{index=2}

### Closed Date

There are **5,653 requests with a missing `Closed Date`**.

Most belong to unresolved requests with statuses such as:

- Open
- In Progress
- Pending
- Assigned
- Started

One request marked `Closed` also has a missing `Closed Date`.

Rather than fabricating a timestamp, the pipeline preserves the missing value. :contentReference[oaicite:3]{index=3}

### Invalid Date Ordering

Inspection identified **40 records where `Created Date` occurs after `Closed Date`**.

Of these:

- 38 are Pending DOT Street Light Condition requests
- 2 are Closed DOT Street Condition requests

The original timestamps are preserved.

Instead of modifying source data without evidence, the cleaning pipeline adds:

```text
Invalid Date Order
```

as a boolean column.

These records are preserved but prevented from receiving invalid negative resolution-time values during transformation. :contentReference[oaicite:4]{index=4}

### Complaint Type Consistency

The inspection identified inconsistent capitalization for two complaint categories:

```text
ELEVATOR
Elevator

PLUMBING
Plumbing
```

The cleaning pipeline standardizes them as:

```text
ELEVATOR → Elevator
PLUMBING → Plumbing
```

### Geographic Data

Some records contain:

- missing ZIP codes
- missing latitude and longitude
- `Unspecified` borough values

These values are preserved when reliable replacement information is unavailable.

The pipeline does not fabricate geographic information.

### Duplicate Validation

The dataset contains:

```text
Full-row duplicates: 0
Duplicate Unique Keys: 0
```

No duplicate-removal step is necessary. :contentReference[oaicite:5]{index=5}

### Agency Validation

Agency inspection found:

```text
Missing or blank agencies: 0
Agency code/name inconsistencies: 0
```

No agency cleaning is required. :contentReference[oaicite:6]{index=6}

## PySpark Ingestion

`ingest.py` is responsible for loading and validating the raw NYC 311 dataset.

The ingestion stage:

- verifies that the raw CSV exists
- loads the dataset into a Spark DataFrame
- reads the CSV header
- infers the initial Spark schema
- preserves identifier-like fields such as ZIP codes as strings
- validates required columns
- returns a reusable Spark DataFrame

Important expected fields include:

- `Unique Key`
- `Created Date`
- `Closed Date`
- `Agency`
- `Problem (formerly Complaint Type)`
- `Status`
- `Borough`

The ingestion stage validates:

```text
381,228 rows
44 raw columns
```

:contentReference[oaicite:7]{index=7}

## PySpark Inspection

`inspection.py` performs data-quality analysis before cleaning decisions are applied.

Current inspections include:

- row count
- column count
- Spark schema
- earliest and latest request dates
- null counts
- null percentages
- core-field completeness
- missing-value placeholder detection
- full-row duplicates
- duplicate Unique Keys
- status distributions
- missing Closed Dates by status
- Closed requests with missing Closed Dates
- non-Closed requests containing Closed Dates
- invalid date ordering
- borough distributions
- missing ZIP codes
- unspecified borough records
- missing geographic coordinates
- coordinate mismatches
- complaint type distributions
- complaint type consistency
- agency distributions
- agency name consistency

The inspection layer is intentionally separate from the cleaning layer.

```text
Inspect
   ↓
Identify Problems
   ↓
Define Cleaning Rules
   ↓
Clean
   ↓
Validate
```

This prevents the pipeline from modifying data blindly. :contentReference[oaicite:8]{index=8}

## PySpark Cleaning

`clean.py` applies the cleaning decisions identified during inspection.

### Datetime Conversion

The following fields are converted from raw strings into Spark timestamp types:

- `Created Date`
- `Closed Date`
- `Resolution Action Updated Date`

### Missing-Value Normalization

Literal `N/A` placeholders are converted to Spark `NULL` values in known affected columns.

The cleaning pipeline handles placeholders found in:

- `Problem Detail (formerly Descriptor)`
- `Additional Details`
- `Facility Type`
- `Resolution Description`
- `Park Facility Name`
- `Location Type`
- `Road Ramp`
- `Bridge Highway Segment`

Post-cleaning validation confirms that no known `N/A` placeholders remain.

### Invalid Date Ordering

The cleaning pipeline preserves the 40 records where:

```text
Created Date > Closed Date
```

and adds:

```text
Invalid Date Order
```

as a boolean flag.

### Complaint Type Standardization

The following complaint categories are standardized:

```text
ELEVATOR → Elevator
PLUMBING → Plumbing
```

### Post-Cleaning Validation

Before the processed dataset is written, the cleaning pipeline verifies that:

- exactly 40 invalid date-order records are flagged
- known `N/A` placeholders have been removed
- unstandardized `ELEVATOR` and `PLUMBING` values no longer remain

If these expectations fail, the pipeline raises an error rather than silently producing incorrect processed data.

### Cleaned Dataset

The cleaned output contains:

```text
Rows: 381,228
Columns: 45
Duplicate Unique Keys: 0
Remaining known N/A placeholders: 0
Invalid Date Order records: 40
```

The cleaned dataset is written to:

```text
data/processed/nyc_311_cleaned.parquet
```

:contentReference[oaicite:9]{index=9}

## PySpark Transformation

`transform.py` converts the cleaned request-level dataset into analysis-ready data.

### Request Date Features

The transformation pipeline adds:

- `request_date`
- `request_year`
- `request_month`
- `request_day_of_week`
- `request_hour`

These fields allow downstream analysis by date, month, weekday, and hour.

### Resolution Time

The pipeline adds:

```text
resolution_time_hours
```

Resolution time is calculated only when:

- `Status` is `Closed`
- `Closed Date` is present
- `Invalid Date Order` is `False`

This prevents unresolved, non-Closed, or invalid-date records from producing misleading resolution-time values.

### Transformation Validation

Before transformed data is written, the pipeline verifies that:

- all expected transformation columns exist
- the transformed row count matches the cleaned row count
- invalid date-order records do not have resolution times
- unresolved records do not have resolution times
- non-Closed records do not have resolution times
- no negative resolution times exist

The transformed request-level dataset contains:

```text
Rows: 381,228
Columns: 51
Invalid rows with resolution time: 0
Non-Closed rows with resolution time: 0
Negative resolution times: 0
```

## Transformation Outputs

The transformation stage produces one request-level transformed dataset and four summary datasets.

### Transformed Request-Level Dataset

```text
data/transformed/nyc_311_transformed.parquet
```

This dataset contains the original cleaned fields plus derived analytical columns.

### Complaint Summary

```text
data/transformed/complaints_summary.parquet
```

Contains:

- complaint type
- request count
- average resolution time in hours

### Borough Summary

```text
data/transformed/borough_summary.parquet
```

Contains:

- borough
- request count
- average resolution time in hours

### Agency Summary

```text
data/transformed/agency_summary.parquet
```

Contains:

- agency
- request count
- average resolution time in hours

### Date Summary

```text
data/transformed/date_summary.parquet
```

Contains:

- request date
- request count

These datasets provide reusable inputs for SQL analysis without requiring every analytical query to rebuild the same aggregations.

## Why Parquet?

The processed and transformed layers use Parquet for analytical processing.

Unlike CSV, Parquet is a columnar storage format designed for analytical workloads.

Benefits include:

- columnar storage
- smaller file sizes
- preserved data types
- efficient column-based reads
- compression
- strong PySpark compatibility
- strong Databricks compatibility
- efficient downstream SQL analytics

The raw dataset remains CSV because that is how the source data is obtained from NYC Open Data.

Spark writes Parquet datasets as directories containing multiple partition files.

Example:

```text
nyc_311_cleaned.parquet/
├── _SUCCESS
├── part-00000-....snappy.parquet
├── part-00001-....snappy.parquet
├── part-00002-....snappy.parquet
└── ...
```

Spark reads the directory as one logical dataset. :contentReference[oaicite:10]{index=10}

## SQL Analysis

SQL is the next stage of the project.

The SQL layer will use the transformed request-level dataset and summary outputs to answer analytical questions such as:

- Which complaint types generated the most requests?
- Which boroughs generated the highest request volume?
- Which agencies handled the most requests?
- How did request volume change throughout the holiday period?
- Which complaint types had the longest average resolution times?
- How did resolution time vary by borough?
- How did resolution time vary by agency?
- How did complaint patterns differ across boroughs?
- How did complaint volume change by day or week?
- Which periods experienced unusually high request volume?

The SQL portion will demonstrate concepts including:

- filtering
- grouping
- aggregations
- CTEs
- joins
- window functions
- ranking
- date-based analysis

## Databricks

A later stage of the project will recreate meaningful parts of the local Spark workflow in Databricks.

The purpose is to demonstrate how the same PySpark, Parquet, and SQL concepts used locally can operate within a modern data engineering platform.

This stage will focus on transferring the existing pipeline workflow rather than rebuilding an unrelated project inside Databricks.

## Project Structure

```text
nyc-311-data-pipeline/
├── data/
│   ├── raw/
│   │   └── .gitkeep
│   ├── processed/
│   │   └── .gitkeep
│   └── transformed/
│       └── .gitkeep
├── notebooks/
├── src/
│   ├── ingest.py
│   ├── inspection.py
│   ├── clean.py
│   └── transform.py
├── README.md
├── requirements.txt
└── .gitignore
```

The actual datasets are excluded from GitHub because of their size.

The `.gitkeep` files allow the repository to preserve the intended directory structure while keeping generated data out of version control.

Locally, the pipeline produces:

```text
data/
├── raw/
│   └── nyc_311_raw.csv
├── processed/
│   └── nyc_311_cleaned.parquet/
└── transformed/
    ├── nyc_311_transformed.parquet/
    ├── complaints_summary.parquet/
    ├── borough_summary.parquet/
    ├── agency_summary.parquet/
    └── date_summary.parquet/
```

## Technologies

- Python
- Apache Spark
- PySpark
- Parquet
- PyArrow
- SQL
- Databricks
- Git
- GitHub

## Requirements

Python dependencies are listed in:

```text
requirements.txt
```

Install them with:

```bash
pip install -r requirements.txt
```

The project also requires a compatible Java installation because Apache Spark runs on the JVM.

## Data Source

**NYC Open Data**  
**311 Service Requests from 2020 to Present**

The raw dataset is downloaded from NYC Open Data and stored locally as:

```text
data/raw/nyc_311_raw.csv
```

The raw dataset itself is not stored in this repository because of its size.

## Project Goal

The goal of this project is to demonstrate a practical Spark-oriented data engineering workflow using real public data.

The project follows the data lifecycle:

```text
Raw Data
   ↓
Ingestion
   ↓
Validation
   ↓
Inspection
   ↓
Cleaning
   ↓
Processed Parquet
   ↓
Transformation
   ↓
Transformed Parquet & Summaries
   ↓
SQL Analysis
   ↓
Databricks
```

The project emphasizes both implementation and data engineering decision-making.

Each stage is designed to answer a specific question:

- Is the expected source data present?
- Does the schema contain the fields the pipeline depends on?
- What data-quality problems exist?
- Which issues should actually be corrected?
- Which missing values should be preserved?
- How should suspicious source records be handled without fabricating information?
- How should valid analytical metrics such as resolution time be defined?
- How can transformed data be structured efficiently for downstream analytics?

The final result is intended to demonstrate an end-to-end data engineering workflow built around PySpark, Parquet, SQL, and Databricks.
