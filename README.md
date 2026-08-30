# NYC 311 Data Engineering Pipeline

A data engineering portfolio project that builds a PySpark-based pipeline for ingesting, validating, cleaning, transforming, and analyzing NYC 311 service request data.

The project uses a historical NYC 311 dataset to demonstrate practical data engineering concepts including Spark DataFrames, data quality validation, Parquet storage, transformations, SQL analytics, and Databricks.

## Table of Contents

- [Project Overview](#project-overview)
- [Pipeline Architecture](#pipeline-architecture)
- [Current Project Status](#current-project-status)
- [Data Quality Findings](#data-quality-findings)
- [PySpark Ingestion](#pyspark-ingestion)
- [PySpark Inspection](#pyspark-inspection)
- [PySpark Cleaning](#pyspark-cleaning)
- [Why Parquet?](#why-parquet)
- [Planned PySpark Transformations](#planned-pyspark-transformations)
- [SQL Analysis](#sql-analysis)
- [Databricks](#databricks)
- [Project Structure](#project-structure)
- [Technologies](#technologies)
- [Requirements](#requirements)
- [Data Source](#data-source)
- [Project Goal](#project-goal)

## Project Overview

This project builds an end-to-end data engineering pipeline using NYC 311 service request data.

The pipeline begins with raw CSV data from NYC Open Data and processes it locally using PySpark. Data quality issues are identified through a dedicated inspection layer before cleaning rules are applied.

The cleaned dataset is stored in Parquet format for downstream Spark transformations, SQL analysis, and eventual Databricks integration.

### Dataset Scope

- **Source:** NYC Open Data
- **Dataset:** 311 Service Requests from 2020 to Present
- **Date range:** November 27, 2025 through January 2, 2026
- **Rows:** 381,228
- **Raw columns:** 44
- **Coverage:** All five NYC boroughs

A historical date range was intentionally selected so the project could work with a mostly completed set of service requests rather than continuously changing current data.

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
PySpark Transformations
      ↓
Transformed Data
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

### In Progress

- PySpark transformation pipeline

### Planned

- Create derived analytical columns
- Calculate valid resolution times
- Build Spark aggregations
- Create transformed datasets
- Write transformed outputs to Parquet
- Analyze transformed data with SQL
- Use CTEs, joins, aggregations, and window functions
- Introduce Databricks into the workflow
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

These values are normalized to Spark `NULL` values during cleaning.

### Closed Date

There are **5,653 requests with a missing `Closed Date`**.

Most belong to unresolved requests with statuses such as:

- Open
- In Progress
- Pending
- Assigned
- Started

One request marked `Closed` also has a missing `Closed Date`.

Rather than fabricating a timestamp, the pipeline preserves the missing value.

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

These 40 records can therefore be excluded from calculations such as resolution time without losing the original records.

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

No duplicate-removal step is currently necessary.

### Agency Validation

Agency inspection found:

```text
Missing or blank agencies: 0
Agency code/name inconsistencies: 0
```

No agency cleaning is currently required.

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

The ingestion stage currently validates:

```text
381,228 rows
44 raw columns
```

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

This allows the project to follow the workflow:

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

rather than modifying the data blindly.

## PySpark Cleaning

`clean.py` applies the cleaning decisions identified during inspection.

### Datetime Conversion

The following fields are converted from raw strings into Spark timestamp types:

- `Created Date`
- `Closed Date`
- `Resolution Action Updated Date`

This allows them to be used safely for downstream date and time calculations.

### Missing-Value Normalization

Literal `N/A` placeholders are converted to Spark `NULL` values in known affected columns.

The cleaning pipeline currently handles placeholders found in fields including:

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

This allows later transformations to prevent invalid negative resolution-time calculations while preserving the original source data.

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

If these expectations fail, the pipeline raises an error rather than silently producing an incorrect processed dataset.

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

## Why Parquet?

The processed layer uses Parquet for downstream analytical processing.

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

Spark writes the cleaned Parquet dataset as a directory containing multiple partition files.

Example:

```text
nyc_311_cleaned.parquet/
├── _SUCCESS
├── part-00000-....snappy.parquet
├── part-00001-....snappy.parquet
├── part-00002-....snappy.parquet
└── ...
```

Spark reads the directory as one logical dataset.

## Planned PySpark Transformations

The next stage of the project will transform the cleaned Parquet dataset into structures that are more useful for analysis.

Planned transformations include:

- request year
- request month
- request day of week
- valid resolution-time calculations
- complaint volume aggregations
- borough-level aggregations
- agency-level aggregations
- complaint-type aggregations
- date-based aggregations
- null-aware transformations

Invalid date-order records will be accounted for when calculating resolution times so that misleading negative durations are not produced.

The transformed outputs will be written to the:

```text
data/transformed/
```

layer.

## SQL Analysis

SQL will be used after the transformation stage to answer analytical questions using the processed data.

Planned questions include:

- Which complaint types generated the most requests?
- Which boroughs generated the highest request volume?
- Which agencies handled the most requests?
- How did request volume change throughout the holiday period?
- Which complaint types had the longest resolution times?
- How did resolution time vary by borough?
- How did resolution time vary by agency?
- How did complaint patterns differ across boroughs?
- How did complaint volume change by day or week?

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
│   │   └── nyc_311_raw.csv
│   ├── processed/
│   │   └── nyc_311_cleaned.parquet/
│   └── transformed/
├── notebooks/
├── src/
│   ├── ingest.py
│   ├── inspection.py
│   └── clean.py
├── README.md
├── requirements.txt
└── .gitignore
```

The raw and processed datasets are excluded from GitHub because of their size.

Additional pipeline files will be added as the transformation and analytical stages are completed.

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
Parquet Storage
   ↓
Transformation
   ↓
SQL Analysis
   ↓
Databricks
```

The project emphasizes both implementation and data engineering decision-making.

Instead of simply applying transformations, each stage is designed to answer a specific question:

- Is the expected source data present?
- Does the schema contain the fields the pipeline depends on?
- What data-quality problems exist?
- Which issues should actually be corrected?
- Which missing values should be preserved?
- How should suspicious source records be handled without fabricating information?
- How can cleaned data be structured efficiently for downstream analytics?

The final result is intended to demonstrate an end-to-end data engineering workflow built around PySpark, Parquet, SQL, and Databricks.
