# NYC 311 Data Engineering Pipeline

A data engineering portfolio project that builds a PySpark-based pipeline for ingesting, validating, cleaning, transforming, and analyzing NYC 311 service request data.

The project uses a historical NYC 311 dataset to practice and demonstrate data engineering concepts including Spark DataFrames, data quality validation, Parquet storage, transformations, SQL analytics, and Databricks.

## Project Overview

The dataset contains NYC 311 service requests across all five boroughs during the 2025 holiday season.

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
Structured / Transformed Output
      ↓
SQL Analysis
      ↓
Databricks
```

PySpark is the primary processing engine for the pipeline.

## Current Project Status

### Completed

- Project structure and virtual environment
- NYC Open Data acquisition
- PySpark installation and local Spark configuration
- Raw CSV ingestion with PySpark
- File existence validation
- Expected column validation
- Spark schema inspection
- Dataset row and column validation
- Date range validation
- Missing-value analysis
- Detection of source placeholder values such as `N/A`
- Full-row duplicate detection
- Unique Key duplicate validation
- Status quality analysis
- Closed Date consistency checks
- Invalid date-order detection
- Borough and geographic data inspection
- Coordinate completeness checks
- Complaint type analysis
- Agency quality and consistency analysis

### In Progress

- PySpark cleaning pipeline

### Planned

- Write cleaned data to Parquet using Spark
- Build meaningful PySpark transformations
- Use `withColumn`, `groupBy`, and aggregations
- Perform null handling with Spark
- Create transformed datasets
- Analyze transformed data with SQL
- Introduce Databricks into the workflow
- Document final pipeline architecture and findings

## Data Quality Findings

The PySpark inspection layer identified several issues that guide the cleaning stage.

### Missing Values

Missing data is not automatically removed.

Some fields are naturally optional or only apply to specific types of 311 requests. The pipeline preserves missing values when there is not enough reliable information to replace them.

The inspection also found that NYC 311 uses the literal value:

```text
N/A
```

in several string columns as a missing-data placeholder.

These values will be normalized to Spark `NULL` values during cleaning.

### Closed Date

Most requests with a missing `Closed Date` are legitimately unresolved requests with statuses such as:

- Open
- In Progress
- Pending
- Assigned
- Started

One request marked `Closed` has a missing `Closed Date`. The pipeline will preserve the missing value rather than fabricate a timestamp.

### Invalid Date Ordering

Inspection identified **40 records where `Created Date` occurs after `Closed Date`**.

Of these:

- 38 are Pending DOT Street Light Condition requests
- 2 are Closed DOT Street Condition requests

The original source dates will be preserved, but these records will not be allowed to produce misleading negative resolution-time values.

### Complaint Type Consistency

Two complaint types appear with inconsistent capitalization:

```text
Elevator
ELEVATOR

Plumbing
PLUMBING
```

These values will be standardized during cleaning.

### Geographic Data

Some records contain:

- missing ZIP codes
- missing latitude/longitude
- `Unspecified` borough values

These values will not be fabricated when reliable replacement information is unavailable.

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

`spark_ingest.py` is responsible for loading and validating the raw NYC 311 dataset.

The ingestion stage:

- verifies that the raw file exists
- creates a Spark DataFrame from the CSV
- reads the CSV header
- infers the initial schema
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

## PySpark Inspection

`spark_inspection.py` performs data-quality analysis before any cleaning decisions are applied.

Current inspections include:

- row and column counts
- schema inspection
- date range validation
- null counts and percentages
- missing-value placeholder detection
- core-field completeness
- full-row duplicates
- duplicate Unique Keys
- status distributions
- Closed Date consistency
- invalid date ordering
- borough distributions
- ZIP-code completeness
- coordinate completeness
- complaint type distributions
- agency distributions
- agency name consistency

The inspection stage is intentionally separated from cleaning so that cleaning rules are based on observed data rather than assumptions.

## Cleaning Plan

`spark_clean.py` will apply the cleaning decisions identified during inspection.

Planned cleaning includes:

1. Convert date columns to Spark timestamp types:
   - `Created Date`
   - `Closed Date`
   - `Resolution Action Updated Date`

2. Normalize known missing-value placeholders:
   - Convert literal `N/A` values to Spark `NULL`

3. Handle invalid date ordering:
   - Preserve original source values
   - Prevent invalid negative resolution times

4. Standardize complaint type capitalization:
   - `ELEVATOR` → `Elevator`
   - `PLUMBING` → `Plumbing`

5. Preserve legitimate unresolved requests with missing `Closed Date`

6. Preserve uncertain geographic values rather than fabricating data

7. Write the cleaned dataset to Parquet

## Why Parquet?

The processed layer uses Parquet instead of relying on CSV for downstream processing.

Parquet provides:

- columnar storage
- smaller file sizes
- preserved data types
- efficient analytical reads
- strong PySpark compatibility
- strong Databricks compatibility
- efficient downstream SQL analytics

The raw source remains CSV because that is how the dataset is obtained from NYC Open Data.

## Planned PySpark Transformations

After cleaning, `spark_transform.py` will contain meaningful project transformations rather than basic Spark practice exercises.

Planned work includes:

- derived date fields
- resolution-time calculations
- complaint volume aggregations
- borough-level aggregations
- agency-level aggregations
- complaint-type aggregations
- null-aware transformations
- potentially joins where they add meaningful analytical value

The goal is to demonstrate how Spark can transform a cleaned dataset into structured analytical outputs.

## SQL Analysis

SQL will be used after the transformation stage to answer analytical questions using the structured data produced by the pipeline.

Planned questions include:

- Which complaint types generated the most requests?
- Which boroughs generated the highest request volume?
- Which agencies handled the most requests?
- How did request volume change throughout the holiday period?
- Which complaint types had the longest resolution times?
- How did resolution time vary by borough?
- How did resolution time vary by agency?
- How did complaint patterns differ across boroughs?

The SQL portion will focus on transferable concepts such as:

- filtering
- grouping
- aggregations
- CTEs
- joins
- window functions

## Databricks

A later stage of the project will recreate meaningful parts of the local Spark workflow in Databricks.

The purpose is to demonstrate how the same PySpark and SQL concepts used locally can operate within a modern data engineering platform.

## Project Structure

```text
nyc-311-data-pipeline/
├── data/
│   ├── raw/
│   ├── processed/
│   └── transformed/
├── notebooks/
├── src/
│   ├── clean.py
│   ├── spark_ingest.py
│   ├── spark_inspection.py
│   ├── spark_clean.py
│   └── spark_transform.py
├── README.md
├── requirements.txt
└── .gitignore
```

`clean.py` is currently retained temporarily as a reference from the earlier Pandas implementation while the cleaning logic is migrated to PySpark. It will be removed once `spark_clean.py` has been completed and validated.

Raw and processed datasets are excluded from GitHub because of their size.

## Technologies

- Python
- Apache Spark
- PySpark
- Parquet
- PyArrow
- SQL
- Databricks
- Git / GitHub

Pandas was used during the early prototype of the project, but the active pipeline is being rebuilt around PySpark.

## Requirements

Python dependencies are listed in:

```text
requirements.txt
```

They can be installed with:

```bash
pip install -r requirements.txt
```

The project also requires a compatible Java installation because Apache Spark runs on the JVM.

## Data Source

**NYC Open Data**  
**311 Service Requests from 2020 to Present**

The raw dataset is not stored in this repository because of its size.

## Project Goal

The goal of this project is to demonstrate a practical Spark-oriented data engineering workflow using real public data.

Rather than using PySpark only for isolated examples, the project applies Spark throughout the pipeline:

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
Transformation
   ↓
Storage
   ↓
SQL Analysis
   ↓
Databricks
```

The final project is intended to demonstrate both the technical implementation and the reasoning behind data-quality and transformation decisions.