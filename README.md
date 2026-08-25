# NYC 311 Data Pipeline

A data engineering portfolio project that processes NYC 311 service request data using Python, Pandas, Parquet, PySpark, SQL, and Databricks.

## Project Overview

This project builds an end-to-end data pipeline using NYC 311 service request data.

The current dataset covers:

- All five NYC boroughs
- November 27, 2025 through January 2, 2026
- 381,228 service requests
- 44 raw columns

The goal is to clean, validate, transform, and analyze the data using tools commonly used in data engineering workflows.

## Current Pipeline

NYC Open Data  
↓  
Raw CSV  
↓  
Python / Pandas Ingestion  
↓  
Schema Validation  
↓  
Data Inspection  
↓  
Data Cleaning  
↓  
Derived Columns  
↓  
Cleaned CSV + Parquet  
↓  
PySpark Transformations  
↓  
SQL Analysis  
↓  
Databricks  

## Current Project Status

### Completed

- Project structure
- Raw data ingestion
- File existence validation
- Expected column validation
- Raw data inspection
- Missing value inspection
- Duplicate detection
- Borough and status inspection
- Complaint type consistency checks
- Date range validation
- Datetime conversion
- Complaint type standardization
- Derived column creation
- Cleaned CSV output
- Cleaned Parquet output

### Next Steps

- Load cleaned Parquet data using PySpark
- Practice Spark DataFrame operations
- Apply filtering and transformations
- Perform aggregations with PySpark
- Build SQL analysis queries
- Introduce Databricks into the workflow

## Data Cleaning

The cleaning stage currently includes:

- Converting date columns to datetime
- Preserving legitimate missing values instead of blindly dropping rows
- Standardizing inconsistent complaint type capitalization
- Keeping unresolved requests with missing closed dates
- Preserving missing geographic fields when the remaining record is still useful

Datetime columns include:

- `Created Date`
- `Closed Date`
- `Resolution Action Updated Date`

## Derived Columns

The pipeline creates the following additional fields:

- `request_year`
- `request_month`
- `request_day_of_week`
- `resolution_time`

`resolution_time` is calculated as:

`Closed Date - Created Date`

Requests that have not been closed retain a missing resolution time.

## Output Formats

The cleaned dataset is written to both:

`data/processed/nyc_311_cleaned.csv`

`data/processed/nyc_311_cleaned.parquet`

Parquet is included because it provides:

- Smaller file sizes
- Better preservation of data types
- Faster analytical processing
- Efficient column-based reads
- Strong compatibility with Spark and Databricks

The raw and processed datasets are excluded from GitHub because of their size.

## Project Structure

nyc-311-data-pipeline/  
├── data/  
│   ├── raw/  
│   ├── processed/  
│   └── transformed/  
├── notebooks/  
├── src/  
│   ├── ingest.py  
│   ├── inspection.py  
│   └── clean.py  
├── README.md  
├── requirements.txt  
└── .gitignore  

## Technologies

- Python
- Pandas
- PyArrow
- Parquet
- PySpark
- SQL
- Databricks

## Data Source

NYC Open Data  
311 Service Requests from 2020 to Present

## Project Goal

The goal of this project is to demonstrate a practical data engineering workflow by taking raw public data and moving it through ingestion, validation, cleaning, transformation, storage, and analysis stages.

Future analysis will focus on questions such as:

- Which complaint types are most common?
- Which boroughs generate the most service requests?
- Which agencies receive the most requests?
- How does complaint volume change over time?
- Which complaint types take the longest to resolve?
- How does resolution time vary across boroughs and agencies?
