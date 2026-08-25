import pandas as pd
from ingest import load_raw_data


def original_inspection(df):
    print("Shape of df:")
    print(df.shape)
    print()

    print("General Information:")
    df.info()
    print()

    print("Column names:")
    print(df.columns.tolist())
    print()

    print("Number of missing values in each column:")
    print(df.isna().sum())
    print()

    print("Full-row duplicates:", df.duplicated().sum())
    print()

    print(
        "Duplicate Unique Keys:",
        df["Unique Key"].duplicated().sum()
    )
    print()

    print("Borough counts:")
    print(df["Borough"].value_counts(dropna=False))
    print()

    print("Status counts:")
    print(df["Status"].value_counts(dropna=False))
    print()

    print("Agency counts:")
    print(df["Agency"].value_counts(dropna=False))
    print()

    print("Top 20 complaint types:")
    print(
        df["Problem (formerly Complaint Type)"]
        .value_counts(dropna=False)
        .head(20)
    )
    print()


def inspect_closed_dates(df):
    closed_date_df = df.loc[
        df["Closed Date"].isna(),
        ["Created Date", "Closed Date", "Status"]
    ]

    print("Rows with missing Closed Date:")
    print(closed_date_df)
    print()

    print("Status counts for rows with missing Closed Date:")
    print(closed_date_df["Status"].value_counts(dropna=False))
    print()

    closed_missing_date_df = df.loc[
        (df["Closed Date"].isna())
        & (df["Status"] == "Closed"),
        [
            "Unique Key",
            "Created Date",
            "Closed Date",
            "Resolution Action Updated Date",
            "Resolution Description",
        ]
    ]

    print("Closed requests with missing Closed Date:")
    print(closed_missing_date_df.to_string(index=False))
    print()


def inspect_missing_zip(df):
    zip_df = df.loc[
        df["Incident Zip"].isna(),
        [
            "Borough",
            "City",
            "Incident Address",
            "Latitude",
            "Longitude",
            "Location",
        ]
    ]

    print("Rows with missing Incident Zip:")
    print(zip_df)
    print()

    valid_borough_count = (
        zip_df["Borough"] != "Unspecified"
    ).sum()

    print(
        "How many still have a real borough:",
        valid_borough_count
    )
    print()

    unspecified_count = (
        zip_df["Borough"] == "Unspecified"
    ).sum()

    print(
        "How many have Borough = Unspecified:",
        unspecified_count
    )
    print()

    lat_long_present_count = (
        zip_df["Latitude"].notna()
        & zip_df["Longitude"].notna()
    ).sum()

    print(
        "How many have latitude/longitude present:",
        lat_long_present_count
    )
    print()

    lat_long_missing_count = (
        zip_df["Latitude"].isna()
        & zip_df["Longitude"].isna()
    ).sum()

    print(
        "How many have latitude/longitude missing:",
        lat_long_missing_count
    )
    print()

    both_missing_count = (
        (zip_df["Borough"] == "Unspecified")
        & zip_df["Latitude"].isna()
        & zip_df["Longitude"].isna()
    ).sum()

    print(
        "How many have an unspecified borough "
        "and missing coordinates:",
        both_missing_count
    )
    print()

    zip2_df = zip_df.loc[
        (zip_df["Borough"] == "Unspecified")
        & zip_df["Latitude"].isna()
        & zip_df["Longitude"].isna(),
        ["City", "Incident Address"]
    ]

    print(
        "Rows with missing ZIP, unspecified borough, "
        "and missing coordinates:"
    )
    print(zip2_df)
    print()

    incident_address_missing_count = (
        zip2_df["Incident Address"].isna().sum()
    )

    print(
        "How many of those rows also have a missing "
        "Incident Address:",
        incident_address_missing_count
    )
    print()


def inspect_unspecified_borough(df):
    unspecified_borough_df = df.loc[
        df["Borough"] == "Unspecified",
        [
            "Incident Zip",
            "City",
            "Incident Address",
            "Latitude",
            "Longitude",
            "Location",
        ]
    ]

    print(
        "Total unspecified borough rows:",
        len(unspecified_borough_df)
    )

    print(
        "With Incident Zip:",
        unspecified_borough_df["Incident Zip"].notna().sum()
    )

    print(
        "With City:",
        unspecified_borough_df["City"].notna().sum()
    )

    print(
        "With Incident Address:",
        unspecified_borough_df["Incident Address"].notna().sum()
    )

    print(
        "With latitude/longitude:",
        (
            unspecified_borough_df["Latitude"].notna()
            & unspecified_borough_df["Longitude"].notna()
        ).sum()
    )

    print(
        "No ZIP, no city, no address, no coordinates:",
        (
            unspecified_borough_df["Incident Zip"].isna()
            & unspecified_borough_df["City"].isna()
            & unspecified_borough_df["Incident Address"].isna()
            & unspecified_borough_df["Latitude"].isna()
            & unspecified_borough_df["Longitude"].isna()
        ).sum()
    )
    print()

def inspect_unspecified_status(df):
    unspecified_status = df.loc[
        df["Status"] == "Unspecified",
        [
            "Unique Key",
            "Problem (formerly Complaint Type)",
            "Problem Detail (formerly Descriptor)",
            "Created Date",
            "Closed Date",
            "Status",
            "Resolution Action Updated Date",
            "Resolution Description",
            "Agency"
        ]
    ]
    print(unspecified_status.to_string(index=False))
    print()

def inspect_complaint_type_consistency(df):
    complaint_col = "Problem (formerly Complaint Type)"

    complaint_df = df[[complaint_col]].copy()

    complaint_df["normalized"] = (
        complaint_df[complaint_col]
        .str.strip()
        .str.lower()
    )

    grouped = (
        complaint_df
        .groupby("normalized")[complaint_col]
        .nunique()
        .sort_values(ascending=False)
    )

    inconsistent_groups = grouped[grouped > 1]

    print("Normalized complaint types with multiple original versions:")
    print(inconsistent_groups)
    print()

    for normalized_value in inconsistent_groups.index:
        original_values = complaint_df.loc[
            complaint_df["normalized"] == normalized_value,
            complaint_col
        ].unique()

        print(f"Normalized value: {normalized_value}")
        print("Original values:", original_values)
        print()

def inspect_date_range(df):
    created_date = "Created Date"

    df[created_date] = pd.to_datetime(
            df[created_date],
            format="%m/%d/%Y %I:%M:%S %p"
        )

    print(f"Earliest created date: {df[created_date].min()}")
    print(f"Latest created date: {df[created_date].max()}")

if __name__ == "__main__":
    df = load_raw_data()

    # Uncomment whichever inspection you want to run.

    original_inspection(df)

    inspect_closed_dates(df)

    inspect_missing_zip(df)

    inspect_unspecified_borough(df)

    inspect_unspecified_status(df)

    inspect_complaint_type_consistency(df)

    inspect_date_range(df)