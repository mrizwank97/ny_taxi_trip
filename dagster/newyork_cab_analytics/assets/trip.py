from dagster import (
    MonthlyPartitionsDefinition,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
    Definitions,
    RetryPolicy,
    Output,
    MetadataValue,
    AssetIn
)
import pandas as pd
from datetime import datetime, timedelta
from utils.download_files import download_file
from db_resource.db_resource import sqlite_resource

@asset(
    partitions_def=MonthlyPartitionsDefinition(start_date="2019-01-01"),
    retry_policy=RetryPolicy(max_retries=3, delay=5),
)
def download_trip(context) -> Output:
    """Asset for download file monthly"""
    format_string = "%Y-%m-%d"
    date_object = datetime.strptime(context.partition_key, format_string)
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date_object.strftime('%Y-%m')}.parquet"
    context.log.info(f"URL : {url}")

    target_file = f"yellow_tripdata_{date_object}.parquet"

    download_file(context, url, target_file)

    data = pd.read_parquet(target_file)

    # Setting up lineage
    context.metadata = {
        "source": MetadataValue.text(url),
        "description": MetadataValue.text(
            "Dynamic data source based on year and month."
        ),
    }

    return Output(
        value=data,
        metadata={
            "file_path": MetadataValue.path(target_file),
            "partition_key": MetadataValue.text(context.partition_key),
            "row_count": MetadataValue.int(len(data)),
        },
    )


@asset(
    ins={"downloaded_trip": AssetIn("download_trip")},
    partitions_def=MonthlyPartitionsDefinition(start_date="2019-01-01"),
)
def clean_trip(context, downloaded_trip: pd.DataFrame) -> Output:
    """Clean the downlaoded data"""
    # Remove the invalid date from data from pickup and drop time
    format_string = "%Y-%m-%d"
    date_object = datetime.strptime(context.partition_key, format_string)

    cleaned_data: pd.DataFrame = downloaded_trip[
        (downloaded_trip["tpep_dropoff_datetime"].dt.year == date_object.year)
        & (downloaded_trip["tpep_dropoff_datetime"].dt.month == date_object.month)
    ]
    
    cleaned_data = cleaned_data[
        (cleaned_data["tpep_pickup_datetime"].dt.year == date_object.year)
        & (cleaned_data["tpep_pickup_datetime"].dt.month == date_object.month)
    ]

    cleaned_data.columns = cleaned_data.columns.str.lower()

    # Remove the trip which have na or 0 and less than zero data
    cleaned_data = cleaned_data[cleaned_data["passenger_count"] > 0]

    # Remove if trip time is 0 or negative
    cleaned_data = cleaned_data[
        (cleaned_data["tpep_dropoff_datetime"] - cleaned_data["tpep_pickup_datetime"])
        > timedelta(seconds=0)
    ]

    # Remove the trip which have na or 0 and less than zero data
    cleaned_data = cleaned_data[cleaned_data["trip_distance"] > 0]

    # Remove negative fare
    cleaned_data = cleaned_data[cleaned_data["fare_amount"] > 0]

    # drop columns
    cleaned_data = cleaned_data.drop(columns = ['ratecodeid', 'store_and_fwd_flag','pulocationid', 'dolocationid', 
                                                'payment_type', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
                                                'improvement_surcharge','congestion_surcharge', 'airport_fee'])

    # rename columns
    cleaned_data = cleaned_data.rename(columns={'tpep_pickup_datetime':'pickup_time',
                                                'tpep_dropoff_datetime':'dropoff_time'
                                                })

    context.log.info(f"Processing {len(downloaded_trip)} rows of cleaned data.")

    return Output(
        value=cleaned_data,  # Or some processed version of it
        metadata={
            "clean_count": MetadataValue.int(len(cleaned_data)),
            "unclean_count": MetadataValue.int(
                len(downloaded_trip) - len(cleaned_data)
            ),
        },
    )


@asset(
    ins={"clean_data": AssetIn("clean_trip")},
    partitions_def=MonthlyPartitionsDefinition(start_date="2019-01-01"),
)
def transform_data(context, clean_data: pd.DataFrame) -> Output:
    """Clean the downlaoded data"""
    clean_data["trip_duration_in_seconds"] = (
        clean_data["dropoff_time"] - clean_data["pickup_time"]
    ).dt.total_seconds()
    clean_data["average_speed_miles_per_second"] = clean_data["trip_distance"] / clean_data[
        "trip_duration_in_seconds"
    ]
    return Output(
        value=clean_data,  # Or some processed version of it
        metadata={
            "description": MetadataValue.text(
                "New column trip_duration_in_seconds and average_speed_miles_per_second"
            )
        },
    )

@asset(
    ins={"featured_data": AssetIn("transform_data")},
    partitions_def=MonthlyPartitionsDefinition(start_date="2019-01-01"),
)
def daily_aggregation(context, featured_data: pd.DataFrame) -> Output:
    """Clean the downlaoded data"""

    featured_data.set_index("pickup_time", inplace=True)

    daily_aggregation = (
        featured_data.resample("D")
        .agg(
            total_trips=("vendorid", "count"), average_fare=("fare_amount", "mean")
        )
        .reset_index()
    )
    taxi_daily_aggregation = daily_aggregation.rename_axis("trip_date", axis=0)

    return Output(
        value=taxi_daily_aggregation,  # Or some processed version of it
        metadata={"description": MetadataValue.text("Aggregated Data on daily level")},
    )

# Dump clean data
@asset(
    ins={"featured_data": AssetIn("transform_data")},
    partitions_def=MonthlyPartitionsDefinition(start_date="2019-01-01"),
    required_resource_keys={"sqlite"}
)
def dump_transformed_data(context, featured_data: pd.DataFrame):
    """Clean the downlaoded data"""
    sqlite = context.resources.sqlite
    featured_data.set_index('pickup_time', inplace=True)
    sqlite.write_to_db(featured_data, "trips", 'append', True)

# Dump Aggrgate data
@asset(
    deps=[dump_transformed_data],
    ins={"aggrgated_data": AssetIn("daily_aggregation")},
    partitions_def=MonthlyPartitionsDefinition(start_date="2019-01-01"),
    required_resource_keys={"sqlite"}
)
def dump_aggregated_data(context, aggrgated_data: pd.DataFrame):
    """Clean the downlaoded data"""
    sqlite = context.resources.sqlite
    sqlite.write_to_db(aggrgated_data, "daily_aggregated_trips", 'append', True)


taxi_job = define_asset_job(
    "taxi_trips",
    selection=[download_trip, clean_trip, transform_data, daily_aggregation, dump_transformed_data, dump_aggregated_data],
    #required_resource_keys={"sqlite"}
)

taxi_job_schedule = build_schedule_from_partitioned_job(taxi_job)


defs = Definitions(
    assets=[download_trip, clean_trip, transform_data, daily_aggregation, dump_transformed_data, dump_aggregated_data],
    schedules=[taxi_job_schedule],
    resources={'sqlite':sqlite_resource}
    )