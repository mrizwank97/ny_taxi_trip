from dagster import (
    MonthlyPartitionsDefinition,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
    Definitions,
    RetryPolicy,
    Output,
    MetadataValue,
    AssetIn,
    multiprocess_executor
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

    format_string = "%Y-%m-%d"
    date_object = datetime.strptime(context.partition_key, format_string)
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date_object.strftime('%Y-%m')}.parquet"
    context.log.info(f"URL : {url}")

    target_file = f"yellow_tripdata_{date_object}.parquet"
    download_file(context, url, target_file)
    data = pd.read_parquet(target_file)

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
    cleaned_data = cleaned_data[cleaned_data["passenger_count"] > 0]
    cleaned_data = cleaned_data[
        (cleaned_data["tpep_dropoff_datetime"] - cleaned_data["tpep_pickup_datetime"])
        > timedelta(seconds=0)
    ]

    cleaned_data = cleaned_data[cleaned_data["trip_distance"] > 0]
    cleaned_data = cleaned_data[cleaned_data["fare_amount"] > 0]

    cleaned_data = cleaned_data.drop(columns=['ratecodeid', 'store_and_fwd_flag','pulocationid', 'dolocationid', 
                                              'payment_type', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
                                              'improvement_surcharge','congestion_surcharge', 'airport_fee'])

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

    clean_data["trip_duration_in_seconds"] = (clean_data["dropoff_time"] - clean_data["pickup_time"]).dt.total_seconds()
    clean_data["trip_duration_in_minutes"] = clean_data["trip_duration_in_seconds"] / 60
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
def aggregate_data(context, featured_data: pd.DataFrame) -> Output:

    featured_data["pickup_year"] = featured_data["pickup_time"].dt.year
    featured_data["pickup_month"] = featured_data["pickup_time"].dt.month
    featured_data["pickup_day"] = featured_data["pickup_time"].dt.day
    featured_data["pickup_hour"] = featured_data["pickup_time"].dt.hour

    aggregated_data = (
        featured_data.groupby(["pickup_year", "pickup_month", "pickup_day", "pickup_hour", "passenger_count"])
        .agg(
            no_of_trips=("vendorid", "count"),
            avg_fare_amount=("fare_amount", "mean"),
            avg_total_amount=("total_amount", "mean"),
            avg_trip_distance=("trip_distance", "mean"),
            avg_speed_mps=("average_speed_miles_per_second", "mean"),
            avg_trip_duration_minutes=("trip_duration_in_minutes", "mean"),
        )
        .reset_index()
    )

    return Output(
        value=aggregated_data,  # Or some processed version of it
        metadata={"description": MetadataValue.text("Data Aggregated on year, month, day, hour and passenger count level")},
    )

@asset(
    ins={"aggrgated_data": AssetIn("aggregate_data")},
    partitions_def=MonthlyPartitionsDefinition(start_date="2019-01-01"),
    required_resource_keys={"sqlite"}
)
def dump_aggregated_data(context, aggrgated_data: pd.DataFrame):
    """Clean the downlaoded data"""
    sqlite = context.resources.sqlite
    sqlite.write_to_db(aggrgated_data, "aggregated_trips", 'append', True)

taxi_job = define_asset_job(
    name="yellow_cab_trips",
    selection=[download_trip, clean_trip, transform_data, aggregate_data, dump_aggregated_data],
    executor_def=multiprocess_executor.configured({"max_concurrent": 1})
)

taxi_job_schedule = build_schedule_from_partitioned_job(
    taxi_job,
    name="trip_download_schedule"
    )

defs = Definitions(
    assets=[download_trip, clean_trip, transform_data, aggregate_data, dump_aggregated_data],
    jobs=[taxi_job],
    schedules=[taxi_job_schedule],
    resources={'sqlite':sqlite_resource}
    )