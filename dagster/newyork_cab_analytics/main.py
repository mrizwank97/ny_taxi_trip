from dagster import Definitions
from mongo_resource.mongo_resource import mongo_resource
from db_resource.db_resource import pandas_resource
from pipelines.mongo_pipeline import mongo_partitioned_job
from schedules.schedules import daily_schedule  # Import your schedules

# Define the Dagster repository
defs = Definitions(
    resources={
        "mongo": mongo_resource,
        "db": pandas_resource
    },
    jobs=[mongo_partitioned_job],
    schedules=[daily_schedule],  # Include your schedules here
)
