from dagster_dbt import DbtCliResource, dbt_assets

from dagster import Definitions, load_assets_from_modules

from .assets import dbt_assets as dbt_assets_module
from .jobs import bronze_monitoring_job, full_pipeline_job, hourly_dbt_job, seed_job
from .resources import bigquery_resource, soda_resource
from .schedules import daily_seed_schedule, hourly_dbt_schedule
from .sensors import bronze_freshness_sensor

# Carrega assets do dbt
dbt_assets = load_assets_from_modules([dbt_assets_module])


# Definições principais do Dagster
defs = Definitions(
    assets=dbt_assets,
    jobs=[
        hourly_dbt_job,
        seed_job,
        bronze_monitoring_job,
        full_pipeline_job,
    ],
    schedules=[
        hourly_dbt_schedule,
        daily_seed_schedule,
    ],
    sensors=[
        bronze_freshness_sensor,
    ],
    resources={
        "bigquery": bigquery_resource,
        "soda": soda_resource,
        "dbt": DbtCliResource(
            project_dir="../dbt_project",
            profiles_dir="../dbt_project",
        ),
    },
)
