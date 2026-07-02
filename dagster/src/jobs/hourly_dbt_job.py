"""
Dagster Jobs para orquestração do pipeline

Jobs definem a ordem de execução dos assets.
"""

from dagster import AssetSelection, define_asset_job

from ..assets import (
    bronze_event_counts,
    bronze_health_check,
    intermediate_assets,
    marts_assets,
    seed_assets,
    staging_assets,
)

# Job principal: roda dbt a cada hora (Silver -> Gold)
hourly_dbt_job = define_asset_job(
    name="hourly_dbt_job",
    description="Executa dbt para atualizar Silver e Gold (agendado hourly)",
    selection=AssetSelection.groups("staging", "intermediate", "marts"),
    tags={"team": "data-engineering", "layer": "transform"},
)

# Job para rodar seeds (menos frequente)
seed_job = define_asset_job(
    name="seed_job",
    description="Atualiza seeds do dbt (dim_products)",
    selection=AssetSelection.groups("seeds"),
    tags={"team": "data-engineering", "layer": "seed"},
)

# Job de monitoramento do Bronze
bronze_monitoring_job = define_asset_job(
    name="bronze_monitoring_job",
    description="Verifica saúde do streaming Bronze",
    selection=AssetSelection.groups("bronze_monitoring"),
    tags={"team": "data-engineering", "layer": "monitoring"},
)

# Job completo: seeds + dbt + monitoring
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    description="Pipeline completo: seeds -> dbt -> monitoring",
    selection=AssetSelection.all(),
    tags={"team": "data-engineering", "pipeline": "full"},
)
