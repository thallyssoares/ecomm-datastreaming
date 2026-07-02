"""
Dagster Jobs - Orquestração dos assets
"""

from dagster import AssetKey, AssetSelection, define_asset_job

# Job principal: dbt build (Silver + Gold)
hourly_dbt_job = define_asset_job(
    name="hourly_dbt_job",
    description="Executa dbt build para materializar Silver e Gold",
    selection=AssetSelection.keys(
        AssetKey("stg_clickstream"),
        AssetKey("stg_products"),
        AssetKey("int_enriched_clickstream"),
        AssetKey("fct_daily_revenue"),
        AssetKey("fct_conversion_funnel"),
        AssetKey("fct_cart_abandonment"),
    ),
    tags={"layer": "transform", "team": "data-engineering"},
)


# Job de monitoramento do Bronze
bronze_monitoring_job = define_asset_job(
    name="bronze_monitoring_job",
    description="Verifica saúde do streaming (freshness, volume, latência)",
    selection=AssetSelection.keys(
        AssetKey("bronze_freshness_check"),
    ),
    tags={"layer": "monitoring", "team": "data-engineering"},
)


# Job completo (seeds + dbt + monitoring)
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    description="Pipeline completo: seeds -> dbt -> monitoring",
    selection=AssetSelection.all(),
    tags={"layer": "full", "team": "data-engineering"},
)
