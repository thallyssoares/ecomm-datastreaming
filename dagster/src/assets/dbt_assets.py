"""
Dagster Assets para integração com dbt.

Usa dagster-dbt para materializar models do dbt como Software-Defined Assets.
"""

from dagster_dbt import DbtCliResource, dbt_assets

from dagster import AssetExecutionContext

from ..resources import bigquery_resource


# Assets do dbt - carrega todo o projeto dbt como assets
@dbt_assets(
    manifest="../dbt_project/target/manifest.json",
    project_dir="../dbt_project",
    profiles_dir="../dbt_project",
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Materializa todos os models do dbt como assets do Dagster.

    Cada model vira um asset com:
    - Dependências automáticas (ref/source)
    - Metadata de colunas, testes, documentação
    - Integração com Soda checks
    """
    yield from dbt.cli(["build"], context=context).stream()


# Asset adicional: monitoramento da Bronze
import pandas as pd

from dagster import AssetIn, MetadataValue, asset


@asset(
    deps=[dbt_analytics],
    description="Verifica freshness dos dados na Bronze",
    group_name="monitoring",
)
def bronze_freshness_check(context, bigquery: BigQueryResource):
    """Checa se dados estão chegando no Bronze (alerting se stale > 30min)"""
    client = bigquery.get_client()

    query = f"""
        select
            max(event_timestamp) as last_event,
            timestamp_diff(current_timestamp(), max(event_timestamp), minute) as minutes_stale,
            count(*) as events_last_hour
        from `{bigquery.project_id}.bronze.raw_clickstream`
        where event_timestamp >= timestamp_sub(current_timestamp(), interval 1 hour)
    """

    result = client.query(query).result()
    row = next(result)

    context.add_output_metadata(
        {
            "last_event": MetadataValue.text(str(row.last_event)),
            "minutes_stale": MetadataValue.int(row.minutes_stale),
            "events_last_hour": MetadataValue.int(row.events_last_hour),
        }
    )

    if row.minutes_stale > 30:
        raise Exception(
            f"⚠️ Bronze stale: último evento há {row.minutes_stale} minutos!"
        )

    return {"status": "healthy", "minutes_stale": row.minutes_stale}
