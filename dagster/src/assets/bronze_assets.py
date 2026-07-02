"""
Assets para monitoramento da camada Bronze (streaming)

Verifica saúde do streaming: volume, latência, freshness.
"""

from google.cloud import bigquery

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from ..resources import bigquery_resource


@asset(
    description="Verifica saúde da tabela bronze.raw_clickstream",
    compute_kind="bigquery",
    group_name="bronze_monitoring",
)
def bronze_health_check(
    context: AssetExecutionContext, bigquery: bigquery.Client
) -> MaterializeResult:
    """
    Verificações de saúde do Bronze:
    - Volume de eventos na última hora
    - Latência média (processing_timestamp - event_timestamp)
    - Freshness (último evento)
    """
    project_id = bigquery.project

    query = f"""
    with stats as (
        select
            count(*) as events_last_hour,
            count(distinct user_id) as unique_users,
            avg(timestamp_diff(processing_timestamp, event_timestamp, second)) as avg_latency_seconds,
            max(event_timestamp) as latest_event,
            min(event_timestamp) as earliest_event
        from `{project_id}.bronze.raw_clickstream`
        where event_timestamp >= timestamp_sub(current_timestamp(), interval 1 hour)
    )
    select * from stats
    """

    result = bigquery.query(query).result()
    row = list(result)[0]

    events = row.events_last_hour
    users = row.unique_users
    latency = row.avg_latency_seconds or 0
    latest = row.latest_event

    # Alertas
    warnings = []
    if events < 100:
        warnings.append(f"Volume baixo: {events} eventos/hora (esperado > 100)")
    if latency > 60:
        warnings.append(f"Latência alta: {latency:.0f}s (esperado < 60s)")
    if latest and (
        latest.timestamp() < (context.instance.get_current_timestamp() - 300)
    ):
        warnings.append(f"Dados parados: último evento em {latest}")

    metadata = {
        "events_last_hour": events,
        "unique_users": users,
        "avg_latency_seconds": round(latency, 2),
        "latest_event": str(latest) if latest else "N/A",
        "warnings": warnings,
    }

    if warnings:
        context.log.warning(f"Bronze health warnings: {warnings}")

    return MaterializeResult(metadata=metadata)


@asset(
    description="Contagem de eventos por tipo na última hora",
    compute_kind="bigquery",
    group_name="bronze_monitoring",
    deps=[bronze_health_check],
)
def bronze_event_counts(
    context: AssetExecutionContext, bigquery: bigquery.Client
) -> MaterializeResult:
    """Distribuição de eventos por tipo para detectar anomalias no funil"""
    project_id = bigquery.project

    query = f"""
    select
        event_type,
        count(*) as count
    from `{project_id}.bronze.raw_clickstream`
    where event_timestamp >= timestamp_sub(current_timestamp(), interval 1 hour)
    group by event_type
    order by count desc
    """

    result = bigquery.query(query).result()
    rows = list(result)

    event_counts = {row.event_type: row.count for row in rows}

    # Calcula ratios do funil
    pv = event_counts.get("page_view", 0)
    vi = event_counts.get("view_item", 0)
    atc = event_counts.get("add_to_cart", 0)
    pur = event_counts.get("purchase", 0)

    metadata = {
        "event_counts": event_counts,
        "funnel_ratios": {
            "page_view_to_view_item": round(vi / pv * 100, 2) if pv else 0,
            "view_item_to_add_to_cart": round(atc / vi * 100, 2) if vi else 0,
            "add_to_cart_to_purchase": round(pur / atc * 100, 2) if atc else 0,
        },
    }

    return MaterializeResult(metadata=metadata)
