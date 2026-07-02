from datetime import datetime, timedelta

from google.cloud import bigquery

from dagster import RunRequest, SensorDefinition, SkipReason, sensor

from ..resources.bigquery_resource import bigquery_resource


@sensor(
    job_name="hourly_dbt_job",
    minimum_interval_seconds=300,  # Verifica a cada 5 min
    description="Sensor que detecta se chegou dados novos no Bronze e dispara dbt",
)
def bronze_freshness_sensor(context):
    """
    Sensor inteligente: só dispara dbt se houver dados novos no Bronze
    desde a última execução bem-sucedida.
    Evita rodar dbt vazio.
    """
    client = bigquery.Client(project=context.resources.bigquery.project)

    # Query: último event_timestamp no Bronze
    query = f"""
        select max(event_timestamp) as last_event
        from `{context.resources.bigquery.project}.bronze.raw_clickstream`
        where event_timestamp >= timestamp_sub(current_timestamp(), interval 2 hour)
    """

    result = client.query(query).result()
    row = next(result)

    if not row.last_event:
        return SkipReason("Nenhum evento recente no Bronze")

    last_event = row.last_event
    now = datetime.utcnow()

    # Se último evento tem mais de 10 min, pode ser problema no streaming
    if (now - last_event.replace(tzinfo=None)) > timedelta(minutes=10):
        context.log.warning(
            f"Bronze stale: último evento há {(now - last_event.replace(tzinfo=None)).seconds / 60:.0f} min"
        )

    # Verifica última execução do dbt (via cursor)
    last_dbt_run = context.cursor or "1970-01-01T00:00:00Z"

    if last_event.isoformat() > last_dbt_run:
        # Tem dados novos! Dispara run
        context.update_cursor(last_event.isoformat())
        return RunRequest(
            run_key=f"bronze_fresh_{last_event.isoformat()}",
            tags={"trigger": "bronze_freshness_sensor"},
        )

    return SkipReason("Nenhum dado novo desde última execução dbt")
