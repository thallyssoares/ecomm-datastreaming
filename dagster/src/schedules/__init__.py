"""
Dagster Schedules - Agendamento dos jobs
"""

from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import bronze_monitoring_job, hourly_dbt_job

# Schedule principal: roda dbt a cada hora (atualiza Silver/Gold)
hourly_dbt_schedule = ScheduleDefinition(
    job=hourly_dbt_job,
    cron_schedule="0 * * * *",  # A cada hora, minuto 0
    default_status=DefaultScheduleStatus.RUNNING,
    description="Atualiza camadas Silver e Gold via dbt a cada hora",
)


# Schedule de monitoramento: roda a cada 15 min (checa saúde do Bronze)
bronze_monitoring_schedule = ScheduleDefinition(
    job=bronze_monitoring_job,
    cron_schedule="*/15 * * * *",  # A cada 15 minutos
    default_status=DefaultScheduleStatus.RUNNING,
    description="Monitora saúde do streaming Bronze (freshness, volume, latência)",
)
