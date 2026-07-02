{% macro lookback_window(hours=2) %}
    -- Macro para janela de lookback dinâmica
    -- Usa var('lookback_hours', 2) como padrão
    timestamp_sub(
        timestamp('{{ var("run_started_at", run_started_at) }}'),
        interval {{ var('lookback_hours', hours) }} hour
    )
{% endmacro %}

{% macro lookback_window_end(minutes=30) %}
    -- Fim da janela (exclui últimos 30 min para dar tempo de arrival)
    timestamp_sub(
        timestamp('{{ var("run_started_at", run_started_at) }}'),
        interval {{ minutes }} minute
    )
{% endmacro %}

{% macro partition_by_day(field='event_timestamp') %}
    partition_by={
        "field": "{{ field }}",
        "data_type": "timestamp",
        "granularity": "day"
    }
{% endmacro %}

{% macro cluster_by_user_event() %}
    cluster_by=["user_id", "event_type"]
{% endmacro %}

{% macro silver_labels() %}
    labels={"layer": "silver", "domain": "clickstream"}
{% endmacro %}

{% macro gold_labels(domain='analytics') %}
    labels={"layer": "gold", "domain": "{{ domain }}"}
{% endmacro %}
