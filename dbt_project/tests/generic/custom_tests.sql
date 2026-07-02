{% test valid_event_type(model, column_name) %}
    -- Testa se event_type está no conjunto válido do funil
    select *
    from {{ model }}
    where {{ column_name }} not in ('page_view', 'view_item', 'add_to_cart', 'purchase')
{% endtest %}

{% test positive_price(model, column_name) %}
    -- Testa se price é positivo (quando não nulo)
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} <= 0
{% endtest %}

{% test session_consistency(model, user_column, session_column, timestamp_column) %}
    -- Testa se session_id é consistente dentro do mesmo user (não muda no meio da sessão)
    -- Nota: teste mais complexo, pode ser lento em tabelas grandes
    with session_changes as (
        select
            {{ user_column }},
            {{ session_column }},
            count(distinct {{ session_column }}) as session_count
        from {{ model }}
        group by {{ user_column }}, {{ session_column }}
        having count(distinct {{ session_column }}) > 1
    )
    select *
    from session_changes
{% endtest %}
