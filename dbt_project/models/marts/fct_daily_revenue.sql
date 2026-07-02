{{
    config(
        materialized='table',
        partition_by={
            "field": "revenue_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["revenue_date"],
        labels={"layer": "gold", "domain": "revenue"},
        description="Faturamento diário agregado para dashboard executivo"
    )
}}

-- ============================================================
-- GOLD: fct_daily_revenue
-- Faturamento bruto acumulado do dia (minuto a minuto via Dagster hourly)
-- Base: int_enriched_clickstream filtrado por event_type = 'purchase'
-- ============================================================

with purchases as (
    select
        date(event_timestamp) as revenue_date,
        user_id,
        event_id,
        price
    from {{ ref('int_enriched_clickstream') }}
    where event_type = 'purchase'
      and price is not null
      and price > 0
)

select
    revenue_date,
    sum(price) as gross_revenue,
    count(distinct event_id) as orders_count,
    count(distinct user_id) as unique_buyers,
    sum(price) / nullif(count(distinct event_id), 0) as avg_order_value,
    current_timestamp() as updated_at
from purchases
group by revenue_date
