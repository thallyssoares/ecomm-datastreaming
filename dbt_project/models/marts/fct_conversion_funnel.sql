{{
    config(
        materialized='table',
        partition_by={
            "field": "funnel_date",
            "data_type": "date",
            "granularity": "day"
        },
        labels={"layer": "gold", "domain": "funnel"},
        description="Métricas do funil de conversão diário"
    )
}}

-- ============================================================
-- GOLD: fct_conversion_funnel
-- Funil: page_view → view_item → add_to_cart → purchase
-- Calcula taxas de conversão entre cada etapa
-- ============================================================

with daily_events as (
    select
        date(event_timestamp) as funnel_date,
        event_type,
        count(*) as event_count,
        count(distinct user_id) as unique_users
    from {{ ref('int_enriched_clickstream') }}
    where event_type in ('page_view', 'view_item', 'add_to_cart', 'purchase')
    group by funnel_date, event_type
),

pivoted as (
    select
        funnel_date,
        coalesce(sum(case when event_type = 'page_view' then event_count end), 0) as page_views,
        coalesce(sum(case when event_type = 'view_item' then event_count end), 0) as view_items,
        coalesce(sum(case when event_type = 'add_to_cart' then event_count end), 0) as add_to_carts,
        coalesce(sum(case when event_type = 'purchase' then event_count end), 0) as purchases,
        coalesce(sum(case when event_type = 'page_view' then unique_users end), 0) as unique_page_viewers,
        coalesce(sum(case when event_type = 'view_item' then unique_users end), 0) as unique_viewers,
        coalesce(sum(case when event_type = 'add_to_cart' then unique_users end), 0) as unique_cart_adders,
        coalesce(sum(case when event_type = 'purchase' then unique_users end), 0) as unique_buyers
    from daily_events
    group by funnel_date
)

select
    funnel_date,
    page_views,
    view_items,
    add_to_carts,
    purchases,
    -- Taxas de conversão por etapa (baseado em usuários únicos)
    safe_divide(unique_viewers, nullif(unique_page_viewers, 0)) * 100 as conversion_rate_view,
    safe_divide(unique_cart_adders, nullif(unique_viewers, 0)) * 100 as conversion_rate_cart,
    safe_divide(unique_buyers, nullif(unique_cart_adders, 0)) * 100 as conversion_rate_purchase,
    -- Taxa geral
    safe_divide(unique_buyers, nullif(unique_page_viewers, 0)) * 100 as overall_conversion_rate,
    current_timestamp() as updated_at
from pivoted
