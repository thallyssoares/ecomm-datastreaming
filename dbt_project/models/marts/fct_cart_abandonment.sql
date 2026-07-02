{{
    config(
        materialized='table',
        partition_by={
            "field": "analysis_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["category_id", "analysis_date"],
        labels={"layer": "gold", "domain": "abandonment"},
        description="Taxa de abandono de carrinho por categoria de produto"
    )
}}

-- ============================================================
-- GOLD: fct_cart_abandonment
-- Abandono de carrinho por categoria
-- Lógica: add_to_cart sem purchase correspondente (mesmo product_id + user_id)
-- Janela: analisa carrinhos adicionados no dia, verifica purchase até fim do dia
-- ============================================================

with cart_events as (
    select
        date(event_timestamp) as analysis_date,
        category_id,
        category_name,
        user_id,
        product_id,
        event_timestamp as cart_at,
        price
    from {{ ref('int_enriched_clickstream') }}
    where event_type = 'add_to_cart'
      and product_id is not null
      and category_id is not null
),

purchase_events as (
    select
        user_id,
        product_id,
        event_timestamp as purchase_at
    from {{ ref('int_enriched_clickstream') }}
    where event_type = 'purchase'
      and product_id is not null
),

-- Match: carrinho que virou compra (mesmo user + product, purchase depois do cart)
cart_to_purchase as (
    select
        c.analysis_date,
        c.category_id,
        c.category_name,
        c.user_id,
        c.product_id,
        c.cart_at,
        p.purchase_at,
        c.price
    from cart_events c
    inner join purchase_events p
        on c.user_id = p.user_id
        and c.product_id = p.product_id
        and p.purchase_at >= c.cart_at
        and p.purchase_at < timestamp_add(c.cart_at, interval 24 hour)
),

-- Agrega por categoria + dia
cart_agg as (
    select
        analysis_date,
        category_id,
        category_name,
        count(*) as cart_additions,
        sum(price) as cart_value
    from cart_events
    group by analysis_date, category_id, category_name
),

purchase_agg as (
    select
        analysis_date,
        category_id,
        count(*) as completed_purchases,
        sum(price) as purchased_value
    from cart_to_purchase
    group by analysis_date, category_id
),

final as (
    select
        c.analysis_date,
        c.category_id,
        c.category_name,
        c.cart_additions,
        coalesce(p.completed_purchases, 0) as completed_purchases,
        (c.cart_additions - coalesce(p.completed_purchases, 0)) as abandoned_carts,
        c.cart_value,
        coalesce(p.purchased_value, 0) as revenue_realized,
        (c.cart_value - coalesce(p.purchased_value, 0)) as revenue_lost,
        -- Taxa de abandono
        safe_divide(c.cart_additions - coalesce(p.completed_purchases, 0), nullif(c.cart_additions, 0)) * 100 as abandonment_rate
    from cart_agg c
    left join purchase_agg p
        on c.analysis_date = p.analysis_date
        and c.category_id = p.category_id
)

select
    analysis_date,
    category_id,
    category_name,
    cart_additions,
    completed_purchases,
    abandoned_carts,
    cart_value,
    revenue_realized,
    revenue_lost,
    abandonment_rate,
    current_timestamp() as updated_at
from final
