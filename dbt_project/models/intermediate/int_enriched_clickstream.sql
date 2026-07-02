{{
    config(
        materialized='table',
        partition_by={
            "field": "event_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["user_id", "event_type", "category_id"],
        labels={"layer": "silver", "domain": "clickstream_enriched"},
        description="Clickstream enriquecido com dados de produtos (JOIN dim_products)"
    )
}}

-- ============================================================
-- INTERMEDIATE: int_enriched_clickstream
-- Junta stg_clickstream com stg_products para enriquecer eventos
-- com dados mestres de produto (categoria oficial, preço oficial, etc)
-- ============================================================

with clickstream as (
    select *
    from {{ ref('stg_clickstream') }}
),

products as (
    select
        product_id,
        category_id as product_category_id,
        category_name as product_category_name,
        brand as product_brand,
        price as product_price,
        currency as product_currency
    from {{ ref('stg_products') }}
)

select
    c.event_id,
    c.user_id,
    c.session_id,
    c.event_type,
    c.event_timestamp,
    c.processing_timestamp,

    -- Produto: prioriza dados do catálogo (dados mestres)
    coalesce(c.product_id, p.product_id) as product_id,
    coalesce(c.category_id, p.product_category_id) as category_id,
    coalesce(c.category_name, p.product_category_name) as category_name,
    coalesce(c.brand, p.product_brand) as brand,
    coalesce(c.price, p.product_price) as price,
    coalesce(c.currency, p.product_currency) as currency,

    -- Contexto da navegação
    c.page_url,
    c.referrer_url,
    c.user_agent,
    c.ip_country,
    c.device_type,
    c.os,
    c.browser,

    -- Metadata
    c.dbt_processed_at,
    c.dbt_invocation_id

from clickstream c
left join products p
    on c.product_id = p.product_id
