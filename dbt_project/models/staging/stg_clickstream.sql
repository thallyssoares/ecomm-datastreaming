{{
    config(
        materialized='table',
        partition_by={
            "field": "event_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["user_id", "event_type"],
        labels={"layer": "silver", "domain": "clickstream"},
        description="Eventos de clickstream limpos, tipados e deduplicados (lookback 1h30)"
    )
}}

-- ============================================================
-- SILVER: stg_clickstream
-- Limpa a tabela bronze.raw_clickstream com:
-- 1. Lookback window (1h30) para capturar late arrivals
-- 2. Deduplicação por event_id (idempotência)
-- 3. Tipagem explícita e validação
-- 4. Enriquecimento com dim_products (JOIN left)
-- ============================================================

with raw as (
    select
        event_id,
        user_id,
        session_id,
        event_type,
        -- Garante timestamp correto
        timestamp(event_timestamp) as event_timestamp,
        timestamp(processing_timestamp) as processing_timestamp,
        product_id,
        category_id,
        category_name,
        brand,
        cast(price as float64) as price,
        currency,
        page_url,
        referrer_url,
        user_agent,
        ip_country,
        device_type,
        os,
        browser,
        -- Metadata de processamento
        current_timestamp() as dbt_processed_at,
        '{{ invocation_id }}' as dbt_invocation_id
    from {{ source('bronze', 'raw_clickstream') }}
    -- LOOKBACK WINDOW: processa janela nominal + 30min buffer para late arrivals
    where event_timestamp >= timestamp_sub(
            timestamp('{{ var("run_started_at", run_started_at) }}'),
            interval 1 hour 30 minute
        )
      and event_timestamp < timestamp_sub(
            timestamp('{{ var("run_started_at", run_started_at) }}'),
            interval 30 minute
        )
),

deduped as (
    -- DEDUPLICAÇÃO: pega o registro mais recente por event_id
    -- (pode aparecer em 2 runs consecutivos por causa do overlap de 30min)
    select *
    from (
        select
            *,
            row_number() over (
                partition by event_id
                order by processing_timestamp desc
            ) as rn
        from raw
    )
    where rn = 1
),

validated as (
    -- VALIDAÇÃO: remove eventos com campos obrigatórios nulos
    select *
    from deduped
    where event_id is not null
      and user_id is not null
      and event_type in ('page_view', 'view_item', 'add_to_cart', 'purchase')
      and event_timestamp is not null
)

select
    event_id,
    user_id,
    session_id,
    event_type,
    event_timestamp,
    processing_timestamp,
    product_id,
    category_id,
    category_name,
    brand,
    price,
    currency,
    page_url,
    referrer_url,
    user_agent,
    ip_country,
    device_type,
    os,
    browser,
    dbt_processed_at,
    dbt_invocation_id
from validated
