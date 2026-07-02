{{
    config(
        materialized='table',
        cluster_by=["category_id", "brand"],
        labels={"layer": "silver", "domain": "catalog"},
        description="Dimensão de produtos carregada via seed CSV"
    )
}}

-- ============================================================
-- SILVER: stg_products
-- Wrapper sobre a seed dim_products para:
-- 1. Tipagem explícita
-- 2. Validações (testes no schema.yml)
-- 3. Padronização de nomes de colunas
-- ============================================================

select
    product_id,
    category_id,
    category_name,
    brand,
    cast(price as float64) as price,
    currency,
    is_active,
    timestamp(created_at) as created_at,
    timestamp(updated_at) as updated_at,
    current_timestamp() as dbt_processed_at
from {{ ref('dim_products') }}
where is_active = true
