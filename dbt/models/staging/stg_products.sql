-- models/staging/stg_products.sql
{{ config(
    description="Enhanced product catalog with calculated metrics",
    materialized='view'
) }}

SELECT 
    -- Identifiers
    product_id,
    supplier_id,
    
    -- Product details
    product_name,
    category,
    subcategory,
    brand,
    
    -- Pricing
    CAST(unit_cost AS NUMERIC) AS unit_cost,
    CAST(selling_price AS NUMERIC) AS selling_price,
    
    -- Calculated pricing metrics
    CAST(selling_price AS NUMERIC) - CAST(unit_cost AS NUMERIC) AS gross_margin,
    ROUND(
        (CAST(selling_price AS NUMERIC) - CAST(unit_cost AS NUMERIC)) / CAST(selling_price AS NUMERIC) * 100, 
        2
    ) AS margin_percentage,
    
    -- Product classification
    CASE 
        WHEN category IN ({{ "'" + var('seasonal_categories') | join("','") + "'" }}) THEN TRUE
        ELSE FALSE
    END AS is_seasonal,
    
    CASE 
        WHEN selling_price >= 200 THEN 'Premium'
        WHEN selling_price >= 100 THEN 'Mid-range'
        ELSE 'Budget'
    END AS price_tier,
    
    -- Metadata
    created_at,
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ source('raw_data', 'products') }}