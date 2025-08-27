-- models/warehouse/dimensions/dim_products.sql
{{ config(
    description="Product dimension with hierarchy and pricing history",
    materialized='table',
    unique_key='product_key'
) }}

SELECT 
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['product_id', 'selling_price', 'unit_cost']) }} AS product_key,
    
    -- Natural key
    product_id,
    supplier_id,
    
    -- Product hierarchy
    category,
    subcategory,
    brand,
    product_name,
    
    -- Pricing (current)
    unit_cost,
    selling_price,
    gross_margin,
    margin_percentage,
    
    -- Product classification
    price_tier,
    is_seasonal,
    
    -- Product metrics
    CASE 
        WHEN margin_percentage >= 50 THEN 'High Margin'
        WHEN margin_percentage >= 30 THEN 'Medium Margin'
        ELSE 'Low Margin'
    END AS margin_category,
    
    -- Metadata
    created_at,
    dbt_updated_at,
    TRUE AS is_current

FROM {{ ref('stg_products') }}