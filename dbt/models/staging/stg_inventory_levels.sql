-- models/staging/stg_inventory_levels.sql
{{ config(
    description="Current inventory levels with reorder alerts",
    materialized='view'
) }}

SELECT 
    -- Identifiers
    store_id,
    product_id,
    
    -- Inventory levels
    current_stock,
    reorder_point,
    max_stock,
    
    -- Dates
    last_restock_date,
    snapshot_timestamp,
    
    -- Calculated fields
    DATE_DIFF(CURRENT_DATE(), last_restock_date, DAY) AS days_since_restock,
    
    CASE 
        WHEN current_stock <= 0 THEN 'Out of Stock'
        WHEN current_stock <= reorder_point THEN 'Needs Reorder'
        WHEN current_stock >= max_stock * 0.9 THEN 'Overstocked'
        ELSE 'Normal'
    END AS stock_status,
    
    ROUND(current_stock / NULLIF(max_stock, 0) * 100, 1) AS stock_fill_rate,
    
    max_stock - current_stock AS available_capacity,
    
    -- Reorder calculations
    GREATEST(reorder_point - current_stock, 0) AS suggested_reorder_quantity,
    
    CASE 
        WHEN current_stock <= reorder_point THEN TRUE
        ELSE FALSE
    END AS needs_reorder,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ source('raw_data', 'inventory_levels') }}