{{ config(
    description="Current inventory fact table with reorder metrics",
    materialized='table',
    cluster_by=["store_id", "stock_status"]
) }}

WITH inventory_with_keys AS (
    SELECT 
        i.*,
        p.product_key,
        {{ dbt_utils.generate_surrogate_key(['i.store_id', 'i.product_id', 'i.snapshot_timestamp']) }} AS inventory_key
    FROM {{ ref('stg_inventory_levels') }} i
    LEFT JOIN {{ ref('dim_products') }} p 
        ON i.product_id = p.product_id 
        AND p.is_current = TRUE
)

SELECT 
    inventory_key,
    product_key,
    store_id,
    product_id,  -- Keep original ID for reference
    
    -- Inventory levels
    current_stock,
    reorder_point,
    max_stock,
    available_capacity,
    
    -- Metrics
    stock_fill_rate,
    suggested_reorder_quantity,
    days_since_restock,
    
    -- Status flags
    stock_status,
    needs_reorder,
    
    -- Dates
    last_restock_date,
    snapshot_timestamp,
    
    -- Business metrics
    CASE 
        WHEN stock_status = 'Out of Stock' THEN current_stock * -1
        WHEN stock_status = 'Needs Reorder' THEN reorder_point - current_stock
        ELSE 0
    END AS stock_deficit,
    
    CASE 
        WHEN stock_status = 'Overstocked' THEN current_stock - (max_stock * 0.9)
        ELSE 0
    END AS excess_stock,
    
    dbt_updated_at

FROM inventory_with_keys