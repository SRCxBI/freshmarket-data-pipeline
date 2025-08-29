{{ config(
    description="Inventory performance metrics by product and store",
    materialized='table'
) }}

WITH inventory_sales AS (
    SELECT 
        i.store_id,
        i.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.unit_cost,
        p.selling_price,
        p.margin_percentage,
        
        -- Current inventory metrics
        i.current_stock,
        i.reorder_point,
        i.max_stock,
        i.stock_status,
        i.needs_reorder,
        i.days_since_restock,
        i.stock_fill_rate,
        i.suggested_reorder_quantity,
        
        -- Sales performance (last 30 days)
        COALESCE(s.units_sold_30d, 0) AS units_sold_30d,
        COALESCE(s.revenue_30d, 0) AS revenue_30d,
        COALESCE(s.transactions_30d, 0) AS transactions_30d

    FROM {{ ref('fct_inventory') }} i
    LEFT JOIN {{ ref('dim_products') }} p 
        ON i.product_id = p.product_id 
        AND p.is_current = TRUE
    LEFT JOIN (
        SELECT 
            product_id,
            store_id,
            SUM(quantity) AS units_sold_30d,
            SUM(net_revenue) AS revenue_30d,
            COUNT(DISTINCT transaction_id) AS transactions_30d
        FROM {{ ref('fct_sales') }}
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY 1, 2
    ) s ON i.product_id = s.product_id AND i.store_id = s.store_id
),

inventory_metrics AS (
    SELECT 
        *,
        -- Inventory turnover calculations
        CASE 
            WHEN current_stock > 0 AND units_sold_30d > 0 
            THEN ROUND(units_sold_30d / (current_stock / 30.0), 2)
            ELSE 0 
        END AS daily_turnover_rate,
        
        CASE 
            WHEN units_sold_30d > 0 AND current_stock > 0
            THEN ROUND(current_stock / (units_sold_30d / 30.0), 1)
            ELSE 999
        END AS days_of_inventory,
        
        -- Sales velocity
        ROUND(units_sold_30d / 30.0, 2) AS avg_daily_sales,
        ROUND(revenue_30d / 30.0, 2) AS avg_daily_revenue,
        
        -- Inventory value
        ROUND(current_stock * unit_cost, 2) AS inventory_cost_value,
        ROUND(current_stock * selling_price, 2) AS inventory_retail_value,
        
        -- Performance indicators
        CASE 
            WHEN stock_status = 'Out of Stock' AND units_sold_30d > 0 THEN 'Stockout Risk'
            WHEN current_stock <= reorder_point AND units_sold_30d > 0 THEN 'Reorder Alert'
            WHEN units_sold_30d = 0 AND current_stock > (max_stock * 0.8) THEN 'Slow Moving'
            WHEN units_sold_30d > 0 AND current_stock > (max_stock * 0.9) THEN 'Overstock'
            WHEN units_sold_30d > (reorder_point * 2) AND current_stock < (max_stock * 0.3) THEN 'High Demand'
            ELSE 'Normal'
        END AS inventory_alert

    FROM inventory_sales
)

SELECT 
    store_id,
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    
    -- Pricing
    unit_cost,
    selling_price,
    margin_percentage,
    
    -- Inventory levels
    current_stock,
    reorder_point,
    max_stock,
    stock_status,
    needs_reorder,
    days_since_restock,
    stock_fill_rate,
    suggested_reorder_quantity,
    
    -- Sales performance
    units_sold_30d,
    revenue_30d,
    transactions_30d,
    avg_daily_sales,
    avg_daily_revenue,
    
    -- Inventory metrics
    daily_turnover_rate,
    days_of_inventory,
    inventory_cost_value,
    inventory_retail_value,
    inventory_alert,
    
    -- Classification
    CASE 
        WHEN daily_turnover_rate >= 0.1 THEN 'Fast Moving'
        WHEN daily_turnover_rate >= 0.05 THEN 'Medium Moving'
        WHEN daily_turnover_rate > 0 THEN 'Slow Moving'
        ELSE 'No Movement'
    END AS movement_category,
    
    CASE 
        WHEN inventory_retail_value >= 10000 THEN 'High Value'
        WHEN inventory_retail_value >= 5000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS inventory_value_tier,
    
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM inventory_metrics