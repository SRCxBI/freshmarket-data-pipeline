-- models/staging/stg_supply_orders.sql
{{ config(
    description="Supply orders with delivery tracking",
    materialized='view'
) }}

SELECT 
    -- Identifiers
    order_id,
    supplier_id,
    product_id,
    
    -- Order details
    quantity_ordered,
    CAST(unit_cost AS NUMERIC) AS unit_cost,
    quantity_ordered * CAST(unit_cost AS NUMERIC) AS total_order_value,
    
    -- Dates
    order_date,
    expected_delivery_date,
    
    -- Status
    status,
    
    -- Calculated fields
    DATE_DIFF(expected_delivery_date, order_date, DAY) AS lead_time_days,
    DATE_DIFF(CURRENT_DATE(), expected_delivery_date, DAY) AS days_overdue,
    
    CASE 
        WHEN status = 'Delivered' THEN 'On Time'
        WHEN CURRENT_DATE() > expected_delivery_date AND status != 'Delivered' THEN 'Overdue'
        WHEN CURRENT_DATE() <= expected_delivery_date AND status != 'Delivered' THEN 'On Track'
        ELSE 'Unknown'
    END AS delivery_status,
    
    CASE 
        WHEN order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'Recent'
        WHEN order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) THEN 'Current'
        ELSE 'Historical'
    END AS order_recency,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ source('raw_data', 'supply_orders') }}