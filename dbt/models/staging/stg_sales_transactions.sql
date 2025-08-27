-- models/staging/stg_sales_transactions.sql
{{ config(
    description="Cleaned and standardized sales transactions",
    materialized='view'
) }}

SELECT 
    -- Identifiers
    transaction_id,
    customer_id,
    store_id,
    product_id,
    
    -- Transaction details
    quantity,
    CAST(unit_price AS NUMERIC) AS unit_price,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(discount_amount AS NUMERIC) AS discount_amount,
    payment_method,
    
    -- Timestamps
    transaction_timestamp,
    DATE(transaction_timestamp) AS transaction_date,
    EXTRACT(HOUR FROM transaction_timestamp) AS transaction_hour,
    EXTRACT(DAYOFWEEK FROM transaction_timestamp) AS day_of_week,
    
    -- Calculated fields
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM transaction_timestamp) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    
    CASE 
        WHEN EXTRACT(HOUR FROM transaction_timestamp) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM transaction_timestamp) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM transaction_timestamp) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,
    
    -- Revenue calculations
    total_amount - discount_amount AS net_revenue,
    CASE 
        WHEN discount_amount > 0 THEN discount_amount / total_amount 
        ELSE 0 
    END AS discount_rate,
    
    -- Metadata
    created_at,
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ source('raw_data', 'sales_transactions') }}
WHERE transaction_timestamp >= '{{ var("start_date") }}'
  AND transaction_timestamp < '{{ var("end_date") }}'
  AND quantity > 0
  AND total_amount > 0