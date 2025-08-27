{{ config(
    description="Daily sales performance across all stores",
    materialized='table'
) }}

SELECT 
    transaction_date,
    store_id,
    
    -- Transaction metrics
    COUNT(DISTINCT transaction_id) AS total_transactions,
    COUNT(DISTINCT customer_key) AS unique_customers,
    SUM(quantity) AS total_items_sold,
    
    -- Revenue metrics
    ROUND(SUM(total_amount), 2) AS gross_revenue,
    ROUND(SUM(discount_amount), 2) AS total_discounts,
    ROUND(SUM(net_revenue), 2) AS net_revenue,
    ROUND(AVG(net_revenue), 2) AS avg_transaction_value,
    
    -- Customer metrics
    ROUND(SUM(net_revenue) / NULLIF(COUNT(DISTINCT customer_key), 0), 2) AS revenue_per_customer,
    ROUND(SUM(quantity) / NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS items_per_transaction,
    
    -- Discount analysis
    ROUND(AVG(discount_rate) * 100, 2) AS avg_discount_rate,
    COUNT(CASE WHEN had_discount THEN 1 END) AS discounted_transactions,
    ROUND(COUNT(CASE WHEN had_discount THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS discount_penetration,
    
    -- Time-based metrics
    COUNT(CASE WHEN time_of_day = 'Morning' THEN 1 END) AS morning_transactions,
    COUNT(CASE WHEN time_of_day = 'Afternoon' THEN 1 END) AS afternoon_transactions,
    COUNT(CASE WHEN time_of_day = 'Evening' THEN 1 END) AS evening_transactions,
    
    -- Payment analysis
    COUNT(CASE WHEN payment_method = 'Cash' THEN 1 END) AS cash_transactions,
    COUNT(CASE WHEN payment_method = 'Credit Card' THEN 1 END) AS card_transactions,
    COUNT(CASE WHEN payment_method = 'Mobile Payment' THEN 1 END) AS mobile_transactions,
    
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ ref('fct_sales') }}
GROUP BY transaction_date, store_id
ORDER BY transaction_date DESC, store_id