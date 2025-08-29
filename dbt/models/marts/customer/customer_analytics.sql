{{ config(
    description="Customer analytics with lifetime value and behavior metrics",
    materialized='table'
) }}

WITH customer_sales AS (
    SELECT 
        c.customer_id,
        c.customer_key,
        c.customer_segment,
        c.loyalty_tier,
        c.customer_tenure,
        c.days_since_registration,
        
        -- Sales metrics
        COUNT(DISTINCT s.transaction_id) AS total_transactions,
        COUNT(DISTINCT s.transaction_date) AS days_active,
        SUM(s.net_revenue) AS lifetime_value,
        AVG(s.net_revenue) AS avg_transaction_value,
        SUM(s.quantity) AS total_items_purchased,
        
        -- Recency metrics
        MAX(s.transaction_date) AS last_purchase_date,
        DATE_DIFF(CURRENT_DATE(), MAX(s.transaction_date), DAY) AS days_since_last_purchase,
        
        -- Frequency metrics
        COUNT(DISTINCT s.transaction_id) / GREATEST(COUNT(DISTINCT s.transaction_date), 1) AS transactions_per_active_day,
        
        -- Purchase behavior
        COUNT(CASE WHEN s.had_discount THEN 1 END) AS discounted_purchases,
        AVG(s.discount_rate) AS avg_discount_rate,
        
        -- Time-based patterns
        COUNT(CASE WHEN s.day_type = 'Weekend' THEN 1 END) AS weekend_purchases,
        COUNT(CASE WHEN s.time_of_day = 'Evening' THEN 1 END) AS evening_purchases

    FROM {{ ref('dim_customers') }} c
    LEFT JOIN {{ ref('fct_sales') }} s 
        ON c.customer_key = s.customer_key
    WHERE c.is_current = TRUE
    GROUP BY 1,2,3,4,5,6
),

customer_segments AS (
    SELECT 
        *,
        -- RFM Analysis
        CASE 
            WHEN days_since_last_purchase <= 30 AND total_transactions >= 10 AND lifetime_value >= {{ var('high_value_customer_threshold') }} THEN 'Champions'
            WHEN days_since_last_purchase <= 30 AND total_transactions >= 5 AND lifetime_value >= 5000 THEN 'Loyal Customers'
            WHEN days_since_last_purchase <= 60 AND total_transactions >= 3 THEN 'Potential Loyalists'
            WHEN days_since_last_purchase <= 30 AND total_transactions <= 2 THEN 'New Customers'
            WHEN days_since_last_purchase BETWEEN 61 AND 120 THEN 'At Risk'
            WHEN days_since_last_purchase > 120 THEN 'Lost'
            ELSE 'Others'
        END AS rfm_segment,
        
        -- Value tiers
        CASE 
            WHEN lifetime_value >= {{ var('high_value_customer_threshold') }} THEN 'High Value'
            WHEN lifetime_value >= 5000 THEN 'Medium Value' 
            WHEN lifetime_value >= 1000 THEN 'Low Value'
            ELSE 'Very Low Value'
        END AS value_tier,
        
        -- Activity level
        CASE 
            WHEN days_since_last_purchase <= 7 THEN 'Highly Active'
            WHEN days_since_last_purchase <= 30 THEN 'Active'
            WHEN days_since_last_purchase <= 90 THEN 'Moderately Active'
            ELSE 'Inactive'
        END AS activity_level

    FROM customer_sales
)

SELECT 
    *,
    ROUND(lifetime_value / GREATEST(total_transactions, 1), 2) AS calculated_avg_transaction_value,
    ROUND(weekend_purchases * 100.0 / GREATEST(total_transactions, 1), 1) AS weekend_purchase_rate,
    ROUND(evening_purchases * 100.0 / GREATEST(total_transactions, 1), 1) AS evening_purchase_rate,
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM customer_segments