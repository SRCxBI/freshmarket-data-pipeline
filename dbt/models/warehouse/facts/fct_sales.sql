{{ config(
    description="Sales fact table with pre-calculated metrics",
    materialized='table',
    partition_by={
        "field": "transaction_date",
        "data_type": "date"
    },
    cluster_by=["store_id", "customer_id"]
) }}

WITH sales_with_keys AS (
    SELECT 
        t.*,
        c.customer_key,
        p.product_key,
        {{ dbt_utils.generate_surrogate_key(['t.transaction_id', 't.product_id']) }} AS sales_key
    FROM {{ ref('stg_sales_transactions') }} t
    LEFT JOIN {{ ref('dim_customers') }} c 
        ON t.customer_id = c.customer_id 
        AND c.is_current = TRUE
    LEFT JOIN {{ ref('dim_products') }} p 
        ON t.product_id = p.product_id 
        AND p.is_current = TRUE
),

sales_metrics AS (
    SELECT 
        sales_key,
        transaction_id,
        customer_key,
        product_key,
        store_id,
        customer_id,  -- Keep original IDs for reference
        product_id,   -- Keep original IDs for reference
        
        -- Time keys
        transaction_date,
        transaction_timestamp,
        transaction_hour,
        day_of_week,
        day_type,
        time_of_day,
        
        -- Measures
        quantity,
        unit_price,
        total_amount,
        discount_amount,
        net_revenue,
        discount_rate,
        payment_method,
        
        -- Calculated metrics
        quantity * unit_price AS gross_sales,
        CASE 
            WHEN discount_rate > 0 THEN TRUE 
            ELSE FALSE 
        END AS had_discount,
        
        -- Metadata
        created_at,
        dbt_updated_at

    FROM sales_with_keys
)

SELECT * FROM sales_metrics