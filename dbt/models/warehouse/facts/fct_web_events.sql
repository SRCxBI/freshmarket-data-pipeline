-- models/warehouse/facts/fct_web_events.sql
{{ config(
    description="Web events fact table for digital analytics",
    materialized='table',
    partition_by={
        "field": "event_date",
        "data_type": "date"
    },
    cluster_by=["session_id", "event_type"]
) }}

WITH events_with_keys AS (
    SELECT 
        w.*,
        c.customer_key,
        p.product_key,
        {{ dbt_utils.generate_surrogate_key(['session_id', 'event_timestamp', 'event_type']) }} AS event_key
    FROM {{ ref('stg_web_events') }} w
    LEFT JOIN {{ ref('dim_customers') }} c 
        ON w.customer_id = c.customer_id 
        AND c.is_current = TRUE
    LEFT JOIN {{ ref('dim_products') }} p 
        ON w.product_id = p.product_id 
        AND p.is_current = TRUE
)

SELECT 
    event_key,
    session_id,
    customer_key,
    product_key,
    
    -- Event details
    event_type,
    event_category,
    page_url,
    page_category,
    user_type,
    
    -- Time dimensions
    event_date,
    event_timestamp,
    event_hour,
    day_of_week,
    day_type,
    
    -- Metrics (for aggregation)
    1 AS event_count,
    
    -- Session analysis
    ROW_NUMBER() OVER (
        PARTITION BY session_id 
        ORDER BY event_timestamp
    ) AS event_sequence,
    
    -- Conversion flags
    CASE 
        WHEN event_type = 'purchase' THEN 1 
        ELSE 0 
    END AS purchase_event,
    
    CASE 
        WHEN event_type = 'add_to_cart' THEN 1 
        ELSE 0 
    END AS cart_event,
    
    dbt_updated_at

FROM events_with_keys