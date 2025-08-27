-- models/staging/stg_web_events.sql
{{ config(
    description="Web events with session analysis",
    materialized='view'
) }}

SELECT 
    -- Identifiers
    session_id,
    customer_id,
    product_id,
    
    -- Event details
    event_type,
    page_url,
    timestamp AS event_timestamp,
    DATE(timestamp) AS event_date,
    
    -- Time analysis
    EXTRACT(HOUR FROM timestamp) AS event_hour,
    EXTRACT(DAYOFWEEK FROM timestamp) AS day_of_week,
    
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM timestamp) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    
    -- Page classification
    CASE 
        WHEN page_url = '/' THEN 'Homepage'
        WHEN page_url LIKE '/products%' THEN 'Product Page'
        WHEN page_url LIKE '/categories%' THEN 'Category Page'
        WHEN page_url = '/cart' THEN 'Cart'
        WHEN page_url = '/checkout' THEN 'Checkout'
        WHEN page_url = '/search' THEN 'Search'
        ELSE 'Other'
    END AS page_category,
    
    -- User classification
    CASE 
        WHEN customer_id IS NOT NULL THEN 'Registered'
        ELSE 'Anonymous'
    END AS user_type,
    
    -- Event classification
    CASE 
        WHEN event_type IN ('add_to_cart', 'purchase') THEN 'Conversion'
        WHEN event_type IN ('product_view', 'page_view') THEN 'Engagement'
        WHEN event_type = 'search' THEN 'Discovery'
        ELSE 'Other'
    END AS event_category,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ source('raw_data', 'web_events') }}