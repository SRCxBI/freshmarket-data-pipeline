-- models/staging/stg_customers.sql
{{ config(
    description="Cleaned customer master data with calculated fields",
    materialized='view'
) }}

SELECT 
    -- Identifiers
    customer_id,
    
    -- Contact information (masked for privacy)
    CONCAT(SUBSTR(email, 1, 3), '***@', SPLIT(email, '@')[OFFSET(1)]) AS masked_email,
    CONCAT(SUBSTR(phone, 1, 3), '-***-', SUBSTR(phone, -4)) AS masked_phone,
    
    -- Customer attributes
    registration_date,
    customer_segment,
    loyalty_tier,
    
    -- Calculated fields
    DATE_DIFF(CURRENT_DATE(), registration_date, DAY) AS days_since_registration,
    
    CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), registration_date, DAY) <= 30 THEN 'New'
        WHEN DATE_DIFF(CURRENT_DATE(), registration_date, DAY) <= 365 THEN 'Active'
        ELSE 'Established'
    END AS customer_tenure,
    
    CASE 
        WHEN customer_segment = 'Premium' THEN 1
        WHEN customer_segment = 'Regular' THEN 2
        WHEN customer_segment = 'Budget' THEN 3
        ELSE 4
    END AS segment_priority,
    
    -- Metadata
    created_at,
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ source('raw_data', 'customers') }}