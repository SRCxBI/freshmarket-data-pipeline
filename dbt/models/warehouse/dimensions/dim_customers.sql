-- models/warehouse/dimensions/dim_customers.sql
{{ config(
    description="Customer dimension with SCD Type 2 for tracking changes",
    materialized='table',
    unique_key='customer_key'
) }}

WITH customer_history AS (
    SELECT 
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_segment', 'loyalty_tier']) }} AS customer_key,
        
        -- Natural key
        customer_id,
        
        -- Attributes
        masked_email,
        masked_phone,
        registration_date,
        customer_segment,
        loyalty_tier,
        customer_tenure,
        segment_priority,
        days_since_registration,
        
        -- SCD Type 2 fields
        dbt_updated_at AS effective_from,
        NULL AS effective_to,
        TRUE AS is_current,
        
        -- Metadata
        created_at,
        dbt_updated_at

    FROM {{ ref('stg_customers') }}
)

SELECT * FROM customer_history
