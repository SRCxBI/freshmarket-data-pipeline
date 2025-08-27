-- models/staging/stg_marketing_campaigns.sql
{{ config(
    description="Marketing campaigns with performance periods",
    materialized='view'
) }}

SELECT 
    -- Identifiers
    campaign_id,
    campaign_name,
    channel,
    
    -- Campaign details
    start_date,
    end_date,
    CAST(budget AS NUMERIC) AS budget,
    target_audience,
    
    -- Calculated fields
    DATE_DIFF(end_date, start_date, DAY) + 1 AS campaign_duration_days,
    CAST(budget AS NUMERIC) / (DATE_DIFF(end_date, start_date, DAY) + 1) AS daily_budget,
    
    CASE 
        WHEN CURRENT_DATE() < start_date THEN 'Scheduled'
        WHEN CURRENT_DATE() BETWEEN start_date AND end_date THEN 'Active'
        WHEN CURRENT_DATE() > end_date THEN 'Completed'
        ELSE 'Unknown'
    END AS campaign_status,
    
    CASE 
        WHEN channel IN ('Facebook', 'Google Ads') THEN 'Digital'
        WHEN channel IN ('Email', 'SMS', 'Line Official') THEN 'Direct Marketing'
        WHEN channel = 'In-store' THEN 'Traditional'
        ELSE 'Other'
    END AS channel_category,
    
    -- Budget classification
    CASE 
        WHEN budget >= 50000 THEN 'Large'
        WHEN budget >= 20000 THEN 'Medium'
        ELSE 'Small'
    END AS budget_tier,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ source('raw_data', 'marketing_campaigns') }}