-- models/warehouse/dimensions/dim_stores.sql
{{ config(
    description="Store dimension with location details",
    materialized='table'
) }}

SELECT 
    'STR001' AS store_id,
    'FreshMarket Central Bangkok' AS store_name,
    'Bangkok CBD' AS location,
    'Central' AS district,
    'Bangkok' AS city,
    'Thailand' AS country,
    'Premium' AS store_type,
    TIMESTAMP('2023-01-15') AS opening_date,
    TRUE AS is_active,
    CURRENT_TIMESTAMP() AS dbt_updated_at

UNION ALL

SELECT 
    'STR002' AS store_id,
    'FreshMarket Sukhumvit' AS store_name,
    'Sukhumvit Road' AS location,
    'Watthana' AS district,
    'Bangkok' AS city,
    'Thailand' AS country,
    'Standard' AS store_type,
    TIMESTAMP('2023-03-10') AS opening_date,
    TRUE AS is_active,
    CURRENT_TIMESTAMP() AS dbt_updated_at

UNION ALL

SELECT 
    'STR003' AS store_id,
    'FreshMarket Silom' AS store_name,
    'Silom District' AS location,
    'Bang Rak' AS district,
    'Bangkok' AS city,
    'Thailand' AS country,
    'Compact' AS store_type,
    TIMESTAMP('2023-05-20') AS opening_date,
    TRUE AS is_active,
    CURRENT_TIMESTAMP() AS dbt_updated_at

UNION ALL

SELECT 
    'STR004' AS store_id,
    'FreshMarket Chatuchak' AS store_name,
    'Chatuchak Market Area' AS location,
    'Chatuchak' AS district,
    'Bangkok' AS city,
    'Thailand' AS country,
    'Market Style' AS store_type,
    TIMESTAMP('2023-07-01') AS opening_date,
    TRUE AS is_active,
    CURRENT_TIMESTAMP() AS dbt_updated_at

UNION ALL

SELECT 
    'STR005' AS store_id,
    'FreshMarket Siam' AS store_name,
    'Siam Square' AS location,
    'Pathum Wan' AS district,
    'Bangkok' AS city,
    'Thailand' AS country,
    'Premium' AS store_type,
    TIMESTAMP('2023-09-15') AS opening_date,
    TRUE AS is_active,
    CURRENT_TIMESTAMP() AS dbt_updated_at