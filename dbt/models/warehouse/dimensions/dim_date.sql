{{ config(
    description="Date dimension for time-based analysis",
    materialized='table'
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(WEEK FROM date_day) AS week,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    EXTRACT(DAYOFYEAR FROM date_day) AS day_of_year,
    
    -- Day names
    FORMAT_DATE('%A', date_day) AS day_name,
    FORMAT_DATE('%B', date_day) AS month_name,
    
    -- Business calendar
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN FALSE
        ELSE TRUE
    END AS is_weekday,
    
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    
    -- Thai holidays (simplified)
    CASE 
        WHEN FORMAT_DATE('%m-%d', date_day) IN ('01-01', '04-13', '04-14', '04-15', '05-01', '12-31') THEN TRUE
        ELSE FALSE
    END AS is_thai_holiday,
    
    -- Business periods
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM date_day) AS STRING), '-', CAST(EXTRACT(YEAR FROM date_day) AS STRING)) AS quarter_name,
    CONCAT(FORMAT_DATE('%B', date_day), ' ', CAST(EXTRACT(YEAR FROM date_day) AS STRING)) AS month_year,
    
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM date_spine