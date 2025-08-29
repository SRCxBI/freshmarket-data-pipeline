{{ config(
    description="Marketing campaign performance with ROI and customer acquisition metrics",
    materialized='table'
) }}

WITH campaign_sales AS (
    SELECT 
        c.campaign_id,
        c.campaign_name,
        c.channel,
        c.channel_category,
        c.budget,
        c.start_date,
        c.end_date,
        c.campaign_duration_days,
        c.daily_budget,
        c.campaign_status,
        c.budget_tier,
        
        -- Web events during campaign
        COALESCE(w.total_events, 0) AS total_web_events,
        COALESCE(w.unique_visitors, 0) AS unique_visitors,
        COALESCE(w.page_views, 0) AS page_views,
        COALESCE(w.product_views, 0) AS product_views,
        COALESCE(w.cart_events, 0) AS cart_events,
        
        -- Sales during campaign (attribution based on timing)
        COALESCE(s.total_transactions, 0) AS attributed_transactions,
        COALESCE(s.revenue, 0) AS attributed_revenue,
        COALESCE(s.customers, 0) AS attributed_customers

    FROM {{ ref('stg_marketing_campaigns') }} c
    LEFT JOIN (
        SELECT 
            -- Simple time-based attribution for web events
            DATE(event_timestamp) AS event_date,
            COUNT(*) AS total_events,
            COUNT(DISTINCT session_id) AS unique_visitors,
            COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS page_views,
            COUNT(CASE WHEN event_type = 'product_view' THEN 1 END) AS product_views,
            COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS cart_events
        FROM {{ ref('fct_web_events') }}
        GROUP BY 1
    ) w ON DATE(c.start_date) <= w.event_date AND w.event_date <= DATE(c.end_date)
    
    LEFT JOIN (
        SELECT 
            -- Simple time-based attribution for sales
            transaction_date,
            COUNT(DISTINCT transaction_id) AS total_transactions,
            SUM(net_revenue) AS revenue,
            COUNT(DISTINCT customer_key) AS customers
        FROM {{ ref('fct_sales') }}
        GROUP BY 1
    ) s ON DATE(c.start_date) <= s.transaction_date AND s.transaction_date <= DATE(c.end_date)
)

SELECT 
    campaign_id,
    campaign_name,
    channel,
    channel_category,
    budget,
    start_date,
    end_date,
    campaign_duration_days,
    daily_budget,
    campaign_status,
    budget_tier,
    
    -- Web engagement metrics
    total_web_events,
    unique_visitors,
    page_views,
    product_views,
    cart_events,
    
    -- Sales attribution
    attributed_transactions,
    attributed_revenue,
    attributed_customers,
    
    -- Performance calculations
    CASE 
        WHEN unique_visitors > 0 
        THEN ROUND(attributed_transactions * 100.0 / unique_visitors, 2)
        ELSE 0 
    END AS conversion_rate,
    
    CASE 
        WHEN cart_events > 0 
        THEN ROUND(attributed_transactions * 100.0 / cart_events, 2)
        ELSE 0 
    END AS cart_conversion_rate,
    
    CASE 
        WHEN attributed_transactions > 0 
        THEN ROUND(attributed_revenue / attributed_transactions, 2)
        ELSE 0 
    END AS avg_order_value,
    
    -- Cost metrics
    CASE 
        WHEN attributed_customers > 0 
        THEN ROUND(budget / attributed_customers, 2)
        ELSE 0 
    END AS customer_acquisition_cost,
    
    CASE 
        WHEN unique_visitors > 0 
        THEN ROUND(budget / unique_visitors, 2)
        ELSE 0 
    END AS cost_per_visitor,
    
    CASE 
        WHEN attributed_transactions > 0 
        THEN ROUND(budget / attributed_transactions, 2)
        ELSE 0 
    END AS cost_per_transaction,
    
    -- ROI calculations
    CASE 
        WHEN budget > 0 
        THEN ROUND((attributed_revenue - budget) / budget * 100, 2)
        ELSE 0 
    END AS roi_percentage,
    
    ROUND(attributed_revenue - budget, 2) AS net_profit,
    
    -- Performance rating
    CASE 
        WHEN attributed_revenue >= budget * 3 THEN 'Excellent'
        WHEN attributed_revenue >= budget * 2 THEN 'Good'
        WHEN attributed_revenue >= budget * 1.5 THEN 'Average'
        WHEN attributed_revenue >= budget THEN 'Below Average'
        ELSE 'Poor'
    END AS performance_rating,
    
    -- Channel efficiency
    ROUND(attributed_revenue / GREATEST(campaign_duration_days, 1), 2) AS daily_revenue,
    ROUND(attributed_transactions / GREATEST(campaign_duration_days, 1), 2) AS daily_transactions,
    
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM campaign_sales