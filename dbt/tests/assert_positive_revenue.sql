-- Test to ensure all sales have positive net revenue
-- This test will fail if any records have negative or zero revenue

SELECT 
    transaction_id,
    net_revenue,
    total_amount,
    discount_amount
FROM {{ ref('fct_sales') }}
WHERE net_revenue <= 0