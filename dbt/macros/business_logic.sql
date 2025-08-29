-- Custom macros for business logic

{% macro mask_email(column_name) %}
  CONCAT(
    SUBSTR({{ column_name }}, 1, 3), 
    '***@', 
    SPLIT({{ column_name }}, '@')[OFFSET(1)]
  )
{% endmacro %}

{% macro mask_phone(column_name) %}
  CONCAT(
    SUBSTR({{ column_name }}, 1, 3), 
    '-***-', 
    SUBSTR({{ column_name }}, -4)
  )
{% endmacro %}

{% macro calculate_customer_tenure(registration_date) %}
  CASE 
    WHEN DATE_DIFF(CURRENT_DATE(), {{ registration_date }}, DAY) <= 30 THEN 'New'
    WHEN DATE_DIFF(CURRENT_DATE(), {{ registration_date }}, DAY) <= 365 THEN 'Active'
    ELSE 'Established'
  END
{% endmacro %}

{% macro get_time_of_day(hour_column) %}
  CASE 
    WHEN {{ hour_column }} BETWEEN 6 AND 11 THEN 'Morning'
    WHEN {{ hour_column }} BETWEEN 12 AND 17 THEN 'Afternoon'
    WHEN {{ hour_column }} BETWEEN 18 AND 22 THEN 'Evening'
    ELSE 'Night'
  END
{% endmacro %}

{% macro get_thai_holidays() %}
  CASE 
    WHEN FORMAT_DATE('%m-%d', date_day) IN ('01-01', '04-13', '04-14', '04-15', '05-01', '12-31') THEN TRUE
    ELSE FALSE
  END
{% endmacro %}

{% macro calculate_stock_status(current_stock, reorder_point, max_stock) %}
  CASE 
    WHEN {{ current_stock }} <= 0 THEN 'Out of Stock'
    WHEN {{ current_stock }} <= {{ reorder_point }} THEN 'Needs Reorder'
    WHEN {{ current_stock }} >= {{ max_stock }} * 0.9 THEN 'Overstocked'
    ELSE 'Normal'
  END
{% endmacro %}