{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='created_at',
    )
}}

SELECT 
    customer_id,
    email,
    phone,
    customer_segment,
    loyalty_tier,
    registration_date,
    created_at,
    updated_at
    
FROM {{ source('raw_data', 'customers') }}

{% endsnapshot %}