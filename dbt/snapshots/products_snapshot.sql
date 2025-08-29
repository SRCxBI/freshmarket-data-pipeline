{% snapshot products_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_id',
      strategy='timestamp',
      updated_at='created_at',
    )
}}

SELECT 
    product_id,
    supplier_id,
    product_name,
    category,
    subcategory,
    brand,
    unit_cost,
    selling_price,
    created_at,
    updated_at
    
FROM {{ source('raw_data', 'products') }}

{% endsnapshot %}