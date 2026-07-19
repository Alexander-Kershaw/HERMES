SELECT 
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    brand,
    unit_price,
    cost_price,
    is_active,
    creation_datetime
FROM {{ ref('stg_products') }}