SELECT
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    brand,
    unit_price,
    cost_price,
    is_active,
    creation_datetime,
    _silver_processed_at,
    _silver_processing_date
FROM {{ source('silver', 'products') }}