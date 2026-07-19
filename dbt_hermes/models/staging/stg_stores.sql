SELECT  
    store_id,
    store_name,
    city,
    region,
    postal_code,
    store_format,
    opening_date,
    is_active,
    _silver_processed_at,
    _silver_processing_date
FROM {{ source('silver', 'stores') }}