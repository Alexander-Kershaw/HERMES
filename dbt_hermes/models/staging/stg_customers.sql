SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    address,
    city,
    region,
    postal_code,
    loyalty_tier,
    creation_datetime,
    is_active,
    _silver_processed_at,
    _silver_processing_date
FROM {{ source('silver', 'customers') }}