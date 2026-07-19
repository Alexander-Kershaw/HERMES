SELECT
    promotion_id,
    promotion_name,
    product_id,
    discount_pct,
    start_date,
    end_date,
    LOWER(TRIM(channel)) AS channel,
    is_active,
    _silver_processed_at,
    _silver_processing_date
FROM {{ source('silver', 'promotions') }}

