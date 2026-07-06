SELECT 
    order_item_id,
    order_id,
    line_number,
    product_id,
    quantity,
    unit_price,
    gross_amount,
    discount_amount,
    net_amount,
    tax_amount,
    _silver_processed_at,
    _silver_processing_date
FROM {{ source('silver', 'order_items') }}