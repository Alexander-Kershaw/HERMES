SELECT 
    inventory_snapshot_id,
    store_id,
    product_id,
    snapshot_date,
    stock_on_hand,
    reorder_point,
    reorder_quantity,
    is_stockout,
    is_below_reorder_point,
    _silver_processed_at,
    _silver_processing_date
FROM {{ source('silver', 'inventory_snapshots') }}