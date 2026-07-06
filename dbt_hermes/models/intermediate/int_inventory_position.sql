WITH inventory AS (
    SELECT * FROM {{ REF('stg_inventory_snapshots') }}
),

products AS (
    SELECT * FROM {{ REF('stg_products') }}
),

stores AS (
    SELECT * FROM {{ REF('stg_stores') }}
)

SELECT 
    inventory.inventory_snapshot_id,
    inventory.snapshot_date,
    inventory.store_id,
    stores.city AS store_city,
    stores.region AS store_region,
    stores.store_format,
    inventory.product_id,
    products.product_name,
    products.category,
    products.subcategory,
    inventory.stock_on_hand,
    inventory.reorder_point,
    inventory.reorder_quantity,
    inventory.is_stockout,
    inventory.is_below_reorder_point,
    inventory.stock_on_hand * products.unit_price AS stock_retail_value,
    inventory.stock_on_hand * products.cost_proce AS stock_cost_value
FROM inventory
LEFT JOIN stores
    ON inventory.store_id = stores.store_id
LEFT JOIN products
    ON inventory.product_id = products.product_id