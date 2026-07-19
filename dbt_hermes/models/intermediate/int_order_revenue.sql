WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT 
    order_items.order_item_id,
    orders.order_id,
    orders.customer_id,
    orders.store_id,
    orders.status,
    orders.channel,
    orders.order_timestamp,
    orders.order_date,
    orders.currency,
    order_items.product_id,
    products.category,
    products.subcategory,
    products.brand,
    order_items.line_number,
    order_items.quantity,
    order_items.unit_price,
    order_items.gross_amount,
    order_items.discount_amount,
    order_items.net_amount,
    order_items.tax_amount,
    products.cost_price,
    order_items.quantity * products.cost_price AS estimated_cost_amount,
    order_items.net_amount - (order_items.quantity * products.cost_price) AS estimated_margin_amount
FROM order_items
INNER JOIN orders
    ON order_items.order_id = orders.order_id
LEFT JOIN products
    ON order_items.product_id = products.product_id