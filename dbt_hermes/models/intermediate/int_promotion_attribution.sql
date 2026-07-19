WITH promotions AS (
    SELECT * FROM {{ ref('stg_promotions')}}
),

order_revenue AS (
    SELECT * FROM {{ ref('int_order_revenue') }}
)

SELECT
    promotions.promotion_id,
    promotions.promotion_name,
    promotions.product_id,
    promotions.discount_pct,
    promotions.start_date,
    promotions.end_date,
    promotions.channel AS promotion_channel,
    order_revenue.order_item_id,
    order_revenue.order_id,
    order_revenue.customer_id,
    order_revenue.store_id,
    order_revenue.channel AS order_channel,
    order_revenue.order_date,
    order_revenue.quantity,
    order_revenue.gross_amount,
    order_revenue.discount_amount,
    order_revenue.net_amount,
    order_revenue.estimated_margin_amount
FROM promotions
LEFT JOIN order_revenue
    ON promotions.product_id = order_revenue.product_id
    AND order_revenue.order_date BETWEEN promotions.start_date AND promotions.end_date
    AND (
        promotions.channel = 'all'
        OR promotions.channel = order_revenue.channel
    )
    

