WITH promotion_attribution AS (
    SELECT * FROM {{ ref('int_promotion_attribution') }}
)

SELECT 
    promotion_id,
    promotion_name,
    product_id,
    discount_pct,
    start_date,
    end_date,
    promotion_channel,
    COUNT(DISTINCT order_id) AS promotion_attributed_orders,
    SUM(quantity) AS promotion_attributed_orders_sold,
    SUM(gross_amount) AS promotion_attributed_gross_revenue,
    SUM(discount_amount) AS promotion_attributed_discount_amount,
    SUM(net_amount) AS promotion_attributed_net_revenue,
    SUM(estimated_margin_amount) AS promotion_attributed_margin_amount
FROM promotion_attribution
GROUP BY
    promotion_id,
    promotion_name,
    product_id,
    discount_pct,
    start_date,
    end_date,
    promotion_channel