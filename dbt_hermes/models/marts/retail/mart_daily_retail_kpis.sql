WITH sales AS (
    SELECT * FROM {{ REF('fct_sales') }}
)

SELECT 
    order_date,
    channel,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products_sold,
    SUM(quantity) AS total_units_sold,
    SUM(gross_amount) AS gross_revenue,
    SUM(discount_amount) AS total_discount_amount,
    SUM(net_amount) AS net_revenue,
    SUM(tax_amount) AS total_tax_amount,
    SUM(estimated_cost_amount) AS total_estimated_cost_amount,
    SUM(estimated_margin_amount) AS total_estimated_margin_amount,
    CASE
        WHEN sum(net_amount) = 0 THEN 0
        ELSE SUM(estimated_margin_amount) / SUM(net_amount)
    END AS estimated_margin_rate,
    AVG(net_amount) AS avg_net_revenue
FROM sales
GROUP BY order_date, channel