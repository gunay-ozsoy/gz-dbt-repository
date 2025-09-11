WITH sales_margin AS (
    SELECT * 
    FROM {{ref("int_sales_margin")}}
)
SELECT
    s.orders_id,
    MIN(s.date_date)                    AS date_date,
    ROUND(SUM(s.revenue),2)             AS revenue,
    SUM(s.quantity)                     AS quantity,
    ROUND(SUM(s.purchase_cost),2)       AS purchase_cost,
    ROUND(SUM(s.margin),2)              AS margin
FROM sales_margin AS s
GROUP BY s.orders_id