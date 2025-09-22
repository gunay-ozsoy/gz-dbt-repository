WITH daily AS (
    SELECT * FROM {{ ref('finance_campaigns_day') }}
)

SELECT
    DATE_TRUNC(date_date, MONTH) AS datemonth,
    ROUND(SUM(ads_margin), 2) AS ads_margin,
    ROUND(SUM(revenue) / NULLIF(SUM(total_quantity_sold),0), 2) AS average_basket,
    SUM(operational_margin) AS operational_margin,
    SUM(ads_cost) AS ads_cost,
    SUM(ads_impression) AS ads_impression,
    SUM(ads_click) AS ads_clicks,
    SUM(total_quantity_sold) AS quantity,
    SUM(total_revenue) AS revenue,
    SUM(total_purchase_cost) AS purchase_cost,
    SUM(total_margin) AS margin,
    SUM(total_shipping_fees) AS shipping_fee,
    SUM(total_log_costs) AS log_cost,
    SUM(total_ship_cost) AS ship_cost
FROM daily
GROUP BY datemonth
ORDER BY datemonth DESC
