WITH finance AS (
    SELECT * FROM {{ ref("finance_day") }}
), ads AS (
    SELECT
        date_date,
        SUM(total_ads_cost)     AS ads_cost,
        SUM(total_clicks)       AS ads_click,
        SUM(total_impressions)  AS ads_impression
    FROM {{ ref("int_campaigns_day") }}
    GROUP BY date_date
)

SELECT
    f.date_date AS date_date,
    ROUND(f.operational_margin - a.ads_cost, 2) AS ads_margin,
    f.avg_basket AS average_basket,
    f.operational_margin,
    a.ads_cost,
    a.ads_impression,
    a.ads_click,
    f.total_quantity_sold,
    f.total_revenue,
    f.total_purchase_cost,
    f.total_margin,
    f.total_shipping_fees,
    f.total_log_costs,
    f.total_ship_cost
FROM finance AS f
LEFT JOIN ads AS a
    ON f.date_date = a.date_date
ORDER BY date_date DESC
