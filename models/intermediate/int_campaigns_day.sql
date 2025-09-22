WITH campaigns AS (
    SELECT *
    FROM {{ref("int_campaigns")}}
)
SELECT
    date_date,
    campaign_key,
    campaign_name,
    paid_source,
    SUM(ads_cost) AS total_ads_cost,
    SUM(click) AS total_clicks,
    SUM(impression) AS total_impressions    
FROM campaigns
GROUP BY date_date, campaign_key, campaign_name, paid_source
ORDER BY date_date DESC, paid_source