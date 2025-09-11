with operational as (
  select
    orders_id,
    date_date,
    revenue,
    quantity,
    purchase_cost,
    margin,
    operational_margin,
    shipping_fee,
    log_cost,
    ship_cost
  from {{ ref('int_orders_operational') }}
)

select
  date_date                                 as date_date,
  count(distinct orders_id)                 as total_transactions,
  round(sum(revenue), 2)                    as total_revenue,
  round(safe_divide(sum(revenue),
                    nullif(count(distinct orders_id), 0)), 2) as avg_basket,
  round(sum(operational_margin), 2)         as operational_margin,
  round(sum(purchase_cost), 2)              as total_purchase_cost,
  round(sum(shipping_fee), 2)               as total_shipping_fees,
  round(sum(log_cost), 2)                   as total_log_costs,
  sum(quantity)                             as total_quantity_sold
from operational
group by date_date