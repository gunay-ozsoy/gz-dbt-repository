with orders as (
    select
        orders_id,
        date_date,
        margin
    from {{ ref('int_orders_margin') }}
),
ship as (
    select
        orders_id,
        sum(shipping_fee) as shipping_fee,
        sum(log_cost)     as log_cost,
        sum(ship_cost)    as ship_cost
    from {{ ref('stg_raw__ship') }}
    group by orders_id
)

select
    o.orders_id,
    o.date_date,
    round(
        o.margin
        + coalesce(s.shipping_fee, 0)
        - coalesce(s.log_cost, 0)
        - coalesce(s.ship_cost, 0)
    , 2) as operational_margin
from orders o
left join ship s using (orders_id)
