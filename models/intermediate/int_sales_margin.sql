with sales as (
    select *
    from {{ ref("stg_raw__sales") }}
),
products as (
    select *
    from {{ ref("stg_raw__product") }}
)

select
    s.orders_id,
    s.products_id,
    s.quantity,
    p.purchase_price,
    s.revenue,
    -- satın alma maliyeti
    round(cast(s.quantity as int64) * cast(p.purchase_price as numeric), 2) as purchase_cost,
    -- marj
    round(s.revenue - (cast(s.quantity as int64) * cast(p.purchase_price as numeric)), 2) as margin
from sales s
inner join products p
    on s.products_id = p.products_id
