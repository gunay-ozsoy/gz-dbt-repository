with 

source as (

    select * from {{ source('raw', 'ship') }}

),

renamed as (

    select
        orders_id,
        safe_cast(shipping_fee as float64)  as shipping_fee,
        safe_cast(logcost as float64)       as log_cost,
        safe_cast(ship_cost as float64)     as ship_cost

    from source

)

select * from renamed
