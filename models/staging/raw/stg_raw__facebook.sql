with source as (
    select * from {{ source('raw', 'facebook') }}
),

renamed as (
    select
        DATE(date_date) AS date_date,
        paid_source,
        campaign_key,
        camPGN_name AS campaign_name,         -- rename
        CAST(ads_cost AS FLOAT64) AS ads_cost, -- type cast
        impression,
        click
    from source
)

select * from renamed
