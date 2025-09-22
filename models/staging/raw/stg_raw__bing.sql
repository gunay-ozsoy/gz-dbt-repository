with source as (
    select 
        * 
    from {{ source('raw', 'bing') }}
),

renamed as (
    select
        DATE(date_date) AS date_date,
        paid_source,
        campaign_key,
        camPGN_name AS campaign_name,        -- yeniden adlandırıldı
        CAST(ads_cost AS FLOAT64) AS ads_cost, -- tip dönüştürüldü
        impression,
        click
    from source
)

select * from renamed
