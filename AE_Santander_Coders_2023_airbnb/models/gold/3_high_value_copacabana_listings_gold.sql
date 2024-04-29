with dados_listings_silver as (
    select * from {{ source('dados_silver', '2_listings_silver') }}
),
dados_calendar_silver as (
    select * from {{ source('dados_silver', '2_calendar_silver') }}
),
final as (
    select a.listing_id
        , a.date as date
        , a.price
        , a.minimum_nights
        , a.maximum_nights
        , b.listing_url
        , b.name
        , b.host_name
        , b.accommodates
        , b.bathrooms_text
        , COALESCE(b.beds,0) beds
    from dados_calendar_silver a
    inner join dados_listings_silver b
        on a.listing_id = b.id
    where a.available = true
        and UPPER(b.neighbourhood_cleansed) = 'COPACABANA'
        and a.price >= 1000
        and b.host_name is not null
        and a.minimum_nights is not null
        and b.maximum_nights is not null
)

select * from final