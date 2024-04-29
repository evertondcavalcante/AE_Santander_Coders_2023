with dados_listings_silver as (
    select * from {{ source('dados_silver', '2_listings_silver') }}
),
final as (
    select neighbourhood_cleansed
        , AVG(price) AS avg_price
    from dados_listings_silver
    where price is not null
    group by neighbourhood_cleansed
)

select * from final