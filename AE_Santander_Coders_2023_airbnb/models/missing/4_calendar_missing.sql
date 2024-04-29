with dados_silver as (
    select * from {{ source('dados_silver_2', '2_calendar_silver') }}
),
final as (
    select *
    from dados_silver
    where listing_id is null
        or date is null
        or available is null
        or price is null
        or minimum_nights is null
        or maximum_nights is null
)

select * from final