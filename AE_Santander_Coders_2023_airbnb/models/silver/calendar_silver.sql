{{ 
    config(order=4) 
}}
with dados_bronze as (
    select * from {{ source('dados_bronze', 'calendar_bronze') }}
),
final as (
    select listing_id::bigint as listing_id
        , date::date as date
        , case 
            when available = 'f' then False
            when available = 't' then True end::boolean as available
        , replace(replace(price,'$',''),',','') :: float as price
        , minimum_nights :: int as minimum_nights
        , maximum_nights :: int as maximum_nights
        , created_at
    from dados_bronze
)

select * from final