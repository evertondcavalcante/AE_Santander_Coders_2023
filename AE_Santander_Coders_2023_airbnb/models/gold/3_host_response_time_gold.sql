with dados_listings_silver as (
    select * from {{ source('dados_silver', '2_listings_silver') }}
),

final as (
    select host_response_time
        , COUNT(*) AS response_count
    from dados_listings_silver 
    group by host_response_time
    order by host_response_time
    where host_response_time is not null
)

select * from final