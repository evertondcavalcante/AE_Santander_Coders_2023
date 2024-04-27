with dados_bronze as (
    select * from {{ source('dados_bronze', 'reviews_bronze') }}
),
final as (
    SELECT listing_id::bigint as listing_id
        , id::bigint as review_id
        , date::timestamp as review_date
        , reviewer_id::bigint as reviewer_id
        , reviewer_name
        , comments
        , created_at
    FROM dados_bronze
)

select * from final