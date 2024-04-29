with dados_silver as (
    select * from {{ source('dados_silver_2', '2_reviews_silver') }}
),
final as (
    SELECT *
    FROM dados_silver
    where listing_id is null
        or review_id is null
        or review_date is null
        or reviewer_id is null
        or reviewer_name is null
        or comments is null
)

select * from final