with dados_listings_silver as (
    select * from {{ source('dados_silver', '2_listings_silver') }}
),
dados_reviews_silver as (
    select * from {{ source('dados_silver', '2_reviews_silver') }}
),
final as (
    select a.listing_id
        , a.review_id
        , a.review_date
        , a.comments
        , b.review_scores_rating
        , b.review_scores_accuracy
        , b.review_scores_cleanliness
        , b.review_scores_checkin
        , b.review_scores_communication
        , b.review_scores_location
        , b.review_scores_value
    from dados_reviews_silver a
    inner join dados_listings_silver b
        on a.listing_id = b.id
    where review_date >= now() -interval '1 year'
        and b.review_scores_rating <= 3
        and b.review_scores_rating is not null
        and coalesce(b.number_of_reviews,0) > 0
)

select * from final