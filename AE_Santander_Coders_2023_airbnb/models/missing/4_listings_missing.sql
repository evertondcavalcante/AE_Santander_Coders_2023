with dados_silver as (
    select * from {{ source('dados_silver_2', '2_listings_silver') }}
),
final as (
    SELECT *
    from dados_silver
    where id is null
        or listing_url is null
        or source is null
        or name is null
        or host_id is null
        or host_url is null
        or host_name is null
        or host_since is null
        or host_is_superhost is null
        or host_thumbnail_url is null
        or host_picture_url is null
        or host_listings_count is null
        or host_total_listings_count is null
        or host_verifications is null
        or host_has_profile_pic is null
        or host_identity_verified is null
        or neighbourhood is null
        or neighbourhood_cleansed is null
        or latitude is null
        or longitude is null
        or property_type is null
        or room_type is null
        or accommodates is null
        or bathrooms_text is null
        or beds is null
        or price is null
        or minimum_nights is null
        or maximum_nights is null
        or minimum_minimum_nights is null
        or maximum_minimum_nights is null
        or minimum_maximum_nights is null
        or maximum_maximum_nights is null
        or minimum_nights_avg_ntm is null
        or maximum_nights_avg_ntm is null
        or has_availability is null
        or availability_30 is null
        or availability_60 is null
        or availability_90 is null
        or availability_365 is null
        or calendar_last_scraped is null
        or number_of_reviews is null
        or number_of_reviews_ltm is null
        or number_of_reviews_l30d is null
        or review_scores_rating is null
        or review_scores_accuracy is null
        or review_scores_cleanliness is null
        or review_scores_checkin is null
        or review_scores_communication is null
        or review_scores_location is null
        or review_scores_value is null
        or instant_bookable is null
        or calculated_host_listings_count is null
        or calculated_host_listings_count_entire_homes is null
        or calculated_host_listings_count_private_rooms is null
        or calculated_host_listings_count_shared_rooms is null
        or reviews_per_month is null
    
)

select * from final