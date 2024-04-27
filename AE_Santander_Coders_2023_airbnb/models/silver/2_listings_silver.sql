with dados_bronze as (
    select * from {{ source('dados_bronze', '1_listings_bronze') }}
),
final as (
    SELECT id::bigint as id
        , listing_url
        , last_scraped::date as last_scraped
        , source
        , name
        , neighborhood_overview
        , picture_url
        , host_id::bigint
        , host_url
        , host_name
        , host_since::timestamp as host_since
        , host_location
        , host_about
        , host_response_time
        , replace(replace(host_response_rate,'%',''),'N/A',null)::float as host_response_rate
        , replace(replace(host_acceptance_rate,'%',''),'N/A',null)::float as host_acceptance_rate
        , host_is_superhost::boolean as host_is_superhost
        , host_thumbnail_url
        , host_picture_url
        , host_neighbourhood
        , host_listings_count::int as host_listings_count
        , host_total_listings_count::int as host_total_listings_count
        , host_verifications
        , host_has_profile_pic::boolean as host_has_profile_pic
        , host_identity_verified::boolean as host_identity_verified
        , neighbourhood
        , neighbourhood_cleansed
        , replace(latitude,'N/A',null)::float as latitude
        , replace(longitude,'N/A',null)::float as longitude
        , property_type
        , room_type
        , accommodates::int as accommodates
        , bathrooms_text
        , beds::int as beds
        , replace(replace(replace(price,'$',''),',',''),'N/A',null)::float price
        , minimum_nights::int as minimum_nights
        , maximum_nights::int as maximum_nights
        , minimum_minimum_nights::int as minimum_minimum_nights
        , maximum_minimum_nights::int as maximum_minimum_nights
        , minimum_maximum_nights::int as minimum_maximum_nights
        , maximum_maximum_nights::int as maximum_maximum_nights
        , replace(minimum_nights_avg_ntm,'N/A',null)::float as minimum_nights_avg_ntm
        , replace(maximum_nights_avg_ntm,'N/A',null)::float as maximum_nights_avg_ntm
        , has_availability::boolean as has_availability
        , availability_30::int as availability_30
        , availability_60::int as availability_60
        , availability_90::int as availability_90
        , availability_365::int as availability_365
        , calendar_last_scraped::timestamp as calendar_last_scraped
        , number_of_reviews::int as number_of_reviews
        , number_of_reviews_ltm::int as number_of_reviews_ltm
        , number_of_reviews_l30d::int as number_of_reviews_l30d
        , first_review::timestamp as first_review
        , last_review::timestamp as last_review
        , replace(review_scores_rating,'N/A',null)::float as review_scores_rating
        , replace(review_scores_accuracy,'N/A',null)::float review_scores_accuracy
        , replace(review_scores_cleanliness,'N/A',null)::float as review_scores_cleanliness
        , replace(review_scores_checkin,'N/A',null)::float as review_scores_checkin
        , replace(review_scores_communication,'N/A',null)::float as review_scores_communication
        , replace(review_scores_location,'N/A',null)::float as review_scores_location
        , replace(review_scores_value,'N/A',null)::float as review_scores_value
        , instant_bookable::boolean as instant_bookable
        , calculated_host_listings_count::int as calculated_host_listings_count
        , calculated_host_listings_count_entire_homes::int as calculated_host_listings_count_entire_homes
        , calculated_host_listings_count_private_rooms::int as calculated_host_listings_count_private_rooms
        , calculated_host_listings_count_shared_rooms::int as calculated_host_listings_count_shared_rooms
        , replace(reviews_per_month,'N/A',null)::float as reviews_per_month
        , created_at
    from dados_bronze
    
)

select * from final