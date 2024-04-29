WITH dados_listings_silver AS (
    SELECT * FROM {{ source('dados_silver', '2_listings_silver') }}
)
, base_data AS (
    SELECT
        id AS listing_id,
        neighbourhood_cleansed,
        amenities,
        price,
        number_of_reviews,
        has_availability
    FROM dados_listings_silver
    WHERE has_availability = True
)
, amenities_analysis AS (
    SELECT neighbourhood_cleansed,
        STRING_AGG(DISTINCT amenities, ', ') AS combined_amenities,
        AVG(price) AS average_price,
        COUNT(listing_id) AS total_listings,
        SUM(number_of_reviews) AS total_reviews
    FROM base_data
    GROUP BY neighbourhood_cleansed
    ORDER BY average_price DESC
)

SELECT *
FROM amenities_analysis