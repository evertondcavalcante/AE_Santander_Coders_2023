-- View to analyze the impact of amenities on booking rates and pricing
-- across different neighborhoods in the Airbnb listings from the silver layer.

WITH base_data AS (
    SELECT
        id AS listing_id,
        neighbourhood_cleansed,
        amenities,
        price,
        number_of_reviews,
        has_availability
    FROM {{ source('dados_silver', '2_listings_silver') }}
    WHERE has_availability = 'true'
),

amenities_analysis AS (
    SELECT
        neighbourhood_cleansed,
        STRING_AGG(DISTINCT amenities, ', ') AS combined_amenities,
        AVG(price) AS average_price,
        COUNT(listing_id) AS total_listings,
        SUM(number_of_reviews) AS total_reviews
    FROM base_data
    GROUP BY neighbourhood_cleansed
    ORDER BY average_price DESC
)

SELECT
    neighbourhood_cleansed,
    combined_amenities,
    average_price,
    total_listings,
    total_reviews
FROM amenities_analysis