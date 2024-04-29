WITH dados_listings_silver AS (
    SELECT * FROM {{ source('dados_silver', '2_listings_silver') }}
),
dados_reviews_silver AS (
    SELECT * FROM {{ source('dados_silver', '2_reviews_silver') }}
),
tendencias_satisfacao AS (
    SELECT 
        DATE_TRUNC('month', a.review_date) AS month,
        COUNT(*) AS total_reviews,
        AVG(b.review_scores_rating) AS average_rating,
        AVG(b.review_scores_accuracy) AS average_accuracy,
        AVG(b.review_scores_cleanliness) AS average_cleanliness,
        AVG(b.review_scores_checkin) AS average_checkin,
        AVG(b.review_scores_communication) AS average_communication,
        AVG(b.review_scores_location) AS average_location,
        AVG(b.review_scores_value) AS average_value
    FROM dados_reviews_silver a
    INNER JOIN dados_listings_silver b
        ON a.listing_id = b.id
    WHERE a.review_date >= CURRENT_DATE - INTERVAL '1 year'
        AND b.review_scores_rating <= 3
        AND b.review_scores_rating IS NOT NULL
        AND COALESCE(b.number_of_reviews, 0) > 0
    GROUP BY month
    ORDER BY month
)

SELECT * FROM tendencias_satisfacao
