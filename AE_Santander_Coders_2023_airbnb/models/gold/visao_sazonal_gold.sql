--visão que agregue os dados por mês, mostrando o número total de noites disponíveis, 
--a média de preços por noite e o número médio de noites mínimas exigidas pelos anfitriões
WITH dados_listings_silver AS (
    SELECT * FROM {{ source('dados_silver', '2_listings_silver') }}
),
dados_calendar_silver AS (
    SELECT * FROM {{ source('dados_silver', '2_calendar_silver') }}
),
visao_sazonal AS (
    SELECT 
        EXTRACT(YEAR FROM a.date) AS year,
        EXTRACT(MONTH FROM a.date) AS month,
        COUNT(*) AS total_nights_available,
        AVG(a.price) AS average_price_per_night,
        AVG(a.minimum_nights) AS average_minimum_nights_required
    FROM dados_calendar_silver a
    INNER JOIN dados_listings_silver b
        ON a.listing_id = b.id
    WHERE a.available = true
        AND UPPER(b.neighbourhood_cleansed) = 'COPACABANA'
        AND a.price >= 1000
        AND b.host_name IS NOT NULL
        AND a.minimum_nights IS NOT NULL
        AND b.maximum_nights IS NOT NULL
    GROUP BY year, month
    ORDER BY year, month
)

SELECT * FROM visao_sazonal;
