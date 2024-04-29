--visão da análise da distribuição de tipos de propriedades em diferentes bairros.
WITH dados_listings_silver AS (
    SELECT * FROM {{ source('dados_silver', '2_listings_silver') }}
),
distribuicao_tipos_imoveis AS (
    SELECT 
        neighbourhood_cleansed,
        property_type,
        COUNT(*) AS qtd_imoveis
    FROM dados_listings_silver
    WHERE has_availability = true
        AND neighbourhood_cleansed IS NOT NULL
        AND property_type IS NOT NULL
    GROUP BY neighbourhood_cleansed, property_type
    ORDER BY neighbourhood_cleansed, property_type
)

SELECT * FROM distribuicao_tipos_imoveis;
