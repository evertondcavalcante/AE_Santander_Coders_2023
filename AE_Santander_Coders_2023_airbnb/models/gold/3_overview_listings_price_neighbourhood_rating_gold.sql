with dados_listings_silver as (
    select * from {{ source('dados_silver', '2_listings_silver') }}
),
final as (
    select neighbourhood_cleansed
        , case 
            when review_scores_rating is null then 'Sem avaliação'
            when review_scores_rating >= 0 and review_scores_rating <= 1 then 'Até 1 estrela'
            when review_scores_rating > 1 and review_scores_rating <= 2 then 'Até 2 estrela'
            when review_scores_rating > 2 and review_scores_rating <= 3 then 'Até 3 estrela'
            when review_scores_rating > 3 and review_scores_rating <= 4 then 'Até 4 estrela'
            when review_scores_rating > 4 and review_scores_rating <= 5 then 'Até 5 estrela'
        end classificacao_imovel
        , count(distinct id) qtd_imoveis
        , avg(price) preco_medio
        , max(price) preco_teto
        , min(price) preco_minimo
    from dados_listings_silver
    where has_availability = true
        and neighbourhood_cleansed is not null
    group by neighbourhood_cleansed
        , case 
            when review_scores_rating is null then 'Sem avaliação'
            when review_scores_rating >= 0 and review_scores_rating <= 1 then 'Até 1 estrela'
            when review_scores_rating > 1 and review_scores_rating <= 2 then 'Até 2 estrela'
            when review_scores_rating > 2 and review_scores_rating <= 3 then 'Até 3 estrela'
            when review_scores_rating > 3 and review_scores_rating <= 4 then 'Até 4 estrela'
            when review_scores_rating > 4 and review_scores_rating <= 5 then 'Até 5 estrela'
        end
    order by 1,2
)

select * from final