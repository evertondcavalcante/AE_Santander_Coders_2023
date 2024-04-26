{{ 
    config(order=3) 
}}
with dados_stage as (
    select * from {{ source('dados_stage', 'reviews') }}
),
final as (
    select *
        , now()::timestamp as created_at
    from dados_stage
    
)

select * from final