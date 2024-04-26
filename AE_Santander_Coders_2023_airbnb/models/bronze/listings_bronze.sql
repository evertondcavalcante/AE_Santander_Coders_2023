{{ 
    config(order=2) 
}}
with dados_stage as (
    select * from {{ source('dados_stage', 'listings') }}
),
final as (
    select *
        , now()::timestamp as created_at
    from dados_stage
    
)

select * from final