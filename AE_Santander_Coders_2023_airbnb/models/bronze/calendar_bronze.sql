{{ 
    config(order=1) 
}}
with dados_stage as (
    select * from {{ source('dados_stage', 'calendar') }}
),
final as (
    select *
        , now()::timestamp as created_at
    from dados_stage
    
)

select * from final