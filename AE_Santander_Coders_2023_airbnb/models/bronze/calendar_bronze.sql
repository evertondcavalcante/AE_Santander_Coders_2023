{{ 
    config(order=1) 
}}
with dados_stage as (
    select * from {{ source('dados_stage', 'calendar_stage') }}
),
final as (
    select distinct *
        , now()::timestamp as created_at
    from dados_stage
    
)

select * from final