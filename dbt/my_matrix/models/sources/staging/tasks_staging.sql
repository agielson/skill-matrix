{{ 
    config(
        materialized='view',
        unique_key='id'
    )
}}

with source as(
    select *
    from {{ source('dev', 'tasks') }}
)

select 
    id,
    task_id,
    task_name,
    department,
    team,
    factory,
    country,
    required_competency,
    required_position,
    deadline,
    created_at at time zone 'utc' at time zone 'Europe/Moscow' as created_at_local
from source