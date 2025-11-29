{{ 
    config(
        materialized='view',
        unique_key='id'
    )
}}

with source as(
    select *
    from {{ source('dev', 'employees') }}
)

select 
    id,
    country,
    factory,
    department,
    team,
    employee_full_name,
    employee_id,
    position,
    age,
    gender,
    competencies,
    planned_vacation_date,
    -- Исправленный синтаксис для конвертации времени
    created_at at time zone 'utc' at time zone 'Europe/Moscow' as created_at_local,
    updated_at at time zone 'utc' at time zone 'Europe/Moscow' as updated_at_local
from source