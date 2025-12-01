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
    planned_vacation_date
from source