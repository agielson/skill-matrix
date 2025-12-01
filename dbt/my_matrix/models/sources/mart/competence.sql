{{
    config(
        materialized='table',
        unique_key='id'
    )
}}

with source as (
    select 
        employee_id,
        competencies
    from {{ source('dev', 'employees') }}
    where competencies is not null and competencies != ''
),

competencies_split as (
    select 
        employee_id,
        trim(unnest(string_to_array(competencies, ','))) as competency
    from source
),

competencies_with_order as (
    select 
        employee_id,
        competency,
        row_number() over (partition by employee_id order by competency) as competency_order,
        md5(employee_id || competency) as row_id
    from competencies_split
)

select 
    employee_id,
    competency,
    competency_order
from competencies_with_order