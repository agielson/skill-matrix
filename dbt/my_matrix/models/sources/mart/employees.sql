{{
    config(
        materialized='table',
        unique_key='id'
    )
}}

select * 
from {{ ref ('employees_staging')}}