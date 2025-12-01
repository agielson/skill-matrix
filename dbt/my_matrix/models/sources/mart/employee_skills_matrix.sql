{{
    config(
        materialized='table'
    )
}}

-- Первый джойн: сотрудники + компетенции
with employees_competencies as (
    select 
        -- Все поля из employees
        e.id as employee_internal_id,
        e.employee_id,
        e.employee_full_name,
        e.position,
        e.department as employee_department,
        e.team as employee_team,
        e.factory as employee_factory,
        e.country as employee_country,
        e.age,
        e.gender,
        e.competencies as employee_all_competencies,
        e.planned_vacation_date,
        
        -- Все поля из компетенций
        ec.competency,
        ec.competency_order
        
    from {{ ref('employees_staging') }} e
    inner join {{ ref('competence') }} ec 
        on e.employee_id = ec.employee_id
),

-- Второй джойн: результат первого джойна + задачи
skills_matrix as (
    select 
        -- Все поля из первого джойна (сотрудники + компетенции)
        ec.*,
        
        -- Все поля из tasks
        t.id as task_internal_id,
        t.task_id,
        t.task_name,
        t.department as task_department,
        t.team as task_team,
        t.factory as task_factory,
        t.country as task_country,
        t.required_competency,
        t.required_position,
        t.deadline,
        t.created_at_local as task_created_at
        
    from employees_competencies ec
    inner join {{ ref('tasks_staging') }} t 
        on ec.competency = t.required_competency  -- по компетенциям
        and ec.employee_country = t.country       -- по стране
        and ec.employee_factory = t.factory       -- по фабрике
)

select 
    
    employee_id,
    employee_full_name,
    position as employee_position,
    employee_department,
    employee_team,
    employee_factory,
    employee_country,
    age,
    gender,
    employee_all_competencies,
    planned_vacation_date
    
    -- Информация о компетенции
    competency,
    competency_order,
    
    -- Информация о задаче
    task_id,
    task_name,
    task_department,
    task_team,
    task_factory,
    task_country,
    required_competency,
    required_position,
    deadline,
    task_created_at,
    
    -- Флаги совпадений для фильтрации
    case when position = required_position then true else false end as position_match
    
from skills_matrix