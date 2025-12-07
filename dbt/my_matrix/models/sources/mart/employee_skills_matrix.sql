{{
    config(
        materialized='table'
    )
}}

SELECT 
    -- 1. Поля из таблицы employees_staging (e)
    e.id AS employee_internal_id,
    e.employee_id,
    e.employee_full_name,
    e.position AS employee_position, -- Явное переименование
    e.department AS employee_department, -- ✅ Алиас
    e.team AS employee_team, -- ✅ Алиас
    e.factory AS employee_factory, -- ✅ Алиас
    e.country AS employee_country, -- ✅ Алиас
    e.age,
    e.gender,
    e.competencies AS employee_all_competencies,
    e.planned_vacation_date,

    -- 2. Поля из таблицы tasks_staging (t)
    t.id AS task_internal_id,
    t.task_id,
    t.task_name,
    t.department AS task_department, -- ✅ Алиас
    t.team AS task_team, -- ✅ Алиас
    t.factory AS task_factory, -- ✅ Алиас
    t.country AS task_country, -- ✅ Алиас
    t.required_competency,
    t.required_position,
    t.deadline,
    
    -- 3. Флаг совпадения
    CASE WHEN e.position = t.required_position THEN TRUE ELSE FALSE END AS position_match

FROM 
    {{ ref('employees_staging') }} e
LEFT JOIN 
    {{ ref('tasks_staging') }} t
        -- Соединение по логике, которую вы использовали в предыдущем запросе, 
        -- но с учетом полей e.* и t.*:
        ON e.position = t.required_position
        AND e.country = t.country 
        AND e.factory = t.factory
        
-- Мы используем LEFT JOIN, чтобы сохранить всех сотрудников (e),
-- даже если для них не нашлось подходящей задачи (t) по условию.