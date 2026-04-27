DROP TABLE IF EXISTS tmp_employees_import;

CREATE TABLE tmp_employees_import (
    country text,
    factory text,
    department text,
    team text,
    employee_full_name text,
    employee_id text,
    position text,
    age text,
    gender text,
    competencies text,
    vacation_date text
);

COPY tmp_employees_import
FROM '/tmp/employees_import.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');

INSERT INTO sm_users (username, password_hash, full_name, department, role, manager_id, is_active)
SELECT
    lower(trim(employee_id)) AS username,
    '$2b$12$EaFEEuz2gcqyqHk2ACHfwul3HtZgqRRGwfwyip/jXqmrE31C1tEDe' AS password_hash,
    trim(employee_full_name) AS full_name,
    NULLIF(trim(department), '') AS department,
    CASE
        WHEN lower(coalesce(position, '')) SIMILAR TO '%(manager|head|lead|руковод|директор|начальник)%'
            THEN 'manager'::userrole
        ELSE 'employee'::userrole
    END AS role,
    NULL::integer AS manager_id,
    TRUE AS is_active
FROM tmp_employees_import
WHERE coalesce(trim(employee_id), '') <> ''
ON CONFLICT (username) DO UPDATE SET
    password_hash = EXCLUDED.password_hash,
    full_name = EXCLUDED.full_name,
    department = EXCLUDED.department,
    role = EXCLUDED.role,
    is_active = TRUE;

WITH mgr AS (
    SELECT department, MIN(id) AS manager_id
    FROM sm_users
    WHERE role = 'manager' AND department IS NOT NULL
    GROUP BY department
),
fallback_mgr AS (
    SELECT MIN(id) AS manager_id
    FROM sm_users
    WHERE role = 'manager'
)
UPDATE sm_users u
SET manager_id = COALESCE(mgr.manager_id, fallback_mgr.manager_id)
FROM fallback_mgr
LEFT JOIN mgr ON mgr.department = u.department
WHERE u.role = 'employee'
  AND u.id <> COALESCE(mgr.manager_id, fallback_mgr.manager_id);

SELECT COUNT(*) AS users_total FROM sm_users;
SELECT username, role FROM sm_users ORDER BY id LIMIT 20;
