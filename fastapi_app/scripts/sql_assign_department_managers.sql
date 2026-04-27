WITH ranked_team AS (
    SELECT
        e.country,
        e.factory,
        e.department,
        e.team,
        e.employee_id,
        ROW_NUMBER() OVER (
            PARTITION BY e.factory, e.department, e.team
            ORDER BY e.employee_id
        ) AS rn_team
    FROM dev.employees e
),
team_heads AS (
    SELECT
        country,
        factory,
        department,
        team,
        employee_id
    FROM ranked_team
    WHERE rn_team = 1
),
ranked_department AS (
    SELECT
        th.country,
        th.factory,
        th.department,
        th.employee_id,
        ROW_NUMBER() OVER (
            PARTITION BY th.factory, th.department
            ORDER BY th.employee_id
        ) AS rn_department
    FROM team_heads th
),
department_heads AS (
    SELECT
        rd.country,
        rd.factory,
        rd.department,
        rd.employee_id,
        lower(rd.employee_id) AS username
    FROM ranked_department rd
    WHERE rd.rn_department = 1
),
department_head_users AS (
    SELECT
        dh.country,
        dh.factory,
        dh.department,
        dh.employee_id,
        su.id AS manager_user_id
    FROM department_heads dh
    JOIN sm_users su ON su.username = dh.username
),
all_user_locations AS (
    SELECT
        su.id AS user_id,
        su.username,
        e.country,
        e.factory,
        e.department,
        e.team,
        e.employee_id
    FROM sm_users su
    JOIN dev.employees e ON lower(e.employee_id) = su.username
),
manager_team_list AS (
    SELECT
        dhu.employee_id AS manager_employee_id,
        string_agg(DISTINCT e.team, ', ' ORDER BY e.team) AS teams_managed
    FROM department_head_users dhu
    JOIN dev.employees e
      ON e.factory = dhu.factory
     AND e.department = dhu.department
    GROUP BY dhu.employee_id
)
UPDATE sm_users su
SET role = CASE
    WHEN su.id IN (SELECT manager_user_id FROM department_head_users)
        THEN 'manager'::userrole
    ELSE 'employee'::userrole
END
WHERE su.username IN (SELECT username FROM all_user_locations);

WITH department_head_users AS (
    SELECT
        dh.country,
        dh.factory,
        dh.department,
        dh.employee_id,
        su.id AS manager_user_id
    FROM (
        SELECT
            rd.country,
            rd.factory,
            rd.department,
            rd.employee_id,
            lower(rd.employee_id) AS username
        FROM (
            SELECT
                th.country,
                th.factory,
                th.department,
                th.employee_id,
                ROW_NUMBER() OVER (
                    PARTITION BY th.factory, th.department
                    ORDER BY th.employee_id
                ) AS rn_department
            FROM (
                SELECT
                    e.country,
                    e.factory,
                    e.department,
                    e.team,
                    e.employee_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.factory, e.department, e.team
                        ORDER BY e.employee_id
                    ) AS rn_team
                FROM dev.employees e
            ) th
            WHERE th.rn_team = 1
        ) rd
        WHERE rd.rn_department = 1
    ) dh
    JOIN sm_users su ON su.username = dh.username
),
all_user_locations AS (
    SELECT
        su.id AS user_id,
        su.username,
        e.country,
        e.factory,
        e.department
    FROM sm_users su
    JOIN dev.employees e ON lower(e.employee_id) = su.username
)
UPDATE sm_users su
SET manager_id = CASE
    WHEN su.id = dhu.manager_user_id THEN NULL
    ELSE dhu.manager_user_id
END
FROM all_user_locations aul
JOIN department_head_users dhu
  ON dhu.factory = aul.factory
 AND dhu.department = aul.department
WHERE su.id = aul.user_id;

WITH manager_team_list AS (
    SELECT
        dh.employee_id AS manager_employee_id,
        string_agg(DISTINCT e.team, ', ' ORDER BY e.team) AS teams_managed
    FROM (
        SELECT
            rd.factory,
            rd.department,
            rd.employee_id
        FROM (
            SELECT
                th.factory,
                th.department,
                th.employee_id,
                ROW_NUMBER() OVER (
                    PARTITION BY th.factory, th.department
                    ORDER BY th.employee_id
                ) AS rn_department
            FROM (
                SELECT
                    e.factory,
                    e.department,
                    e.team,
                    e.employee_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.factory, e.department, e.team
                        ORDER BY e.employee_id
                    ) AS rn_team
                FROM dev.employees e
            ) th
            WHERE th.rn_team = 1
        ) rd
        WHERE rd.rn_department = 1
    ) dh
    JOIN dev.employees e
      ON e.factory = dh.factory
     AND e.department = dh.department
    GROUP BY dh.employee_id
)
UPDATE dev.employees e
SET
    position = CASE
        WHEN e.employee_id IN (SELECT manager_employee_id FROM manager_team_list)
            THEN 'Менеджер отдела'
        ELSE e.position
    END,
    team = CASE
        WHEN e.employee_id IN (SELECT manager_employee_id FROM manager_team_list)
            THEN (SELECT mtl.teams_managed FROM manager_team_list mtl WHERE mtl.manager_employee_id = e.employee_id)
        ELSE e.team
    END
WHERE e.employee_id IN (
    SELECT manager_employee_id FROM manager_team_list
);

SELECT role, COUNT(*) FROM sm_users GROUP BY role ORDER BY role;
SELECT department, COUNT(*) AS managers_per_department
FROM sm_users
WHERE role = 'manager'
GROUP BY department
ORDER BY department
LIMIT 20;
