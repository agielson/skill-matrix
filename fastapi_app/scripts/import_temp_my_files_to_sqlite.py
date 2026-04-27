from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from typing import Iterable

import pandas as pd

import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi_app.auth import hash_password


EMPLOYEES_XLSX = ROOT / "temp_my_files" / "employees.xlsx"
TASKS_XLSX = ROOT / "temp_my_files" / "tasks.xlsx"
DB_PATHS = [
    ROOT / "fastapi_app" / "skill_matrix.db",
    ROOT / "fastapi_app" / "skill_matrix_fallback.db",
]


def _clean(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _derive_role(position: str | None) -> str:
    raw = (position or "").lower()
    manager_tokens = ("manager", "head", "lead", "руковод", "директор", "начальник")
    return "manager" if any(token in raw for token in manager_tokens) else "employee"


def _derive_level(position: str | None) -> int:
    raw = (position or "").lower()
    if any(token in raw for token in ("senior", "lead", "руковод", "сеньор")):
        return 3
    if any(token in raw for token in ("middle", "mid", "мидл")):
        return 2
    return 1


def _competencies(raw: str | None) -> list[str]:
    if not raw:
        return []
    normalized = raw.replace(";", ",").replace("|", ",")
    return [item.strip() for item in normalized.split(",") if item.strip()]


def _ensure_dev_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS "dev.flask_users" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name TEXT NOT NULL UNIQUE,
            employee_id TEXT,
            password_hash TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS "dev.employees" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id TEXT NOT NULL UNIQUE,
            employee_full_name TEXT NOT NULL,
            department TEXT,
            team TEXT,
            factory TEXT,
            country TEXT,
            competencies TEXT,
            position TEXT,
            age INTEGER DEFAULT 30,
            gender TEXT DEFAULT 'unknown',
            planned_vacation_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS "dev.tasks" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL UNIQUE,
            task_name TEXT NOT NULL,
            department TEXT NOT NULL,
            team TEXT NOT NULL,
            factory TEXT NOT NULL,
            country TEXT NOT NULL,
            required_competency TEXT NOT NULL,
            required_position TEXT NOT NULL,
            deadline TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS "dev.task_status" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL UNIQUE,
            status TEXT DEFAULT 'new',
            priority INTEGER DEFAULT 3,
            assigned_employee_id TEXT,
            assigned_at TEXT,
            completed_at TEXT,
            notes TEXT,
            updated_by TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS "dev.employee_competency_level" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id TEXT NOT NULL,
            competency TEXT NOT NULL,
            level INTEGER DEFAULT 1,
            confirmed_by TEXT,
            confirmed_at TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(employee_id, competency)
        )
        """
    )


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _id_map(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT su.id, de.employee_id
        FROM sm_users su
        LEFT JOIN "dev.flask_users" dfu ON dfu.user_name = su.username
        LEFT JOIN "dev.employees" de ON de.employee_id = dfu.employee_id
        """
    ).fetchall()
    return {str(employee_id): int(user_id) for user_id, employee_id in rows if employee_id is not None}


def _assign_tasks(
    tasks_df: pd.DataFrame,
    employees_df: pd.DataFrame,
) -> Iterable[tuple[str, str | None, int, str | None]]:
    staff_by_team: dict[tuple[str, str], list[dict[str, str | list[str]]]] = {}
    for _, row in employees_df.iterrows():
        dept = _clean(row.get("Department")) or ""
        team = _clean(row.get("Team")) or ""
        staff_by_team.setdefault((dept, team), []).append(
            {
                "employee_id": _clean(row.get("Employee_ID")) or "",
                "competencies": _competencies(_clean(row.get("Competencies"))),
            }
        )

    counters: dict[tuple[str, str], int] = {}
    for _, row in tasks_df.iterrows():
        task_id = _clean(row.get("Task_ID"))
        if not task_id:
            continue
        required = set(_competencies(_clean(row.get("Required_Competency"))))
        dept = _clean(row.get("Department")) or ""
        team = _clean(row.get("Team")) or ""
        pool = staff_by_team.get((dept, team)) or []
        if not pool:
            yield task_id, None, 3, "new"
            continue

        ranked = [
            member
            for member in pool
            if required.intersection(set(member["competencies"]))  # type: ignore[arg-type]
        ] or pool
        idx = counters.get((dept, team), 0) % len(ranked)
        counters[(dept, team)] = idx + 1
        assignee = str(ranked[idx]["employee_id"])
        priority = 2 if len(required) <= 1 else 1
        yield task_id, assignee, priority, "in_progress"


def import_into_db(db_path: Path) -> None:
    if not db_path.exists():
        print(f"[skip] {db_path} does not exist")
        return

    employees_df = pd.read_excel(EMPLOYEES_XLSX, engine="openpyxl").where(pd.notnull, None)
    tasks_df = pd.read_excel(TASKS_XLSX, engine="openpyxl").where(pd.notnull, None)
    pwd_hash = hash_password("123456")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        _ensure_dev_tables(conn)

        if _table_exists(conn, "sm_task_proposals"):
            conn.execute("DELETE FROM sm_task_proposals")
        conn.execute("DELETE FROM sm_notifications")
        conn.execute("DELETE FROM sm_tasks")
        conn.execute("DELETE FROM sm_employee_competencies")
        conn.execute("DELETE FROM sm_competencies")
        conn.execute("DELETE FROM sm_users")
        conn.execute('DELETE FROM "dev.task_status"')
        conn.execute('DELETE FROM "dev.tasks"')
        conn.execute('DELETE FROM "dev.employee_competency_level"')
        conn.execute('DELETE FROM "dev.employees"')
        conn.execute('DELETE FROM "dev.flask_users"')

        # 1) users + legacy employees
        managers: dict[str, int] = {}
        for _, row in employees_df.iterrows():
            employee_id = _clean(row.get("Employee_ID"))
            full_name = _clean(row.get("Employee_Full_Name"))
            if not employee_id or not full_name:
                continue
            dept = _clean(row.get("Department"))
            team = _clean(row.get("Team"))
            position = _clean(row.get("Position"))
            country = _clean(row.get("Country"))
            factory = _clean(row.get("Factory"))
            age = _clean(row.get("Age"))
            gender = _clean(row.get("Gender"))
            competency_text = _clean(row.get("Competencies"))
            vacation = _clean(row.get("Vacation_Date"))
            username = employee_id.lower()
            role = _derive_role(position)

            conn.execute(
                """
                INSERT INTO sm_users (username, password_hash, full_name, department, role, manager_id, is_active)
                VALUES (?, ?, ?, ?, ?, NULL, 1)
                """,
                (username, pwd_hash, full_name, dept, role),
            )
            user_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            if role == "manager":
                managers[dept or ""] = user_id

            conn.execute(
                """
                INSERT INTO "dev.flask_users" (user_name, employee_id, password_hash)
                VALUES (?, ?, ?)
                """,
                (username, employee_id, pwd_hash),
            )
            conn.execute(
                """
                INSERT INTO "dev.employees" (
                    employee_id, employee_full_name, department, team, factory, country,
                    competencies, position, age, gender, planned_vacation_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (employee_id, full_name, dept, team, factory, country, competency_text, position, age, gender, vacation),
            )

            level = _derive_level(position)
            for competency in _competencies(competency_text):
                conn.execute(
                    """
                    INSERT INTO "dev.employee_competency_level" (employee_id, competency, level, confirmed_by)
                    VALUES (?, ?, ?, 'import')
                    """,
                    (employee_id, competency, level),
                )

        # 2) link employees to manager by department
        user_rows = conn.execute(
            """
            SELECT su.id, su.department, su.role
            FROM sm_users su
            """
        ).fetchall()
        default_manager = next((row[0] for row in user_rows if row[2] == "manager"), None)
        for uid, dept, role in user_rows:
            if role != "employee":
                continue
            manager_id = managers.get(dept or "") or default_manager
            if manager_id and manager_id != uid:
                conn.execute("UPDATE sm_users SET manager_id = ? WHERE id = ?", (manager_id, uid))

        # 3) competencies in sm_* and user links
        competency_ids: dict[str, int] = {}
        for _, row in employees_df.iterrows():
            employee_id = _clean(row.get("Employee_ID"))
            if not employee_id:
                continue
            username = employee_id.lower()
            user_row = conn.execute("SELECT id FROM sm_users WHERE username = ?", (username,)).fetchone()
            if not user_row:
                continue
            user_id = int(user_row[0])
            level = _derive_level(_clean(row.get("Position")))
            for competency in _competencies(_clean(row.get("Competencies"))):
                comp_id = competency_ids.get(competency)
                if not comp_id:
                    conn.execute("INSERT INTO sm_competencies (name) VALUES (?)", (competency,))
                    comp_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                    competency_ids[competency] = comp_id
                conn.execute(
                    """
                    INSERT INTO sm_employee_competencies (employee_id, competency_id, level)
                    VALUES (?, ?, ?)
                    """,
                    (user_id, comp_id, level),
                )

        # 4) tasks in dev.* and sm_tasks + task_status assignment
        user_id_by_employee_id = _id_map(conn)
        manager_by_department = {
            dept: mid for dept, mid in managers.items() if mid is not None
        }
        default_creator = next(iter(manager_by_department.values()), 1)

        assignments = list(_assign_tasks(tasks_df, employees_df))
        assignment_map = {task_id: (employee_id, priority, status) for task_id, employee_id, priority, status in assignments}

        for _, row in tasks_df.iterrows():
            task_id = _clean(row.get("Task_ID"))
            task_name = _clean(row.get("Task_Name"))
            if not task_id or not task_name:
                continue
            dept = _clean(row.get("Department"))
            team = _clean(row.get("Team"))
            factory = _clean(row.get("Factory")) or "Unknown"
            country = _clean(row.get("Country")) or "Unknown"
            required_comp = _clean(row.get("Required_Competency")) or "General"
            required_pos = _clean(row.get("Required_Position")) or "Employee"
            deadline = _clean(row.get("Deadline")) or "2026-12-31"

            conn.execute(
                """
                INSERT INTO "dev.tasks" (
                    task_id, task_name, department, team, factory, country,
                    required_competency, required_position, deadline
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (task_id, task_name, dept, team, factory, country, required_comp, required_pos, deadline),
            )

            assignee_employee_id, priority, status = assignment_map.get(task_id, (None, 3, "new"))
            assignee_user_id = user_id_by_employee_id.get(str(assignee_employee_id)) if assignee_employee_id else None
            creator_id = manager_by_department.get(dept or "") or default_creator

            required_map: dict[str, int] = {}
            for competency in _competencies(required_comp):
                comp_id = competency_ids.get(competency)
                if not comp_id:
                    conn.execute("INSERT INTO sm_competencies (name) VALUES (?)", (competency,))
                    comp_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                    competency_ids[competency] = comp_id
                required_map[str(comp_id)] = 2

            conn.execute(
                """
                INSERT INTO sm_tasks (
                    title, description, department, status, priority, required_competencies,
                    created_by_id, assigned_to_id, created_at
                ) VALUES (?, ?, ?, ?, ?, json(?), ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    task_name,
                    f"Импортировано из Excel: {task_id}",
                    dept,
                    status,
                    priority,
                    json.dumps(required_map, ensure_ascii=False),
                    creator_id,
                    assignee_user_id,
                ),
            )

            conn.execute(
                """
                INSERT INTO "dev.task_status" (
                    task_id, status, priority, assigned_employee_id, updated_by
                ) VALUES (?, ?, ?, ?, 'import')
                """,
                (task_id, status, priority, assignee_employee_id),
            )

        conn.commit()
        users_count = conn.execute("SELECT COUNT(*) FROM sm_users").fetchone()[0]
        tasks_count = conn.execute("SELECT COUNT(*) FROM sm_tasks").fetchone()[0]
        print(f"[ok] {db_path.name}: users={users_count}, tasks={tasks_count}")
    finally:
        conn.close()


def main() -> None:
    if not EMPLOYEES_XLSX.exists() or not TASKS_XLSX.exists():
        raise FileNotFoundError("Expected temp_my_files/employees.xlsx and temp_my_files/tasks.xlsx")
    for db_path in DB_PATHS:
        import_into_db(db_path)
    print("Import completed.")


if __name__ == "__main__":
    main()
