from __future__ import annotations

from pathlib import Path

import asyncio
import bcrypt
import pandas as pd
import asyncpg


ROOT = Path(__file__).resolve().parents[2]
EMPLOYEES_XLSX = ROOT / "temp_my_files" / "employees.xlsx"


def clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def derive_role(position: str | None) -> str:
    raw = (position or "").lower()
    manager_tokens = ("manager", "head", "lead", "руковод", "директор", "начальник")
    return "manager" if any(token in raw for token in manager_tokens) else "employee"


async def main() -> None:
    df = pd.read_excel(EMPLOYEES_XLSX, engine="openpyxl").where(pd.notnull, None)
    password_hash = bcrypt.hashpw("123456".encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=5432,
        database="db",
        user="db_user",
        password="db_password",
    )
    try:
        manager_by_department: dict[str, int] = {}
        user_rows: list[tuple[int, str, str]] = []

        for _, row in df.iterrows():
            employee_id = clean(row.get("Employee_ID"))
            full_name = clean(row.get("Employee_Full_Name"))
            department = clean(row.get("Department"))
            if not employee_id or not full_name:
                continue
            username = employee_id.lower()
            role = derive_role(clean(row.get("Position")))

            rec = await conn.fetchrow(
                """
                INSERT INTO sm_users (username, password_hash, full_name, department, role, manager_id, is_active)
                VALUES ($1, $2, $3, $4, $5, NULL, TRUE)
                ON CONFLICT (username) DO UPDATE SET
                    password_hash = EXCLUDED.password_hash,
                    full_name = EXCLUDED.full_name,
                    department = EXCLUDED.department,
                    role = EXCLUDED.role,
                    is_active = TRUE
                RETURNING id, department, role
                """,
                username,
                password_hash,
                full_name,
                department,
                role,
            )
            user_id = int(rec["id"])
            dept_key = str(rec["department"] or "").strip()
            user_role = str(rec["role"])
            if user_role == "manager" and dept_key and dept_key not in manager_by_department:
                manager_by_department[dept_key] = user_id
            user_rows.append((user_id, dept_key, user_role))

        default_manager_id = next((u[0] for u in user_rows if u[2] == "manager"), None)
        for user_id, dept, role in user_rows:
            if role != "employee":
                continue
            manager_id = manager_by_department.get(dept) or default_manager_id
            if manager_id and manager_id != user_id:
                await conn.execute("UPDATE sm_users SET manager_id = $1 WHERE id = $2", manager_id, user_id)

        total = await conn.fetchval("SELECT COUNT(*) FROM sm_users")
        print(f"sm_users synced: {total}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
