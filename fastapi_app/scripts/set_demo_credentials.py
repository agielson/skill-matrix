import bcrypt
import sqlite3
from pathlib import Path


password_hash = bcrypt.hashpw(b"123456", bcrypt.gensalt()).decode("utf-8")
base_dir = Path(__file__).resolve().parents[1]
db_paths = {
    base_dir / "skill_matrix.db",
    base_dir / "skill_matrix_fallback.db",
    base_dir / "fastapi_app" / "skill_matrix.db",
    base_dir / "fastapi_app" / "skill_matrix_fallback.db",
}
for candidate in base_dir.rglob("skill_matrix*.db"):
    db_paths.add(candidate)

for db_path in sorted(db_paths):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sm_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(100) UNIQUE,
            password_hash VARCHAR(255),
            full_name VARCHAR(200),
            department VARCHAR(100),
            role VARCHAR(8),
            manager_id INTEGER,
            is_active BOOLEAN
        )
        """
    )

    cur.execute(
        """
        INSERT INTO sm_users (username, password_hash, full_name, department, role, is_active)
        VALUES (?, ?, ?, ?, ?, 1)
        ON CONFLICT(username) DO UPDATE SET
            password_hash = excluded.password_hash,
            full_name = excluded.full_name,
            department = excluded.department,
            role = excluded.role,
            is_active = 1
        """,
        ("manager_hr", password_hash, "Петухов Василий Федотович", "HR", "manager"),
    )
    cur.execute("SELECT id FROM sm_users WHERE username = ?", ("manager_hr",))
    manager_id = cur.fetchone()[0]

    cur.execute(
        """
        INSERT INTO sm_users (username, password_hash, full_name, department, role, manager_id, is_active)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(username) DO UPDATE SET
            password_hash = excluded.password_hash,
            full_name = excluded.full_name,
            department = excluded.department,
            role = excluded.role,
            manager_id = excluded.manager_id,
            is_active = 1
        """,
        ("fil_genad", password_hash, "Филатова Элеонора Геннадьевна", "Логистика", "employee", manager_id),
    )

    cur.execute(
        """
        INSERT INTO sm_users (username, password_hash, full_name, department, role, is_active)
        VALUES (?, ?, ?, ?, ?, 1)
        ON CONFLICT(username) DO UPDATE SET
            password_hash = excluded.password_hash,
            full_name = excluded.full_name,
            department = excluded.department,
            role = excluded.role,
            is_active = 1
        """,
        ("manager1", password_hash, "Демо Менеджер", "demo", "manager"),
    )
    cur.execute("SELECT id FROM sm_users WHERE username = ?", ("manager1",))
    manager1_id = cur.fetchone()[0]

    cur.execute(
        """
        INSERT INTO sm_users (username, password_hash, full_name, department, role, manager_id, is_active)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(username) DO UPDATE SET
            password_hash = excluded.password_hash,
            full_name = excluded.full_name,
            department = excluded.department,
            role = excluded.role,
            manager_id = excluded.manager_id,
            is_active = 1
        """,
        ("employee1", password_hash, "Демо Сотрудник", "demo", "employee", manager1_id),
    )

    conn.commit()
    cur.execute(
        """
        SELECT username, role, department, manager_id
        FROM sm_users
        WHERE username IN ('manager_hr', 'fil_genad', 'manager1', 'employee1')
        ORDER BY username
        """
    )
    print(db_path.name, cur.fetchall())
    conn.close()
