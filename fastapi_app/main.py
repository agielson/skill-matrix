import re
import asyncio
import logging
from datetime import date, timedelta

from fastapi import FastAPI
from sqlalchemy import select, text

from .auth import hash_password
from .config import settings
from . import database as db
from .database import Base
from .legacy_sql import legacy_table_names
from .models import Competency, EmployeeCompetency, Notification, Task, User, UserRole
from .routers import auth, employees, notifications, tasks

app = FastAPI(title="API матрицы компетенций", version="0.1.0")
logger = logging.getLogger(__name__)

app.include_router(auth.router, prefix=settings.api_prefix)
app.include_router(employees.router, prefix=settings.api_prefix)
app.include_router(tasks.router, prefix=settings.api_prefix)
app.include_router(notifications.router, prefix=settings.api_prefix)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.on_event("startup")
async def on_startup() -> None:
    retries = 5
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            async with db.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            last_error = None
            break
        except Exception as exc:  # transient network/db startup failures on Windows/LAN
            last_error = exc
            logger.warning("DB startup attempt %s/%s failed: %s", attempt, retries, exc)
            if attempt < retries:
                await asyncio.sleep(2 * attempt)
    if last_error is not None:
        if settings.database_url and settings.database_url.startswith("postgresql") and settings.allow_sqlite_fallback:
            fallback_url = "sqlite+aiosqlite:///./fastapi_app/skill_matrix_fallback.db"
            logger.warning("Falling back to local SQLite DB because PostgreSQL is unavailable: %s", fallback_url)
            db.configure_engine(fallback_url)
            async with db.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        else:
            raise last_error

    await normalize_sqlite_legacy_enums()
    await ensure_sqlite_legacy_tables()

    imported = False
    await ensure_legacy_support_tables()
    if settings.bootstrap_from_legacy:
        imported = await bootstrap_from_legacy_data()
    if not imported:
        await seed_demo_data()
    await ensure_default_login_users()
    await sync_sqlite_legacy_mirror()
    await backfill_assignments_for_visible_tasks()


async def normalize_sqlite_legacy_enums() -> None:
    """SQLite may contain older SQLAlchemy enum *names* (NEW, MANAGER) instead of values (new, manager)."""
    if not str(db.engine.url).startswith("sqlite"):
        return
    stmts = [
        "UPDATE sm_users SET role = 'manager' WHERE role IN ('MANAGER', 'Manager')",
        "UPDATE sm_users SET role = 'employee' WHERE role IN ('EMPLOYEE', 'Employee')",
        "UPDATE sm_tasks SET status = 'new' WHERE status IN ('NEW', 'New')",
        "UPDATE sm_tasks SET status = 'in_progress' WHERE status IN ('IN_PROGRESS', 'In_progress', 'IN PROGRESS')",
        "UPDATE sm_tasks SET status = 'done' WHERE status IN ('DONE', 'Done')",
        "UPDATE sm_tasks SET status = 'blocked' WHERE status IN ('BLOCKED', 'Blocked')",
    ]
    async with db.engine.begin() as conn:
        for stmt in stmts:
            await conn.execute(text(stmt))


async def ensure_sqlite_legacy_tables() -> None:
    """When using local SQLite fallback, create dev.* legacy tables (PostgreSQL has real schemas)."""
    if not str(db.engine.url).startswith("sqlite"):
        return
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS "dev.flask_users" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name TEXT NOT NULL UNIQUE,
            employee_id TEXT,
            password_hash TEXT
        )
        """,
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
        """,
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
        """,
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
        """,
    ]
    async with db.engine.begin() as conn:
        for stmt in ddl:
            await conn.execute(text(stmt))


async def sync_sqlite_legacy_mirror() -> None:
    """Mirror sm_users into dev.flask_users / dev.employees so legacy SQL works on SQLite."""
    if not str(db.engine.url).startswith("sqlite"):
        return
    t = legacy_table_names("sqlite")
    async with db.SessionLocal() as session:
        users = (await session.execute(select(User))).scalars().all()
        for u in users:
            eid = "EMP0001" if u.username == "manager1" else str(u.id)
            display_name = "Олег Олегович Олегов" if u.username == "manager1" else u.full_name
            dept = "it" if u.username == "manager1" else (u.department or "Unassigned")
            team_label = "it - разработчики" if u.username == "manager1" else f"{dept} team"
            await session.execute(
                text(
                    f"""
                    INSERT INTO {t["flask_users"]} (user_name, employee_id, password_hash)
                    VALUES (:un, :eid, '')
                    ON CONFLICT(user_name) DO UPDATE SET employee_id = excluded.employee_id
                    """
                ),
                {"un": u.username, "eid": eid},
            )
            comp_rows = (
                await session.execute(
                    select(Competency.name)
                    .join(EmployeeCompetency, EmployeeCompetency.competency_id == Competency.id)
                    .where(EmployeeCompetency.employee_id == u.id)
                )
            ).scalars().all()
            comp_str = ", ".join(comp_rows) if comp_rows else ""
            pos = "Manager" if u.role == UserRole.MANAGER else "Employee"
            await session.execute(
                text(
                    f"""
                    INSERT INTO {t["employees"]} (
                        employee_id, employee_full_name, department, team, factory, country, position, competencies
                    )
                    VALUES (:eid, :fn, :dept, :team, :fact, :ct, :pos, :comp)
                    ON CONFLICT(employee_id) DO UPDATE SET
                        employee_full_name = excluded.employee_full_name,
                        department = excluded.department,
                        team = excluded.team,
                        competencies = excluded.competencies,
                        position = excluded.position
                    """
                ),
                {
                    "eid": eid,
                    "fn": display_name,
                    "dept": dept,
                    "team": team_label,
                    "fact": "Local",
                    "ct": "RU",
                    "pos": pos,
                    "comp": comp_str,
                },
            )
        tasks = (await session.execute(select(Task))).scalars().all()
        for task in tasks:
            creator = await session.get(User, task.created_by_id)
            creator_name = creator.username if creator else "system"
            task_comp_names: list[str] = []
            for competency_id in task.required_competencies.keys():
                if str(competency_id).isdigit():
                    competency = await session.get(Competency, int(competency_id))
                    if competency:
                        task_comp_names.append(f"{competency.name}:{task.required_competencies[competency_id]}")
            if not task_comp_names and task.required_competencies:
                task_comp_names = [f"{name}:{lvl}" for name, lvl in task.required_competencies.items()]
            required_comp = ", ".join(task_comp_names) if task_comp_names else "General:2"
            legacy_task_id = f"SM-{task.id}"
            await session.execute(
                text(
                    f"""
                    INSERT INTO {t["tasks"]} (
                        task_id, task_name, department, team, factory, country, required_competency, required_position, deadline
                    )
                    VALUES (:task_id, :task_name, :department, :team, :factory, :country, :required_competency, :required_position, :deadline)
                    ON CONFLICT(task_id) DO UPDATE SET
                        task_name = excluded.task_name,
                        department = excluded.department,
                        required_competency = excluded.required_competency
                    """
                ),
                {
                    "task_id": legacy_task_id,
                    "task_name": task.title,
                    "department": task.department or "Unassigned",
                    "team": (task.department or "Unassigned") + " team",
                    "factory": "Local",
                    "country": "RU",
                    "required_competency": required_comp,
                    "required_position": "Employee",
                    "deadline": (date.today() + timedelta(days=14)).isoformat(),
                },
            )
            assigned_employee_id = None
            if task.assigned_to_id:
                assignee = await session.get(User, task.assigned_to_id)
                assigned_employee_id = str(assignee.id) if assignee else None
            await session.execute(
                text(
                    f"""
                    INSERT INTO {t["task_status"]} (task_id, status, priority, assigned_employee_id, updated_by)
                    VALUES (:task_id, :status, :priority, :assigned_employee_id, :updated_by)
                    ON CONFLICT(task_id) DO UPDATE SET
                        status = excluded.status,
                        priority = excluded.priority,
                        assigned_employee_id = excluded.assigned_employee_id,
                        updated_by = excluded.updated_by,
                        updated_at = CURRENT_TIMESTAMP
                    """
                ),
                {
                    "task_id": legacy_task_id,
                    "status": task.status.value,
                    "priority": task.priority,
                    "assigned_employee_id": assigned_employee_id,
                    "updated_by": creator_name,
                },
            )
        await session.commit()


def _schema_name() -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]", "", settings.legacy_schema)
    return normalized or "dev"


def _map_legacy_role(raw_role: str | None) -> UserRole:
    role = (raw_role or "").lower()
    if role in {"manager", "team_lead", "admin", "hr"}:
        return UserRole.MANAGER
    return UserRole.EMPLOYEE


def _split_competencies(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [item.strip() for item in re.split(r"[,;|]", raw_value) if item and item.strip()]


async def _legacy_table_exists(schema: str, table_name: str) -> bool:
    try:
        async with db.SessionLocal() as session:
            exists = (
                await session.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = :schema
                              AND table_name = :table_name
                        )
                        """
                    ),
                    {"schema": schema, "table_name": table_name},
                )
            ).scalar_one()
        return bool(exists)
    except Exception:
        return False


async def _legacy_column_exists(schema: str, table_name: str, column_name: str) -> bool:
    try:
        async with db.SessionLocal() as session:
            exists = (
                await session.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema = :schema
                              AND table_name = :table_name
                              AND column_name = :column_name
                        )
                        """
                    ),
                    {"schema": schema, "table_name": table_name, "column_name": column_name},
                )
            ).scalar_one()
        return bool(exists)
    except Exception:
        return False


async def bootstrap_from_legacy_data() -> bool:
    schema = _schema_name()
    if not await _legacy_table_exists(schema, "flask_users"):
        return False

    async with db.SessionLocal() as session:
        employees_by_id: dict[str, dict[str, str | None]] = {}
        if await _legacy_table_exists(schema, "employees"):
            employee_rows = (
                await session.execute(
                    text(
                        f"""
                        SELECT employee_id, employee_full_name, department, competencies
                        FROM {schema}.employees
                        """
                    )
                )
            ).mappings().all()
            for row in employee_rows:
                employees_by_id[str(row["employee_id"])] = {
                    "full_name": row["employee_full_name"],
                    "department": row["department"],
                    "competencies": row["competencies"],
                }

        has_roles = await _legacy_table_exists(schema, "roles")
        has_role_id = await _legacy_column_exists(schema, "flask_users", "role_id")
        has_role_name = await _legacy_column_exists(schema, "flask_users", "role")

        if has_roles and has_role_id:
            legacy_users_query = text(
                f"""
                SELECT fu.user_name, fu.password_hash, fu.employee_id, COALESCE(r.name, 'employee') AS role_name
                FROM {schema}.flask_users fu
                LEFT JOIN {schema}.roles r ON fu.role_id = r.id
                """
            )
        elif has_role_name:
            legacy_users_query = text(
                f"""
                SELECT fu.user_name, fu.password_hash, fu.employee_id, COALESCE(fu.role, 'employee') AS role_name
                FROM {schema}.flask_users fu
                """
            )
        else:
            legacy_users_query = text(
                f"""
                SELECT fu.user_name, fu.password_hash, fu.employee_id, 'employee' AS role_name
                FROM {schema}.flask_users fu
                """
            )

        legacy_users = (await session.execute(legacy_users_query)).mappings().all()
        if not legacy_users:
            return False

        imported_users: list[User] = []
        profile_by_username: dict[str, dict[str, str | None]] = {}
        for row in legacy_users:
            username = str(row["user_name"])
            employee_id = row["employee_id"]
            employee_profile = employees_by_id.get(str(employee_id), {}) if employee_id is not None else {}
            profile_by_username[username] = employee_profile
            existing = (await session.execute(select(User).where(User.username == username))).scalar_one_or_none()
            if existing:
                imported_users.append(existing)
                continue

            user = User(
                username=username,
                password_hash=str(row["password_hash"]),
                full_name=str(employee_profile.get("full_name") or username),
                role=_map_legacy_role(str(row["role_name"])),
                department=str(employee_profile.get("department")) if employee_profile.get("department") else None,
            )
            session.add(user)
            imported_users.append(user)

        await session.flush()

        managers = [user for user in imported_users if user.role == UserRole.MANAGER]
        employees_list = [user for user in imported_users if user.role == UserRole.EMPLOYEE]
        manager_by_department: dict[str, User] = {}
        for manager in managers:
            if manager.department and manager.department not in manager_by_department:
                manager_by_department[manager.department] = manager

        default_manager = managers[0] if managers else None
        for employee in employees_list:
            if employee.manager_id:
                continue
            mapped_manager = manager_by_department.get(employee.department or "") or default_manager
            if mapped_manager and mapped_manager.id != employee.id:
                employee.manager_id = mapped_manager.id

        competency_by_name = {
            competency.name: competency
            for competency in (await session.execute(select(Competency))).scalars().all()
        }
        for employee in employees_list:
            profile = profile_by_username.get(employee.username)
            for competency_name in _split_competencies(str(profile["competencies"]) if profile else None):
                competency = competency_by_name.get(competency_name)
                if not competency:
                    competency = Competency(name=competency_name)
                    session.add(competency)
                    await session.flush()
                    competency_by_name[competency_name] = competency
                exists = (
                    await session.execute(
                        select(EmployeeCompetency).where(
                            EmployeeCompetency.employee_id == employee.id,
                            EmployeeCompetency.competency_id == competency.id,
                        )
                    )
                ).scalar_one_or_none()
                if not exists:
                    session.add(
                        EmployeeCompetency(
                            employee_id=employee.id,
                            competency_id=competency.id,
                            level=1,
                        )
                    )

        employees_by_department: dict[str, list[User]] = {}
        for employee in employees_list:
            key = (employee.department or "").strip().lower()
            employees_by_department.setdefault(key, []).append(employee)

        if await _legacy_table_exists(schema, "tasks") and managers:
            tasks_rows = (
                await session.execute(
                    text(
                        f"""
                        SELECT task_name, department, required_competency
                        FROM {schema}.tasks
                        ORDER BY id
                        LIMIT 200
                        """
                    )
                )
            ).mappings().all()

            existing_tasks = (
                await session.execute(select(Task.title, Task.department, Task.created_by_id))
            ).all()
            existing_keys = {
                (str(title).strip().lower(), str(department or "").strip().lower(), int(created_by_id))
                for title, department, created_by_id in existing_tasks
            }
            assignee_index_by_department: dict[str, int] = {}
            for row in tasks_rows:
                title = str(row["task_name"]).strip()
                department = str(row["department"]) if row["department"] else None
                department_key = (department or "").strip().lower()
                task_key = (title.lower(), department_key, managers[0].id)
                if task_key in existing_keys:
                    continue

                required_competencies: dict[str, int] = {}
                for competency_name in _split_competencies(str(row["required_competency"])):
                    competency = competency_by_name.get(competency_name)
                    if not competency:
                        competency = Competency(name=competency_name)
                        session.add(competency)
                        await session.flush()
                        competency_by_name[competency_name] = competency
                    required_competencies[str(competency.id)] = 2

                assigned_to_id = None
                department_employees = employees_by_department.get(department_key, [])
                if department_employees:
                    idx = assignee_index_by_department.get(department_key, 0) % len(department_employees)
                    assigned_to_id = department_employees[idx].id
                    assignee_index_by_department[department_key] = idx + 1

                session.add(
                    Task(
                        title=title,
                        description=f"Импортировано из {schema}.tasks",
                        priority=2,
                        department=department,
                        created_by_id=managers[0].id,
                        assigned_to_id=assigned_to_id,
                        required_competencies=required_competencies,
                    )
                )
                existing_keys.add(task_key)

        if employees_list:
            for employee in employees_list:
                has_note = (
                    await session.execute(select(Notification.id).where(Notification.user_id == employee.id).limit(1))
                ).scalar_one_or_none()
                if not has_note:
                    session.add(
                        Notification(
                            user_id=employee.id,
                            title="Профиль загружен",
                            body="Данные сотрудника импортированы из основной базы.",
                        )
                    )

        await session.commit()
        return True


async def seed_demo_data() -> None:
    async with db.SessionLocal() as session:
        manager = (await session.execute(select(User).where(User.username == "manager1"))).scalar_one_or_none()
        if manager:
            return

        manager = User(
            username="manager1",
            password_hash=hash_password("manager123"),
            full_name="Менеджер Команды",
            role=UserRole.MANAGER,
            department="analytics",
        )
        session.add(manager)
        await session.flush()

        employee = User(
            username="employee1",
            password_hash=hash_password("employee123"),
            full_name="Сотрудник Один",
            role=UserRole.EMPLOYEE,
            manager_id=manager.id,
            department="analytics",
        )
        session.add(employee)
        await session.flush()

        competencies = [
            Competency(name="SQL"),
            Competency(name="Python"),
            Competency(name="Power BI"),
        ]
        session.add_all(competencies)
        await session.flush()

        session.add_all(
            [
                EmployeeCompetency(employee_id=employee.id, competency_id=competencies[0].id, level=3),
                EmployeeCompetency(employee_id=employee.id, competency_id=competencies[1].id, level=2),
                EmployeeCompetency(employee_id=employee.id, competency_id=competencies[2].id, level=1),
            ]
        )

        session.add_all(
            [
                Task(
                    title="Собрать отчет по загрузке",
                    description="Соберите weekly отчет по отделу",
                    priority=2,
                    created_by_id=manager.id,
                    required_competencies={
                        str(competencies[0].id): 2,
                        str(competencies[2].id): 1,
                    },
                ),
                Task(
                    title="Оптимизировать SQL витрину",
                    description="Ускорить витрину employee_skills_matrix",
                    priority=1,
                    created_by_id=manager.id,
                    required_competencies={
                        str(competencies[0].id): 3,
                        str(competencies[1].id): 3,
                    },
                ),
            ]
        )

        session.add(Notification(user_id=employee.id, title="Добро пожаловать", body="Профиль сотрудника готов."))
        await session.commit()


async def ensure_default_login_users() -> None:
    async with db.SessionLocal() as session:
        manager = (await session.execute(select(User).where(User.username == "manager1"))).scalar_one_or_none()
        if not manager:
            manager = User(
                username="manager1",
                password_hash=hash_password("manager123"),
                full_name="Олег Олегович Олегов",
                role=UserRole.MANAGER,
                department="it",
            )
            session.add(manager)
            await session.flush()
        else:
            manager.full_name = "Олег Олегович Олегов"
            manager.department = "it"

        employee = (await session.execute(select(User).where(User.username == "employee1"))).scalar_one_or_none()
        if not employee:
            employee = User(
                username="employee1",
                password_hash=hash_password("employee123"),
                full_name="Сотрудник Команды",
                role=UserRole.EMPLOYEE,
                manager_id=manager.id,
                department=manager.department,
            )
            session.add(employee)
            await session.flush()

        if employee.manager_id is None and manager.id != employee.id:
            employee.manager_id = manager.id

        orphan_employees = (
            await session.execute(
                select(User).where(
                    User.role == UserRole.EMPLOYEE,
                    User.is_active.is_(True),
                    User.manager_id.is_(None),
                )
            )
        ).scalars().all()
        for orphan in orphan_employees:
            if orphan.id != manager.id:
                orphan.manager_id = manager.id
        await session.commit()


async def ensure_legacy_support_tables() -> None:
    if db.engine is None or db.engine.url.get_backend_name() != "postgresql":
        return
    async with db.SessionLocal() as session:
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS dev"))
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dev.task_status (
                    id SERIAL PRIMARY KEY,
                    task_id VARCHAR(50) UNIQUE,
                    status VARCHAR(30) DEFAULT 'new',
                    priority INTEGER DEFAULT 3,
                    assigned_employee_id VARCHAR(50),
                    assigned_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    notes TEXT,
                    updated_by VARCHAR(100),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
                """
            )
        )
        await session.execute(
            text(
                """
                CREATE OR REPLACE FUNCTION dev.enforce_task_assignment_department()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    manager_department text;
                    manager_factory text;
                    task_department text;
                    task_factory text;
                BEGIN
                    IF NEW.assigned_employee_id IS NULL OR NEW.updated_by IS NULL THEN
                        RETURN NEW;
                    END IF;

                    SELECT su.department, e.factory
                    INTO manager_department, manager_factory
                    FROM sm_users su
                    LEFT JOIN dev.flask_users fu ON fu.user_name = su.username
                    LEFT JOIN dev.employees e ON e.employee_id = fu.employee_id
                    WHERE su.username = NEW.updated_by
                      AND su.role = 'manager'
                    LIMIT 1;

                    -- Not a manager update: allow.
                    IF manager_department IS NULL THEN
                        RETURN NEW;
                    END IF;

                    SELECT t.department, t.factory
                    INTO task_department, task_factory
                    FROM dev.tasks t
                    WHERE t.task_id = NEW.task_id
                    LIMIT 1;

                    IF task_department IS NULL THEN
                        RETURN NEW;
                    END IF;

                    IF lower(trim(manager_department)) <> lower(trim(task_department)) THEN
                        RAISE EXCEPTION 'Manager % cannot assign task from another department (% <> %)',
                            NEW.updated_by, manager_department, task_department
                            USING ERRCODE = 'P0001';
                    END IF;

                    IF manager_factory IS NOT NULL
                       AND task_factory IS NOT NULL
                       AND lower(trim(manager_factory)) <> lower(trim(task_factory)) THEN
                        RAISE EXCEPTION 'Manager % cannot assign task from another factory (% <> %)',
                            NEW.updated_by, manager_factory, task_factory
                            USING ERRCODE = 'P0001';
                    END IF;
                    RETURN NEW;
                END;
                $$;
                """
            )
        )
        await session.execute(
            text("DROP TRIGGER IF EXISTS trg_enforce_task_assignment_department ON dev.task_status")
        )
        await session.execute(
            text(
                """
                CREATE TRIGGER trg_enforce_task_assignment_department
                BEFORE INSERT OR UPDATE OF assigned_employee_id, updated_by
                ON dev.task_status
                FOR EACH ROW
                EXECUTE FUNCTION dev.enforce_task_assignment_department();
                """
            )
        )
        await session.commit()


async def backfill_assignments_for_visible_tasks() -> None:
    async with db.SessionLocal() as session:
        employees = (
            await session.execute(select(User).where(User.role == UserRole.EMPLOYEE, User.is_active.is_(True)))
        ).scalars().all()
        if not employees:
            return

        for employee in employees:
            assigned_count = (
                await session.execute(select(Task).where(Task.assigned_to_id == employee.id))
            ).scalars().first()
            if assigned_count is not None:
                continue

            manager = await session.get(User, employee.manager_id) if employee.manager_id else None
            candidate_query = select(Task).where(Task.assigned_to_id.is_(None))
            if manager:
                candidate_query = candidate_query.where(Task.created_by_id == manager.id)
            if employee.department:
                candidate_query = candidate_query.where(
                    (Task.department == employee.department) | (Task.department.is_(None))
                )

            candidates = (await session.execute(candidate_query.order_by(Task.id).limit(5))).scalars().all()
            for task in candidates:
                task.assigned_to_id = employee.id

        await session.commit()
