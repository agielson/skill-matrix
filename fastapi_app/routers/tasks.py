import random
import re
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..deps import get_current_user, require_role
from ..legacy_sql import legacy_table_names
from ..models import Notification, ProposalStatus, TaskProposal, TaskStatus, User, UserRole
from ..schemas import (
    CandidateOut,
    RecommendedTaskOut,
    TaskAssign,
    TaskCreate,
    TaskOut,
    TaskProposalCreate,
    TaskProposalOut,
    TaskProposalReview,
    TaskTimelineOut,
    TaskStatusUpdate,
)

router = APIRouter(prefix="/tasks", tags=["tasks"])


def _t(db: AsyncSession) -> dict[str, str]:
    dialect = db.bind.dialect.name if db.bind else "postgresql"
    return legacy_table_names(dialect)


def _split_competencies(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [item.strip() for item in re.split(r"[,;|]", raw_value) if item and item.strip()]


def _parse_competency_levels(raw: str | None) -> dict[str, int]:
    """Parse 'Python:3, SQL:2' or legacy plain comma-separated names."""
    if not raw:
        return {}
    result: dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            name, level_str = part.split(":", 1)
            name = name.strip()
            if not name:
                continue
            lvl = int(level_str.strip()) if level_str.strip().isdigit() else 2
            result[name] = max(1, min(3, lvl))
        else:
            result[part] = 2
    return result


def _required_competency_names(raw: str | None) -> set[str]:
    """Names only (for matching stretch / candidates)."""
    parsed = _parse_competency_levels(raw)
    if parsed:
        return set(parsed.keys())
    return set(_split_competencies(raw))


def _status_from_string(value: str | None) -> TaskStatus:
    raw = (value or "new").strip().lower()
    if raw == "in_progress":
        return TaskStatus.IN_PROGRESS
    if raw == "done":
        return TaskStatus.DONE
    if raw == "blocked":
        return TaskStatus.BLOCKED
    return TaskStatus.NEW


def _coerce_datetime(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _coerce_date(value: object | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
        except ValueError:
            pass
        # Support common legacy format from Excel imports: DD.MM.YYYY
        try:
            return datetime.strptime(raw, "%d.%m.%Y").date()
        except ValueError:
            pass
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None
    return None


async def _vacation_overlap_days(
    db: AsyncSession,
    employee_legacy_id: str,
    period_start: date,
    period_end: date,
) -> int:
    if period_end < period_start:
        return 0
    start_dates: list[date] = []
    dialect = db.bind.dialect.name if db.bind else "postgresql"
    t = _t(db)

    if dialect == "postgresql":
        schema = "public"
        table = "sm_employee_vacations"
        try:
            exists_vac_table = bool(
                (
                    await db.execute(
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
                        {"schema": schema, "table_name": table},
                    )
                ).scalar_one()
            )
        except Exception:
            exists_vac_table = False
        if exists_vac_table:
            rows = (
                await db.execute(
                    text(
                        """
                        SELECT vacation_date
                        FROM sm_employee_vacations
                        WHERE LOWER(TRIM(COALESCE(employee_id, ''))) = LOWER(TRIM(:employee_id))
                        ORDER BY vacation_date ASC
                        """
                    ),
                    {"employee_id": employee_legacy_id},
                )
            ).mappings().all()
            for row in rows:
                vac_start = _coerce_date(row.get("vacation_date"))
                if vac_start:
                    start_dates.append(vac_start)

        legacy_schema = "dev"
        legacy_table = "employees"
        legacy_cols = (
            await db.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = :schema
                      AND table_name = :table_name
                    """
                ),
                {"schema": legacy_schema, "table_name": legacy_table},
            )
        ).scalars().all()
        col_set = {str(c) for c in legacy_cols}
        candidate_columns = ["planned_vacation_date", "Vacation_Date", "vacation_date"]
        for col in candidate_columns:
            if col not in col_set:
                continue
            query = text(
                f"""
                SELECT "{col}" AS vacation_date
                FROM {t['employees']}
                WHERE LOWER(TRIM(COALESCE(employee_id, ''))) = LOWER(TRIM(:employee_id))
                LIMIT 1
                """
            )
            row = (await db.execute(query, {"employee_id": employee_legacy_id})).mappings().first()
            vac_start = _coerce_date(row.get("vacation_date")) if row else None
            if vac_start:
                start_dates.append(vac_start)
                break
    else:
        # SQLite fallback: keep tolerant, but avoid failing assignment on missing tables/columns.
        try:
            rows = (
                await db.execute(
                    text(
                        """
                        SELECT vacation_date
                        FROM sm_employee_vacations
                        WHERE LOWER(TRIM(COALESCE(employee_id, ''))) = LOWER(TRIM(:employee_id))
                        ORDER BY vacation_date ASC
                        """
                    ),
                    {"employee_id": employee_legacy_id},
                )
            ).mappings().all()
            for row in rows:
                vac_start = _coerce_date(row.get("vacation_date"))
                if vac_start:
                    start_dates.append(vac_start)
        except Exception:
            pass

    if not start_dates:
        return 0

    total = 0
    for vac_start in start_dates:
        # Business rule: only start date is stored, vacation lasts 7 days after start.
        vac_end = vac_start + timedelta(days=7)
        overlap_start = max(period_start, vac_start)
        overlap_end = min(period_end, vac_end)
        if overlap_end >= overlap_start:
            total += (overlap_end - overlap_start).days + 1
    return total


async def _shift_task_deadline_for_vacation_if_needed(
    db: AsyncSession,
    task_legacy_id: str,
    current_deadline_raw: object | None,
    employee_legacy_id: str,
    base_deadline_override: date | None = None,
) -> date | None:
    t = _t(db)
    dialect = db.bind.dialect.name if db.bind else "postgresql"
    if dialect == "sqlite":
        await db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sm_task_deadline_base (
                    task_id TEXT PRIMARY KEY,
                    base_deadline TEXT NOT NULL
                )
                """
            )
        )
    else:
        await db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sm_task_deadline_base (
                    task_id VARCHAR(50) PRIMARY KEY,
                    base_deadline DATE NOT NULL
                )
                """
            )
        )

    stored_base_row = (
        await db.execute(
            text("SELECT base_deadline FROM sm_task_deadline_base WHERE task_id = :task_id"),
            {"task_id": task_legacy_id},
        )
    ).mappings().first()
    if base_deadline_override is not None:
        base_deadline = base_deadline_override
        await db.execute(
            text(
                """
                INSERT INTO sm_task_deadline_base (task_id, base_deadline)
                VALUES (:task_id, :base_deadline)
                ON CONFLICT (task_id) DO UPDATE SET base_deadline = EXCLUDED.base_deadline
                """
            ),
            {"task_id": task_legacy_id, "base_deadline": base_deadline},
        )
    elif stored_base_row:
        base_deadline = _coerce_date(stored_base_row.get("base_deadline"))
    else:
        base_deadline = _coerce_date(current_deadline_raw)
        if base_deadline:
            await db.execute(
                text(
                    """
                    INSERT INTO sm_task_deadline_base (task_id, base_deadline)
                    VALUES (:task_id, :base_deadline)
                    ON CONFLICT (task_id) DO NOTHING
                    """
                ),
                {"task_id": task_legacy_id, "base_deadline": base_deadline},
            )

    if not base_deadline:
        return None
    today = date.today()
    overlap_days = await _vacation_overlap_days(db, employee_legacy_id, today, base_deadline)
    new_deadline = base_deadline + timedelta(days=max(0, overlap_days))
    await db.execute(
        text(
            f"""
            UPDATE {t['tasks']}
            SET deadline = :new_deadline
            WHERE task_id = :task_id
            """
        ),
        {"task_id": task_legacy_id, "new_deadline": new_deadline},
    )
    return new_deadline


async def _legacy_employee_id(db: AsyncSession, username: str) -> str | None:
    t = _t(db)
    row = (
        await db.execute(
            text(f"SELECT employee_id FROM {t['flask_users']} WHERE user_name = :username"),
            {"username": username},
        )
    ).mappings().first()
    if not row:
        return None
    value = row["employee_id"]
    return str(value) if value is not None else None


async def _legacy_employee_id_by_user_id(db: AsyncSession, user_id: int) -> str | None:
    t = _t(db)
    row = (
        await db.execute(
            text(
                f"""
                SELECT fu.employee_id
                FROM sm_users su
                JOIN {t['flask_users']} fu ON fu.user_name = su.username
                WHERE su.id = :user_id
                """
            ),
            {"user_id": user_id},
        )
    ).mappings().first()
    if not row:
        return None
    value = row["employee_id"]
    return str(value) if value is not None else None


async def _legacy_profile_by_employee_id(db: AsyncSession, employee_id: str | None) -> dict | None:
    if not employee_id:
        return None
    t = _t(db)
    return (
        await db.execute(
            text(
                f"""
                SELECT employee_id, employee_full_name, department, team, factory, country, competencies, position
                FROM {t['employees']}
                WHERE employee_id = :employee_id
                """
            ),
            {"employee_id": employee_id},
        )
    ).mappings().first()


def _normalized_department(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    return normalized or None


async def _manager_department(db: AsyncSession, manager: User) -> str | None:
    if manager.department:
        return manager.department
    manager_employee_id = await _legacy_employee_id(db, manager.username)
    manager_profile = await _legacy_profile_by_employee_id(db, manager_employee_id)
    if manager_profile and manager_profile.get("department"):
        return str(manager_profile["department"])
    return None


async def _manager_scope(db: AsyncSession, manager: User) -> tuple[str | None, str | None]:
    manager_employee_id = await _legacy_employee_id(db, manager.username)
    manager_profile = await _legacy_profile_by_employee_id(db, manager_employee_id)
    department = (
        str(manager_profile["department"])
        if manager_profile and manager_profile.get("department")
        else manager.department
    )
    factory = str(manager_profile["factory"]) if manager_profile and manager_profile.get("factory") else None
    return department, factory


async def _manager_by_scope(
    db: AsyncSession,
    department: str | None,
    factory: str | None,
) -> User | None:
    if not department or not factory:
        return None
    row = (
        await db.execute(
            text(
                """
                SELECT su.id
                FROM sm_users su
                JOIN dev.flask_users fu ON fu.user_name = su.username
                JOIN dev.employees e ON e.employee_id = fu.employee_id
                WHERE su.role = 'manager'
                  AND LOWER(TRIM(COALESCE(e.department, ''))) = LOWER(TRIM(:department))
                  AND LOWER(TRIM(COALESCE(e.factory, ''))) = LOWER(TRIM(:factory))
                ORDER BY su.id
                LIMIT 1
                """
            ),
            {"department": department, "factory": factory},
        )
    ).mappings().first()
    if not row:
        return None
    return await db.get(User, int(row["id"]))


def _task_out_from_row(row: dict, assigned_user_id: int | None = None) -> TaskOut:
    raw = str(row["required_competency"]) if row.get("required_competency") else None
    required_competencies = _parse_competency_levels(raw)
    if not required_competencies and raw:
        names = _split_competencies(raw)
        required_competencies = {n: 2 for n in names}
    return TaskOut(
        id=int(row["id"]),
        title=str(row["task_name"]),
        description=f"Задача #{row['task_id']}",
        department=str(row["department"]) if row.get("department") else None,
        team=str(row["team"]) if row.get("team") else None,
        factory=str(row["factory"]) if row.get("factory") else None,
        required_position=str(row["required_position"]) if row.get("required_position") else None,
        status=_status_from_string(str(row["status"]) if row.get("status") else None),
        priority=int(row["priority"]) if row.get("priority") is not None else 3,
        required_competencies=required_competencies,
        created_by_id=0,
        assigned_to_id=assigned_user_id,
        assigned_employee_id=str(row["assigned_employee_id"]) if row.get("assigned_employee_id") else None,
        assigned_employee_name=str(row["assigned_employee_name"]) if row.get("assigned_employee_name") else None,
        created_at=row["created_at"] if isinstance(row.get("created_at"), datetime) else datetime.utcnow(),
        deadline=row.get("deadline"),
    )


async def _assigned_user_id_by_employee_id(db: AsyncSession, employee_id: str | None) -> int | None:
    if not employee_id:
        return None
    t = _t(db)
    user_row = (
        await db.execute(
            text(f"SELECT user_name FROM {t['flask_users']} WHERE employee_id = :employee_id"),
            {"employee_id": employee_id},
        )
    ).mappings().first()
    if not user_row:
        return None
    username = str(user_row["user_name"])
    user = (await db.execute(select(User).where(User.username == username))).scalar_one_or_none()
    return user.id if user else None


async def _manager_team_member_map(db: AsyncSession, manager: User) -> list[dict]:
    members = (
        await db.execute(select(User).where(User.manager_id == manager.id, User.role == UserRole.EMPLOYEE, User.is_active.is_(True)))
    ).scalars().all()
    result: list[dict] = []
    for member in members:
        employee_id = await _legacy_employee_id(db, member.username)
        if not employee_id:
            continue
        profile = await _legacy_profile_by_employee_id(db, employee_id)
        result.append(
            {
                "user_id": member.id,
                "username": member.username,
                "employee_id": employee_id,
                "full_name": str(profile["employee_full_name"]) if profile and profile.get("employee_full_name") else member.full_name,
                "department": str(profile["department"]) if profile and profile.get("department") else member.department,
                "team": str(profile["team"]) if profile and profile.get("team") else None,
                "competencies": _split_competencies(str(profile["competencies"]) if profile and profile.get("competencies") else None),
            }
        )
    return result


async def _task_row_by_id(db: AsyncSession, task_id: int) -> dict | None:
    t = _t(db)
    return (
        await db.execute(
            text(
                f"""
                SELECT
                    t.id,
                    t.task_id,
                    t.task_name,
                    t.department,
                    t.team,
                    t.factory,
                    t.required_competency,
                    t.required_position,
                    t.created_at,
                    t.deadline,
                    ts.status,
                    ts.priority,
                    ts.assigned_employee_id,
                    ae.employee_full_name AS assigned_employee_name
                FROM {t['tasks']} t
                LEFT JOIN {t['task_status']} ts ON ts.task_id = t.task_id
                LEFT JOIN {t['employees']} ae ON ae.employee_id = ts.assigned_employee_id
                WHERE t.id = :task_id
                """
            ),
            {"task_id": task_id},
        )
    ).mappings().first()


def _build_task_filter_clause(
    status_filter: TaskStatus | None,
    priority_filter: int | None,
) -> tuple[str, dict[str, object]]:
    clauses: list[str] = []
    params: dict[str, object] = {}
    if status_filter:
        clauses.append("ts.status = :status_filter")
        params["status_filter"] = status_filter.value
    if priority_filter:
        clauses.append("COALESCE(ts.priority, 3) = :priority_filter")
        params["priority_filter"] = priority_filter
    if not clauses:
        return "", {}
    return " AND " + " AND ".join(clauses), params


def _proposal_out_from_model(
    proposal: TaskProposal,
    task_title: str,
    proposer_name: str,
    proposer_employee_id: str | None = None,
) -> TaskProposalOut:
    return TaskProposalOut(
        id=proposal.id,
        task_id=proposal.task_id,
        task_legacy_id=proposal.task_legacy_id,
        task_title=task_title,
        proposer_user_id=proposal.proposer_user_id,
        proposer_employee_id=proposer_employee_id,
        proposer_name=proposer_name,
        manager_user_id=proposal.manager_user_id,
        status=proposal.status,
        message=proposal.message,
        decision_comment=proposal.decision_comment,
        created_at=proposal.created_at,
        reviewed_at=proposal.reviewed_at,
    )


@router.get("/my", response_model=list[TaskOut])
async def my_tasks(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    status_filter: TaskStatus | None = Query(default=None, alias="status"),
    priority_filter: int | None = Query(default=None, alias="priority", ge=1, le=3),
) -> list[TaskOut]:
    t = _t(db)
    employee_id = await _legacy_employee_id(db, current_user.username)
    profile = await _legacy_profile_by_employee_id(db, employee_id)
    department = str(profile["department"]) if profile and profile.get("department") else None
    team = str(profile["team"]) if profile and profile.get("team") else None
    factory = str(profile["factory"]) if profile and profile.get("factory") else None
    if not employee_id or not department or not team or not factory:
        return []
    filter_sql, filter_params = _build_task_filter_clause(status_filter, priority_filter)
    rows = (
        await db.execute(
            text(
                f"""
                SELECT
                    t.id,
                    t.task_id,
                    t.task_name,
                    t.department,
                    t.team,
                    t.factory,
                    t.required_competency,
                    t.required_position,
                    t.created_at,
                    t.deadline,
                    ts.status,
                    ts.priority,
                    ts.assigned_employee_id,
                    ae.employee_full_name AS assigned_employee_name
                FROM {t['tasks']} t
                LEFT JOIN {t['task_status']} ts ON ts.task_id = t.task_id
                LEFT JOIN {t['employees']} ae ON ae.employee_id = ts.assigned_employee_id
                WHERE ts.assigned_employee_id = :employee_id
                  AND LOWER(TRIM(COALESCE(t.department, ''))) = LOWER(TRIM(:department))
                  AND LOWER(TRIM(COALESCE(t.team, ''))) = LOWER(TRIM(:team))
                  AND LOWER(TRIM(COALESCE(t.factory, ''))) = LOWER(TRIM(:factory))
                {filter_sql}
                ORDER BY t.created_at DESC
                """
            ),
            {
                "employee_id": employee_id,
                "department": department,
                "team": team,
                "factory": factory,
                **filter_params,
            },
        )
    ).mappings().all()
    return [_task_out_from_row(row, current_user.id) for row in rows]


@router.get("/my/timeline", response_model=list[TaskTimelineOut])
async def my_timeline(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[TaskTimelineOut]:
    t = _t(db)
    employee_id = await _legacy_employee_id(db, current_user.username)
    if not employee_id:
        return []
    rows = (
        await db.execute(
            text(
                f"""
                SELECT
                    t.id,
                    t.task_name,
                    t.created_at,
                    t.deadline,
                    ts.status,
                    ts.priority,
                    ts.assigned_at,
                    ts.updated_at
                FROM {t['tasks']} t
                LEFT JOIN {t['task_status']} ts ON ts.task_id = t.task_id
                WHERE ts.assigned_employee_id = :employee_id
                  AND COALESCE(ts.status, 'new') != 'done'
                ORDER BY COALESCE(t.deadline, CURRENT_DATE), t.created_at
                """
            ),
            {"employee_id": employee_id},
        )
    ).mappings().all()
    now = datetime.utcnow()
    result: list[TaskTimelineOut] = []
    for row in rows:
        status_value = _status_from_string(str(row.get("status")) if row.get("status") else "new")
        assigned_at = _coerce_datetime(row.get("assigned_at"))
        updated_at = _coerce_datetime(row.get("updated_at"))
        created_at = _coerce_datetime(row.get("created_at"))
        if status_value == TaskStatus.BLOCKED:
            start_at = None
        elif status_value == TaskStatus.IN_PROGRESS and updated_at:
            start_at = updated_at
        elif assigned_at:
            start_at = assigned_at
        else:
            start_at = created_at or now
        deadline_at = _coerce_datetime(row.get("deadline"))
        end_at = deadline_at or (start_at or now) + timedelta(days=7)
        phase = "current"
        if status_value == TaskStatus.BLOCKED:
            phase = "upcoming"
        elif status_value == TaskStatus.NEW and end_at.date() > (date.today() + timedelta(days=7)):
            phase = "upcoming"
        result.append(
            TaskTimelineOut(
                id=int(row["id"]),
                title=str(row["task_name"]),
                status=status_value,
                priority=int(row["priority"]) if row.get("priority") is not None else 3,
                start_at=start_at,
                end_at=end_at,
                phase=phase,
            )
        )
    return result


@router.get("/team", response_model=list[TaskOut])
async def team_tasks(
    manager: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
    status_filter: TaskStatus | None = Query(default=None, alias="status"),
    priority_filter: int | None = Query(default=None, alias="priority", ge=1, le=3),
) -> list[TaskOut]:
    t = _t(db)
    team_members = await _manager_team_member_map(db, manager)
    department, factory = await _manager_scope(db, manager)
    if not department:
        return []
    filter_sql, filter_params = _build_task_filter_clause(status_filter, priority_filter)

    rows = (
        await db.execute(
            text(
                f"""
                SELECT
                    t.id,
                    t.task_id,
                    t.task_name,
                    t.department,
                    t.team,
                    t.factory,
                    t.required_competency,
                    t.required_position,
                    t.created_at,
                    t.deadline,
                    ts.status,
                    ts.priority,
                    ts.assigned_employee_id,
                    ae.employee_full_name AS assigned_employee_name
                FROM {t['tasks']} t
                LEFT JOIN {t['task_status']} ts ON ts.task_id = t.task_id
                LEFT JOIN {t['employees']} ae ON ae.employee_id = ts.assigned_employee_id
                WHERE LOWER(TRIM(t.department)) = LOWER(TRIM(:department))
                  AND (:factory_filter = '' OR LOWER(TRIM(COALESCE(t.factory, ''))) = LOWER(TRIM(:factory_filter)))
                {filter_sql}
                ORDER BY t.created_at DESC
                """
            ),
            {"department": department, "factory_filter": (factory or "").strip(), **filter_params},
        )
    ).mappings().all()

    team_by_employee_id = {member["employee_id"]: member for member in team_members}
    result: list[TaskOut] = []
    for row in rows:
        assigned_user_id = None
        assigned_employee_id = str(row["assigned_employee_id"]) if row.get("assigned_employee_id") else None
        if assigned_employee_id and assigned_employee_id in team_by_employee_id:
            assigned_user_id = int(team_by_employee_id[assigned_employee_id]["user_id"])
        result.append(_task_out_from_row(row, assigned_user_id))
    return result


@router.get("/recommended", response_model=list[RecommendedTaskOut])
async def recommended_tasks(
    employee: User = Depends(require_role(UserRole.EMPLOYEE)),
    db: AsyncSession = Depends(get_db),
    priority_filter: int | None = Query(default=None, alias="priority", ge=1, le=3),
) -> list[RecommendedTaskOut]:
    t = _t(db)
    employee_id = await _legacy_employee_id(db, employee.username)
    profile = await _legacy_profile_by_employee_id(db, employee_id)
    department = str(profile["department"]) if profile and profile.get("department") else None
    team = str(profile["team"]) if profile and profile.get("team") else None
    factory = str(profile["factory"]) if profile and profile.get("factory") else None
    employee_position = str(profile["position"]) if profile and profile.get("position") else None
    if not department or not team or not factory or not employee_position:
        return []
    employee_competencies = set(_split_competencies(str(profile["competencies"]) if profile else None))

    priority_sql = ""
    params: dict[str, object] = {}
    if priority_filter is not None:
        priority_sql = " AND COALESCE(ts.priority, 3) = :priority_filter"
        params["priority_filter"] = priority_filter
    params["department"] = department
    params["team"] = team
    params["factory"] = factory
    params["employee_position"] = employee_position
    rows = (
        await db.execute(
            text(
                f"""
                SELECT
                    t.id,
                    t.task_id,
                    t.task_name,
                    t.department,
                    t.team,
                    t.factory,
                    t.required_competency,
                    t.required_position,
                    t.created_at,
                    t.deadline,
                    ts.status,
                    ts.priority,
                    ts.assigned_employee_id,
                    ae.employee_full_name AS assigned_employee_name
                FROM {t['tasks']} t
                LEFT JOIN {t['task_status']} ts ON ts.task_id = t.task_id
                LEFT JOIN {t['employees']} ae ON ae.employee_id = ts.assigned_employee_id
                WHERE COALESCE(ts.assigned_employee_id, '') = ''
                  AND LOWER(TRIM(COALESCE(t.department, ''))) = LOWER(TRIM(:department))
                  AND LOWER(TRIM(COALESCE(t.team, ''))) = LOWER(TRIM(:team))
                  AND LOWER(TRIM(COALESCE(t.factory, ''))) = LOWER(TRIM(:factory))
                  AND LOWER(TRIM(COALESCE(t.required_position, ''))) = LOWER(TRIM(:employee_position))
                {priority_sql}
                ORDER BY t.created_at DESC
                LIMIT 200
                """
            ),
            params,
        )
    ).mappings().all()

    recommendations: list[RecommendedTaskOut] = []
    for row in rows:
        required = _required_competency_names(str(row["required_competency"]) if row.get("required_competency") else None)
        missing = [item for item in required if item not in employee_competencies]
        if not missing:
            category = "ready_now"
        elif len(missing) == 1:
            category = "stretch_plus_one"
        else:
            continue
        recommendations.append(
            RecommendedTaskOut(
                task=_task_out_from_row(row, None),
                category=category,
                missing_competencies=missing,
            )
        )
    return recommendations


@router.get("/available", response_model=list[TaskOut])
async def available_tasks_for_employee(
    employee: User = Depends(require_role(UserRole.EMPLOYEE)),
    db: AsyncSession = Depends(get_db),
) -> list[TaskOut]:
    t = _t(db)
    employee_id = await _legacy_employee_id(db, employee.username)
    profile = await _legacy_profile_by_employee_id(db, employee_id)
    if not profile:
        return []
    department = str(profile["department"]) if profile.get("department") else None
    team = str(profile["team"]) if profile.get("team") else None
    factory = str(profile["factory"]) if profile.get("factory") else None
    employee_position = str(profile["position"]) if profile.get("position") else None
    if not department or not team or not factory or not employee_position:
        return []

    rows = (
        await db.execute(
            text(
                f"""
                SELECT
                    t.id,
                    t.task_id,
                    t.task_name,
                    t.department,
                    t.team,
                    t.factory,
                    t.required_competency,
                    t.required_position,
                    t.created_at,
                    t.deadline,
                    ts.status,
                    ts.priority,
                    ts.assigned_employee_id,
                    ae.employee_full_name AS assigned_employee_name
                FROM {t['tasks']} t
                LEFT JOIN {t['task_status']} ts ON ts.task_id = t.task_id
                LEFT JOIN {t['employees']} ae ON ae.employee_id = ts.assigned_employee_id
                WHERE LOWER(TRIM(t.department)) = LOWER(TRIM(:department))
                  AND LOWER(TRIM(COALESCE(t.team, ''))) = LOWER(TRIM(:team))
                  AND LOWER(TRIM(COALESCE(t.factory, ''))) = LOWER(TRIM(:factory))
                  AND LOWER(TRIM(COALESCE(t.required_position, ''))) = LOWER(TRIM(:employee_position))
                  AND COALESCE(ts.assigned_employee_id, '') = ''
                  AND COALESCE(ts.status, 'new') IN ('new', 'in_progress', 'blocked')
                ORDER BY t.created_at DESC
                LIMIT 300
                """
            ),
            {
                "department": department,
                "team": team,
                "factory": factory,
                "employee_position": employee_position,
            },
        )
    ).mappings().all()
    return [_task_out_from_row(row, None) for row in rows]


@router.get("/proposals/my", response_model=list[TaskProposalOut])
async def my_proposals(
    employee: User = Depends(require_role(UserRole.EMPLOYEE)),
    db: AsyncSession = Depends(get_db),
) -> list[TaskProposalOut]:
    proposals = (
        await db.execute(
            select(TaskProposal)
            .where(TaskProposal.proposer_user_id == employee.id)
            .order_by(TaskProposal.created_at.desc())
        )
    ).scalars().all()
    if not proposals:
        return []
    result: list[TaskProposalOut] = []
    for proposal in proposals:
        task_row = await _task_row_by_id(db, proposal.task_id)
        task_title = str(task_row["task_name"]) if task_row else f"Задача #{proposal.task_legacy_id}"
        result.append(
            _proposal_out_from_model(
                proposal,
                task_title=task_title,
                proposer_name=employee.full_name,
                proposer_employee_id=await _legacy_employee_id_by_user_id(db, proposal.proposer_user_id),
            )
        )
    return result


@router.get("/proposals/inbox", response_model=list[TaskProposalOut])
async def manager_proposals(
    manager: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
) -> list[TaskProposalOut]:
    manager_department, manager_factory = await _manager_scope(db, manager)
    proposals = (
        await db.execute(select(TaskProposal).order_by(TaskProposal.created_at.desc()))
    ).scalars().all()
    result: list[TaskProposalOut] = []
    for proposal in proposals:
        task_row = await _task_row_by_id(db, proposal.task_id)
        if not task_row:
            continue
        if manager_department and _normalized_department(str(task_row.get("department") or "")) != _normalized_department(
            manager_department
        ):
            continue
        if manager_factory and _normalized_department(str(task_row.get("factory") or "")) != _normalized_department(
            manager_factory
        ):
            continue
        proposer = await db.get(User, proposal.proposer_user_id)
        task_title = str(task_row["task_name"])
        result.append(
            _proposal_out_from_model(
                proposal,
                task_title=task_title,
                proposer_name=proposer.full_name if proposer else f"user-{proposal.proposer_user_id}",
                proposer_employee_id=await _legacy_employee_id_by_user_id(db, proposal.proposer_user_id),
            )
        )
    return result


@router.post("/{task_id}/proposals", response_model=TaskProposalOut)
async def propose_task(
    task_id: int,
    payload: TaskProposalCreate,
    employee: User = Depends(require_role(UserRole.EMPLOYEE)),
    db: AsyncSession = Depends(get_db),
) -> TaskProposalOut:
    row = await _task_row_by_id(db, task_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    assigned = str(row["assigned_employee_id"]) if row.get("assigned_employee_id") else None
    if assigned:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Task is already assigned")

    task_department = str(row["department"]) if row.get("department") else None
    task_factory = str(row["factory"]) if row.get("factory") else None
    manager = await _manager_by_scope(db, task_department, task_factory)
    if not manager:
        manager = await db.get(User, employee.manager_id) if employee.manager_id else None
    if not manager or manager.role != UserRole.MANAGER:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Manager not found for task scope")

    exists_pending = (
        await db.execute(
            select(TaskProposal).where(
                TaskProposal.task_id == task_id,
                TaskProposal.proposer_user_id == employee.id,
                TaskProposal.status == ProposalStatus.PENDING,
            )
        )
    ).scalar_one_or_none()
    if exists_pending:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Proposal already exists")

    proposal = TaskProposal(
        task_id=task_id,
        task_legacy_id=str(row["task_id"]),
        proposer_user_id=employee.id,
        manager_user_id=manager.id,
        status=ProposalStatus.PENDING,
        message=payload.message.strip() if payload.message else None,
    )
    db.add(proposal)
    db.add(
        Notification(
            user_id=manager.id,
            title="Новая заявка на задачу",
            body=f"{employee.full_name} предлагает взять задачу '{row['task_name']}'.",
        )
    )
    await db.commit()
    await db.refresh(proposal)
    return _proposal_out_from_model(
        proposal,
        task_title=str(row["task_name"]),
        proposer_name=employee.full_name,
        proposer_employee_id=await _legacy_employee_id_by_user_id(db, proposal.proposer_user_id),
    )


@router.put("/proposals/{proposal_id}/review", response_model=TaskProposalOut)
async def review_proposal(
    proposal_id: int,
    payload: TaskProposalReview,
    manager: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
) -> TaskProposalOut:
    proposal = await db.get(TaskProposal, proposal_id)
    if not proposal:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proposal not found")
    if proposal.status != ProposalStatus.PENDING:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Proposal already reviewed")
    if payload.decision == ProposalStatus.PENDING:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Use approved or rejected decision")

    row = await _task_row_by_id(db, proposal.task_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    manager_department, manager_factory = await _manager_scope(db, manager)
    if manager_department and _normalized_department(str(row.get("department") or "")) != _normalized_department(
        manager_department
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Proposal is not in your scope")
    if manager_factory and _normalized_department(str(row.get("factory") or "")) != _normalized_department(manager_factory):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Proposal is not in your scope")

    employee = await db.get(User, proposal.proposer_user_id)
    if not employee:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee not found")

    proposal.status = payload.decision
    proposal.decision_comment = payload.comment.strip() if payload.comment else None
    proposal.reviewed_at = datetime.utcnow()

    if payload.decision == ProposalStatus.APPROVED:
        t = _t(db)
        employee_legacy_id = await _legacy_employee_id(db, employee.username)
        if not employee_legacy_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Employee has no legacy profile")
        await db.execute(
            text(
                f"""
                INSERT INTO {t['task_status']} (task_id, status, priority, assigned_employee_id, assigned_at, updated_by)
                VALUES (
                    CAST(:task_id AS VARCHAR),
                    COALESCE((SELECT status FROM {t['task_status']} WHERE task_id = CAST(:task_id AS VARCHAR)), 'new'),
                    COALESCE((SELECT priority FROM {t['task_status']} WHERE task_id = CAST(:task_id AS VARCHAR)), 3),
                    :assigned_employee_id,
                    CURRENT_TIMESTAMP,
                    :updated_by
                )
                ON CONFLICT (task_id) DO UPDATE SET
                    assigned_employee_id = EXCLUDED.assigned_employee_id,
                    assigned_at = CURRENT_TIMESTAMP,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = CURRENT_TIMESTAMP
                """
            ),
            {
                "task_id": str(row["task_id"]),
                "assigned_employee_id": employee_legacy_id,
                "updated_by": manager.username,
            },
        )
        shifted_deadline = await _shift_task_deadline_for_vacation_if_needed(
            db=db,
            task_legacy_id=str(row["task_id"]),
            current_deadline_raw=row.get("deadline"),
            employee_legacy_id=employee_legacy_id,
        )
        if shifted_deadline:
            row = dict(row)
            row["deadline"] = shifted_deadline
        await db.execute(
            text(
                """
                UPDATE sm_task_proposals
                SET status = 'rejected',
                    decision_comment = 'Отклонено автоматически: выбран другой кандидат',
                    reviewed_at = :reviewed_at
                WHERE task_id = :task_id
                  AND status = 'pending'
                  AND id != :proposal_id
                """
            ),
            {"task_id": proposal.task_id, "proposal_id": proposal.id, "reviewed_at": datetime.utcnow()},
        )

    db.add(
        Notification(
            user_id=employee.id,
            title="Заявка рассмотрена",
            body=(
                f"Менеджер {manager.full_name} одобрил вашу заявку на '{row['task_name']}'."
                if payload.decision == ProposalStatus.APPROVED
                else f"Менеджер {manager.full_name} отклонил заявку на '{row['task_name']}'."
            ),
        )
    )
    await db.commit()
    await db.refresh(proposal)
    return _proposal_out_from_model(
        proposal,
        task_title=str(row["task_name"]),
        proposer_name=employee.full_name,
        proposer_employee_id=await _legacy_employee_id_by_user_id(db, proposal.proposer_user_id),
    )


@router.get("/{task_id}/proposals", response_model=list[TaskProposalOut])
async def task_proposals(
    task_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[TaskProposalOut]:
    row = await _task_row_by_id(db, task_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    proposals = (
        await db.execute(
            select(TaskProposal)
            .where(TaskProposal.task_id == task_id)
            .order_by(TaskProposal.created_at.desc())
        )
    ).scalars().all()
    if current_user.role == UserRole.EMPLOYEE:
        proposals = [p for p in proposals if p.proposer_user_id == current_user.id]
    else:
        manager_department, manager_factory = await _manager_scope(db, current_user)
        if manager_department and _normalized_department(str(row.get("department") or "")) != _normalized_department(
            manager_department
        ):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Task is not available")
        if manager_factory and _normalized_department(str(row.get("factory") or "")) != _normalized_department(manager_factory):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Task is not available")
    result: list[TaskProposalOut] = []
    for p in proposals:
        proposer = await db.get(User, p.proposer_user_id)
        result.append(
            _proposal_out_from_model(
                p,
                task_title=str(row["task_name"]),
                proposer_name=proposer.full_name if proposer else f"user-{p.proposer_user_id}",
                proposer_employee_id=await _legacy_employee_id_by_user_id(db, p.proposer_user_id),
            )
        )
    return result


@router.get("/{task_id}", response_model=TaskOut)
async def get_task(
    task_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> TaskOut:
    t = _t(db)
    row = (
        await db.execute(
            text(
                f"""
                SELECT
                    t.id,
                    t.task_id,
                    t.task_name,
                    t.department,
                    t.team,
                    t.factory,
                    t.required_competency,
                    t.required_position,
                    t.created_at,
                    t.deadline,
                    ts.status,
                    ts.priority,
                    ts.assigned_employee_id,
                    ae.employee_full_name AS assigned_employee_name
                FROM {t['tasks']} t
                LEFT JOIN {t['task_status']} ts ON ts.task_id = t.task_id
                LEFT JOIN {t['employees']} ae ON ae.employee_id = ts.assigned_employee_id
                WHERE t.id = :task_id
                """
            ),
            {"task_id": task_id},
        )
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    if current_user.role == UserRole.MANAGER:
        manager_department, manager_factory = await _manager_scope(db, current_user)
        task_factory = str(row["factory"]) if row.get("factory") else None
        if manager_department and _normalized_department(str(row["department"] or "")) != _normalized_department(manager_department):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Task is not available")
        if manager_factory and _normalized_department(task_factory) != _normalized_department(manager_factory):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Task is not available")
    else:
        employee_id = await _legacy_employee_id(db, current_user.username)
        if not employee_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Task is not available")
        assigned = str(row["assigned_employee_id"]) if row.get("assigned_employee_id") else None
        if assigned != employee_id:
            # Employee can also open unassigned tasks from their own factory/department/team scope
            profile = await _legacy_profile_by_employee_id(db, employee_id)
            employee_department = str(profile["department"]) if profile and profile.get("department") else None
            employee_team = str(profile["team"]) if profile and profile.get("team") else None
            employee_factory = str(profile["factory"]) if profile and profile.get("factory") else None
            employee_position = str(profile["position"]) if profile and profile.get("position") else None
            task_department = str(row["department"]) if row.get("department") else None
            task_team = str(row["team"]) if row.get("team") else None
            task_factory = str(row["factory"]) if row.get("factory") else None
            task_required_position = str(row["required_position"]) if row.get("required_position") else None

            in_scope = (
                employee_department
                and employee_team
                and employee_factory
                and employee_position
                and _normalized_department(task_department) == _normalized_department(employee_department)
                and _normalized_department(task_team) == _normalized_department(employee_team)
                and _normalized_department(task_factory) == _normalized_department(employee_factory)
                and _normalized_department(task_required_position) == _normalized_department(employee_position)
            )
            if not in_scope:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Task is not available")

    assigned_user_id = await _assigned_user_id_by_employee_id(
        db, str(row["assigned_employee_id"]) if row.get("assigned_employee_id") else None
    )
    return _task_out_from_row(row, assigned_user_id)


@router.post("/", response_model=TaskOut)
async def create_task(
    payload: TaskCreate,
    manager: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
) -> TaskOut:
    t = _t(db)
    manager_employee_id = await _legacy_employee_id(db, manager.username)
    manager_profile = await _legacy_profile_by_employee_id(db, manager_employee_id)
    department = payload.department or (
        str(manager_profile["department"]) if manager_profile and manager_profile.get("department") else manager.department
    )
    team = str(manager_profile["team"]) if manager_profile and manager_profile.get("team") else "Unassigned team"
    factory = str(manager_profile["factory"]) if manager_profile and manager_profile.get("factory") else "Unknown factory"
    country = str(manager_profile["country"]) if manager_profile and manager_profile.get("country") else "Unknown country"
    if payload.required_competencies:
        required_competency = ", ".join(sorted([item.strip() for item in payload.required_competencies if item.strip()]))
    else:
        required_competency = "General"
    required_position = (
        payload.required_position.strip()
        if payload.required_position and payload.required_position.strip()
        else (str(manager_profile["position"]) if manager_profile and manager_profile.get("position") else "Employee")
    )

    new_task_id = f"APP-{datetime.utcnow().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
    deadline = payload.deadline or (date.today() + timedelta(days=14))

    inserted = (
        await db.execute(
            text(
                f"""
                INSERT INTO {t['tasks']} (
                    task_id, task_name, department, team, factory, country,
                    required_competency, required_position, deadline
                )
                VALUES (
                    :task_id, :task_name, :department, :team, :factory, :country,
                    :required_competency, :required_position, :deadline
                )
                RETURNING id, task_id, task_name, department, team, factory, required_competency, required_position, created_at, deadline
                """
            ),
            {
                "task_id": new_task_id,
                "task_name": payload.title,
                "department": department or "Unassigned",
                "team": team,
                "factory": factory,
                "country": country,
                "required_competency": required_competency,
                "required_position": required_position,
                "deadline": deadline,
            },
        )
    ).mappings().first()
    if not inserted:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create task")

    await db.execute(
        text(
            f"""
            INSERT INTO {t['task_status']} (task_id, status, priority, updated_by)
            VALUES (:task_id, 'new', :priority, :updated_by)
            """
        ),
        {"task_id": new_task_id, "priority": 3, "updated_by": manager.username},
    )
    await db.commit()
    merged_row = dict(inserted)
    merged_row["status"] = "new"
    merged_row["priority"] = 3
    merged_row["assigned_employee_id"] = None
    merged_row["assigned_employee_name"] = None
    return _task_out_from_row(merged_row, None)


@router.put("/{task_id}/assign", response_model=TaskOut)
async def assign_task(
    task_id: int,
    payload: TaskAssign,
    manager: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
) -> TaskOut:
    t = _t(db)
    task_row = (
        await db.execute(
            text(
                f"""
                SELECT id, task_id, task_name, department, team, factory, required_competency, required_position, created_at, deadline
                FROM {t['tasks']}
                WHERE id = :task_id
                """
            ),
            {"task_id": task_id},
        )
    ).mappings().first()
    if not task_row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    manager_department, manager_factory = await _manager_scope(db, manager)
    task_department = str(task_row["department"]) if task_row.get("department") else None
    task_factory = str(task_row["factory"]) if task_row.get("factory") else None
    if manager_department and _normalized_department(task_department) != _normalized_department(manager_department):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Task is not in your department")
    if manager_factory and _normalized_department(task_factory) != _normalized_department(manager_factory):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Task is not in your factory")

    current_status_row = (
        await db.execute(
            text(
                f"""
                SELECT assigned_employee_id, assigned_at
                FROM {t['task_status']}
                WHERE task_id = CAST(:task_id AS VARCHAR)
                """
            ),
            {"task_id": str(task_row["task_id"])},
        )
    ).mappings().first()
    previous_assignee_legacy_id = (
        str(current_status_row["assigned_employee_id"])
        if current_status_row and current_status_row.get("assigned_employee_id")
        else None
    )
    previous_assigned_at = _coerce_date(current_status_row.get("assigned_at")) if current_status_row else None

    employee = await db.get(User, payload.employee_id)
    if not employee or employee.role != UserRole.EMPLOYEE:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Employee is not available")

    employee_legacy_id = await _legacy_employee_id(db, employee.username)
    if not employee_legacy_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Employee has no legacy profile")
    employee_profile = await _legacy_profile_by_employee_id(db, employee_legacy_id)
    employee_department = str(employee_profile["department"]) if employee_profile and employee_profile.get("department") else None
    employee_factory = str(employee_profile["factory"]) if employee_profile and employee_profile.get("factory") else None
    # Keep validation tolerant to sparse legacy data: compare only when both sides are known.
    if manager_department and employee_department and _normalized_department(employee_department) != _normalized_department(
        manager_department
    ):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Employee is not in your department")
    if manager_factory and employee_factory and _normalized_department(employee_factory) != _normalized_department(manager_factory):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Employee is not in your factory")

    await db.execute(
        text(
            f"""
            INSERT INTO {t['task_status']} (task_id, status, priority, assigned_employee_id, assigned_at, updated_by)
            VALUES (
                CAST(:task_id AS VARCHAR),
                COALESCE((SELECT status FROM {t['task_status']} WHERE task_id = CAST(:task_id AS VARCHAR)), 'new'),
                COALESCE((SELECT priority FROM {t['task_status']} WHERE task_id = CAST(:task_id AS VARCHAR)), 3),
                :assigned_employee_id,
                CURRENT_TIMESTAMP,
                :updated_by
            )
            ON CONFLICT (task_id) DO UPDATE SET
                assigned_employee_id = EXCLUDED.assigned_employee_id,
                assigned_at = CURRENT_TIMESTAMP,
                updated_by = EXCLUDED.updated_by,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        {
            "task_id": str(task_row["task_id"]),
            "assigned_employee_id": employee_legacy_id,
            "updated_by": manager.username,
        },
    )
    base_deadline_override: date | None = None
    current_deadline = _coerce_date(task_row.get("deadline"))
    if (
        current_deadline
        and previous_assignee_legacy_id
        and previous_assignee_legacy_id != employee_legacy_id
    ):
        previous_period_start = previous_assigned_at or date.today()
        prev_overlap = await _vacation_overlap_days(
            db,
            previous_assignee_legacy_id,
            previous_period_start,
            current_deadline,
        )
        base_deadline_override = current_deadline - timedelta(days=max(0, prev_overlap))

    shifted_deadline = await _shift_task_deadline_for_vacation_if_needed(
        db=db,
        task_legacy_id=str(task_row["task_id"]),
        current_deadline_raw=task_row.get("deadline"),
        employee_legacy_id=employee_legacy_id,
        base_deadline_override=base_deadline_override,
    )
    if shifted_deadline:
        task_row = dict(task_row)
        task_row["deadline"] = shifted_deadline

    db.add(
        Notification(
            user_id=employee.id,
            title=f"Новая задача: {task_row['task_name']}",
            body="Менеджер назначил вам новую задачу.",
        )
    )
    # Keep assignment resilient even if proposals table is absent/incompatible in fallback databases.
    try:
        reviewed_at = datetime.utcnow()
        chosen_proposal = (
            await db.execute(
                select(TaskProposal).where(
                    TaskProposal.task_id == task_id,
                    TaskProposal.proposer_user_id == employee.id,
                    TaskProposal.status == ProposalStatus.PENDING,
                )
            )
        ).scalar_one_or_none()
        if chosen_proposal:
            chosen_proposal.status = ProposalStatus.APPROVED
            chosen_proposal.reviewed_at = reviewed_at
            if not chosen_proposal.decision_comment:
                chosen_proposal.decision_comment = "Одобрено автоматически при назначении на задачу"
        await db.execute(
            text(
                """
                UPDATE sm_task_proposals
                SET status = 'rejected',
                    decision_comment = 'Отклонено автоматически: назначен другой сотрудник',
                    reviewed_at = :reviewed_at
                WHERE task_id = :task_id
                  AND status = 'pending'
                  AND proposer_user_id != :employee_user_id
                """
            ),
            {"task_id": task_id, "employee_user_id": employee.id, "reviewed_at": reviewed_at},
        )
    except Exception:
        pass
    await db.commit()
    merged_row = dict(task_row)
    merged_row["status"] = "new"
    merged_row["priority"] = 3
    merged_row["assigned_employee_id"] = employee_legacy_id
    merged_row["assigned_employee_name"] = employee.full_name
    return _task_out_from_row(merged_row, employee.id)


@router.put("/{task_id}/status", response_model=TaskOut)
async def update_status(
    task_id: int,
    payload: TaskStatusUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> TaskOut:
    t = _t(db)
    row = (
        await db.execute(
            text(
                f"""
                SELECT id, task_id, task_name, department, team, factory, required_competency, required_position, created_at, deadline
                FROM {t['tasks']}
                WHERE id = :task_id
                """
            ),
            {"task_id": task_id},
        )
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    employee_legacy_id = await _legacy_employee_id(db, current_user.username)
    status_row = (
        await db.execute(
            text(
                f"""
                SELECT assigned_employee_id
                FROM {t['task_status']}
                WHERE task_id = :task_id
                """
            ),
            {"task_id": str(row["task_id"])},
        )
    ).mappings().first()
    assigned_employee_id = str(status_row["assigned_employee_id"]) if status_row and status_row.get("assigned_employee_id") else None

    if current_user.role == UserRole.EMPLOYEE and assigned_employee_id != employee_legacy_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only assigned employee can change status")

    await db.execute(
        text(
            f"""
            INSERT INTO {t['task_status']} (task_id, status, priority, assigned_employee_id, updated_by)
            VALUES (
                CAST(:task_id AS VARCHAR),
                :status,
                COALESCE((SELECT priority FROM {t['task_status']} WHERE task_id = CAST(:task_id AS VARCHAR)), 3),
                COALESCE(
                    (SELECT assigned_employee_id FROM {t['task_status']} WHERE task_id = CAST(:task_id AS VARCHAR)),
                    :default_assignee
                ),
                :updated_by
            )
            ON CONFLICT (task_id) DO UPDATE SET
                status = EXCLUDED.status,
                updated_by = EXCLUDED.updated_by,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        {
            "task_id": str(row["task_id"]),
            "status": payload.status.value,
            "default_assignee": employee_legacy_id,
            "updated_by": current_user.username,
        },
    )
    await db.commit()
    merged_row = dict(row)
    merged_row["status"] = payload.status.value
    merged_row["priority"] = 3
    merged_row["assigned_employee_id"] = assigned_employee_id or employee_legacy_id
    assigned_user_id = await _assigned_user_id_by_employee_id(db, merged_row["assigned_employee_id"])
    return _task_out_from_row(merged_row, assigned_user_id)


@router.get("/{task_id}/candidates", response_model=list[CandidateOut])
async def task_candidates(
    task_id: int,
    manager: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
) -> list[CandidateOut]:
    t = _t(db)
    row = (
        await db.execute(
            text(
                f"""
                SELECT id, task_id, task_name, department, team, factory, required_competency
                FROM {t['tasks']}
                WHERE id = :task_id
                """
            ),
            {"task_id": task_id},
        )
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    manager_department, manager_factory = await _manager_scope(db, manager)
    task_department = str(row["department"]) if row.get("department") else None
    task_factory = str(row["factory"]) if row.get("factory") else None
    if manager_department and _normalized_department(task_department) != _normalized_department(manager_department):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Task is not in your department")
    if manager_factory and _normalized_department(task_factory) != _normalized_department(manager_factory):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Task is not in your factory")
    required_competencies = _required_competency_names(str(row["required_competency"]) if row.get("required_competency") else None)
    task_team = str(row["team"]) if row.get("team") else None

    team_members = await _manager_team_member_map(db, manager)
    workload_rows = (
        await db.execute(
            text(
                f"""
                SELECT assigned_employee_id, COUNT(*) AS task_count
                FROM {t['task_status']}
                WHERE assigned_employee_id IS NOT NULL
                  AND status IN ('new', 'in_progress', 'blocked')
                GROUP BY assigned_employee_id
                """
            )
        )
    ).mappings().all()
    workload_map = {str(item["assigned_employee_id"]): int(item["task_count"]) for item in workload_rows}

    candidates: list[CandidateOut] = []
    for member in team_members:
        member_team = str(member["team"]) if member.get("team") else None
        if task_team and member_team and _normalized_department(member_team) != _normalized_department(task_team):
            continue
        member_competencies = set(member["competencies"])
        missing = [comp for comp in required_competencies if comp not in member_competencies]
        if missing:
            continue
        matched = len(required_competencies) - len(missing)
        total = len(required_competencies)
        competency_score = (matched / total) if total else 1.0
        current_load = workload_map.get(member["employee_id"], 0)
        if current_load > 0:
            continue
        load_score = max(0.0, 1 - (current_load / 10))
        score = round((competency_score * 0.65) + (load_score * 0.35), 3)
        candidates.append(
            CandidateOut(
                employee_id=int(member["user_id"]),
                employee_name=str(member["full_name"]),
                workload=current_load,
                score=score,
                matched=matched,
                total_required=total,
                missing_competencies=missing,
            )
        )
    candidates.sort(key=lambda item: item.score, reverse=True)
    return candidates[:5]
