import re
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..deps import get_current_user, require_role
from ..legacy_sql import legacy_table_names
from ..models import Notification, SkillRequest, SkillRequestStatus, User, UserRole
from ..schemas import CompetencyOut, ProfileOut, SkillRequestOut, UserOut, VacationCreate, VacationOut
from .tasks import _manager_team_member_map

router = APIRouter(prefix="/employees", tags=["employees"])


def _t(db: AsyncSession) -> dict[str, str]:
    dialect = db.bind.dialect.name if db.bind else "postgresql"
    return legacy_table_names(dialect)


def _split_competencies(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [item.strip() for item in re.split(r"[,;|]", raw_value) if item and item.strip()]


def _as_date(value: object | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
    return None


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


async def _legacy_employee_profile(db: AsyncSession, employee_id: str | None) -> dict | None:
    if not employee_id:
        return None
    t = _t(db)
    return (
        await db.execute(
            text(
                f"""
                SELECT employee_id, employee_full_name, department, team, competencies, position, planned_vacation_date
                FROM {t['employees']}
                WHERE employee_id = :employee_id
                """
            ),
            {"employee_id": employee_id},
        )
    ).mappings().first()


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


async def _ensure_vacations_table(db: AsyncSession) -> None:
    dialect = db.bind.dialect.name if db.bind else "postgresql"
    if dialect == "sqlite":
        await db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sm_employee_vacations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    employee_id TEXT NOT NULL,
                    vacation_date TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
    else:
        await db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sm_employee_vacations (
                    id BIGSERIAL PRIMARY KEY,
                    employee_id TEXT NOT NULL,
                    vacation_date DATE NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )


async def _ensure_skill_requests_schema(db: AsyncSession) -> None:
    dialect = db.bind.dialect.name if db.bind else "postgresql"
    try:
        if dialect == "sqlite":
            await db.execute(text("ALTER TABLE sm_skill_requests ADD COLUMN manager_course_link TEXT"))
        else:
            await db.execute(
                text("ALTER TABLE sm_skill_requests ADD COLUMN IF NOT EXISTS manager_course_link VARCHAR(500)")
            )
    except Exception:
        pass


async def _vacations_by_employee_ids(db: AsyncSession, employee_ids: list[str]) -> dict[str, list[dict]]:
    if not employee_ids:
        return {}
    await _ensure_vacations_table(db)
    dialect = db.bind.dialect.name if db.bind else "postgresql"
    if dialect == "sqlite":
        rows: list[dict] = []
        for employee_id in employee_ids:
            rows.extend(
                (
                    await db.execute(
                        text(
                            """
                            SELECT employee_id, vacation_date, reason
                            FROM sm_employee_vacations
                            WHERE employee_id = :employee_id
                            ORDER BY vacation_date ASC
                            """
                        ),
                        {"employee_id": employee_id},
                    )
                ).mappings().all()
            )
    else:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT employee_id, vacation_date, reason
                    FROM sm_employee_vacations
                    WHERE employee_id = ANY(:employee_ids)
                    ORDER BY vacation_date ASC
                    """
                ),
                {"employee_ids": employee_ids},
            )
        ).mappings().all()
    result: dict[str, list[dict]] = {}
    for row in rows:
        emp_id = str(row["employee_id"])
        result.setdefault(emp_id, []).append(
            {
                "planned_vacation_date": _as_date(row.get("vacation_date")),
                "reason": str(row.get("reason")) if row.get("reason") else None,
            }
        )
    return result


def _uploads_dir() -> Path:
    path = Path(__file__).resolve().parent.parent / "uploads"
    path.mkdir(parents=True, exist_ok=True)
    return path


async def _available_department_skills(db: AsyncSession, employee_id: str) -> list[str]:
    t = _t(db)
    profile = await _legacy_employee_profile(db, employee_id)
    if not profile or not profile.get("department"):
        return []
    dept = str(profile["department"])
    task_rows = (
        await db.execute(
            text(
                f"""
                SELECT required_competency
                FROM {t['tasks']}
                WHERE LOWER(TRIM(COALESCE(department, ''))) = LOWER(TRIM(:department))
                """
            ),
            {"department": dept},
        )
    ).mappings().all()
    names: set[str] = set()
    for row in task_rows:
        for item in _split_competencies(str(row.get("required_competency") or "")):
            names.add(item)
    return sorted(names)


async def _available_department_positions(db: AsyncSession, employee_id: str) -> list[str]:
    t = _t(db)
    profile = await _legacy_employee_profile(db, employee_id)
    if not profile or not profile.get("department"):
        return []
    dept = str(profile["department"])
    rows = (
        await db.execute(
            text(
                f"""
                SELECT DISTINCT required_position
                FROM {t['tasks']}
                WHERE LOWER(TRIM(COALESCE(department, ''))) = LOWER(TRIM(:department))
                  AND COALESCE(TRIM(required_position), '') <> ''
                ORDER BY required_position
                """
            ),
            {"department": dept},
        )
    ).mappings().all()
    return sorted({str(r["required_position"]).strip() for r in rows if r.get("required_position")})


@router.get("/me", response_model=ProfileOut)
async def get_my_profile(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ProfileOut:
    t = _t(db)
    legacy_employee_id = await _legacy_employee_id(db, current_user.username)
    profile = await _legacy_employee_profile(db, legacy_employee_id)
    competencies: list[CompetencyOut] = []
    if legacy_employee_id:
        try:
            level_rows = (
                await db.execute(
                    text(
                        f"""
                        SELECT competency, level
                        FROM {t['employees'].replace('employees', 'employee_competency_level')}
                        WHERE employee_id = :employee_id
                        ORDER BY competency
                        """
                    ),
                    {"employee_id": legacy_employee_id},
                )
            ).mappings().all()
        except Exception:
            level_rows = []
        if level_rows:
            competencies = [
                CompetencyOut(competency=str(item["competency"]), level=int(item["level"]))
                for item in level_rows
            ]
    if not competencies:
        competencies = [
            CompetencyOut(competency=name, level=1)
            for name in _split_competencies(str(profile["competencies"]) if profile else None)
        ]

    active_tasks = 0
    completed_tasks = 0
    if legacy_employee_id:
        active_tasks = int(
            (
                await db.execute(
                    text(
                        f"""
                        SELECT COUNT(*)
                        FROM {t['task_status']}
                        WHERE assigned_employee_id = :employee_id
                          AND status IN ('new', 'in_progress', 'blocked')
                        """
                    ),
                    {"employee_id": legacy_employee_id},
                )
            ).scalar_one()
        )
        completed_tasks = int(
            (
                await db.execute(
                    text(
                        f"""
                        SELECT COUNT(*)
                        FROM {t['task_status']}
                        WHERE assigned_employee_id = :employee_id
                          AND status = 'done'
                        """
                    ),
                    {"employee_id": legacy_employee_id},
                )
            ).scalar_one()
        )

    managed_team_label = None
    if current_user.role == UserRole.MANAGER:
        if current_user.username == "manager1":
            managed_team_label = "it - разработчики"
        team_count = int(
            (
                await db.execute(
                    select(func.count()).select_from(User).where(
                        User.manager_id == current_user.id,
                        User.role == UserRole.EMPLOYEE,
                        User.is_active.is_(True),
                    )
                )
            ).scalar_one()
        )
        if not managed_team_label:
            dept = str(profile["department"]) if profile and profile.get("department") else current_user.department
            team_name = str(profile["team"]) if profile and profile.get("team") else None
            if team_name and dept:
                managed_team_label = f"{dept} — {team_name}"
            elif dept:
                managed_team_label = f"Отдел {dept} — {team_count} сотрудников"
            else:
                managed_team_label = f"Команда — {team_count} сотрудников"

    vacations: list[VacationOut] = []
    if current_user.role == UserRole.MANAGER:
        members = await _manager_team_member_map(db, current_user)
        employee_ids = [str(member.get("employee_id") or "") for member in members if member.get("employee_id")]
        vacations_by_employee = await _vacations_by_employee_ids(db, employee_ids)
        for member in members:
            employee_id = str(member.get("employee_id") or "")
            employee_name = str(member.get("full_name") or "")
            items = vacations_by_employee.get(employee_id, [])
            if items:
                for item in items:
                    vacations.append(
                        VacationOut(
                            employee_id=employee_id,
                            employee_name=employee_name,
                            planned_vacation_date=item["planned_vacation_date"],
                            reason=item["reason"],
                        )
                    )
            else:
                member_profile = await _legacy_employee_profile(db, employee_id)
                vacations.append(
                    VacationOut(
                        employee_id=employee_id,
                        employee_name=employee_name,
                        planned_vacation_date=_as_date(member_profile.get("planned_vacation_date")) if member_profile else None,
                        reason=None,
                    )
                )
    elif legacy_employee_id:
        vacations_by_employee = await _vacations_by_employee_ids(db, [legacy_employee_id])
        items = vacations_by_employee.get(legacy_employee_id, [])
        if items:
            vacations.extend(
                [
                    VacationOut(
                        employee_id=legacy_employee_id,
                        employee_name=(
                            str(profile["employee_full_name"]) if profile and profile.get("employee_full_name") else current_user.full_name
                        ),
                        planned_vacation_date=item["planned_vacation_date"],
                        reason=item["reason"],
                    )
                    for item in items
                ]
            )
        else:
            vacations.append(
                VacationOut(
                    employee_id=legacy_employee_id,
                    employee_name=(
                        str(profile["employee_full_name"]) if profile and profile.get("employee_full_name") else current_user.full_name
                    ),
                    planned_vacation_date=_as_date(profile.get("planned_vacation_date")) if profile else None,
                    reason=None,
                )
            )

    return ProfileOut(
        user=UserOut(
            id=current_user.id,
            username=current_user.username,
            full_name=(
                "Олег Олегович Олегов"
                if current_user.username == "manager1"
                else (str(profile["employee_full_name"]) if profile and profile["employee_full_name"] else current_user.full_name)
            ),
            role=current_user.role,
            department=str(profile["department"]) if profile and profile["department"] else current_user.department,
            manager_id=current_user.manager_id,
            legacy_employee_id="EMP0001" if current_user.username == "manager1" else legacy_employee_id,
            team=(
                "it - разработчики"
                if current_user.username == "manager1"
                else (str(profile["team"]) if profile and profile.get("team") else None)
            ),
            position=str(profile["position"]) if profile and profile.get("position") else None,
            managed_team_label=managed_team_label,
        ),
        competencies=competencies,
        active_tasks=active_tasks,
        completed_tasks=completed_tasks,
        vacations=vacations,
    )


@router.post("/me/vacations", response_model=VacationOut)
async def add_my_vacation(
    payload: VacationCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> VacationOut:
    allowed_reasons = {"больничный", "обычный отпуск"}
    reason = payload.reason.strip().lower()
    if reason not in allowed_reasons:
        reason = "обычный отпуск"
    legacy_employee_id = await _legacy_employee_id(db, current_user.username)
    if not legacy_employee_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee profile not found")
    profile = await _legacy_employee_profile(db, legacy_employee_id)
    full_name = str(profile["employee_full_name"]) if profile and profile.get("employee_full_name") else current_user.full_name
    await _ensure_vacations_table(db)
    await db.execute(
        text(
            """
            INSERT INTO sm_employee_vacations (employee_id, vacation_date, reason)
            VALUES (:employee_id, :vacation_date, :reason)
            """
        ),
        {
            "employee_id": legacy_employee_id,
            "vacation_date": payload.planned_vacation_date.isoformat(),
            "reason": reason,
        },
    )
    await db.commit()
    return VacationOut(
        employee_id=legacy_employee_id,
        employee_name=full_name,
        planned_vacation_date=payload.planned_vacation_date,
        reason=reason,
    )


@router.get("/me/available-skills", response_model=list[str])
async def my_available_skills(
    current_user: User = Depends(require_role(UserRole.EMPLOYEE)),
    db: AsyncSession = Depends(get_db),
) -> list[str]:
    legacy_employee_id = await _legacy_employee_id(db, current_user.username)
    if not legacy_employee_id:
        return []
    return await _available_department_skills(db, legacy_employee_id)


@router.get("/me/available-positions", response_model=list[str])
async def my_available_positions(
    current_user: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
) -> list[str]:
    legacy_employee_id = await _legacy_employee_id(db, current_user.username)
    if not legacy_employee_id:
        return []
    return await _available_department_positions(db, legacy_employee_id)


@router.get("/skill-requests/my", response_model=list[SkillRequestOut])
async def my_skill_requests(
    current_user: User = Depends(require_role(UserRole.EMPLOYEE)),
    db: AsyncSession = Depends(get_db),
) -> list[SkillRequestOut]:
    await _ensure_skill_requests_schema(db)
    rows = (
        await db.execute(select(SkillRequest).where(SkillRequest.employee_user_id == current_user.id).order_by(SkillRequest.created_at.desc()))
    ).scalars().all()
    return [
        SkillRequestOut(
            id=row.id,
            employee_user_id=row.employee_user_id,
            manager_user_id=row.manager_user_id,
            skill_name=row.skill_name,
            requested_level=row.requested_level,
            reason_type=row.reason_type,
            description=row.description,
            certificate_file_name=row.certificate_file_name,
            status=row.status,
            review_comment=row.review_comment,
            created_at=row.created_at,
            reviewed_at=row.reviewed_at,
        )
        for row in rows
    ]


@router.post("/skill-requests", response_model=SkillRequestOut)
async def create_skill_request(
    skill_name: str = Form(...),
    requested_level: int = Form(...),
    reason_type: str = Form(...),
    description: str | None = Form(default=None),
    certificate: UploadFile | None = File(default=None),
    current_user: User = Depends(require_role(UserRole.EMPLOYEE)),
    db: AsyncSession = Depends(get_db),
) -> SkillRequestOut:
    await _ensure_skill_requests_schema(db)
    if requested_level < 1 or requested_level > 3:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="requested_level must be 1..3")
    if not skill_name.strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="skill_name is required")
    reason_norm = reason_type.strip().lower()
    if reason_norm not in {"training", "add"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="reason_type must be training/add")
    if reason_norm == "training" and not (description or "").strip():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="comment is required for training request")
    if reason_norm == "add" and (certificate is None or not certificate.filename):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="certificate is required for add request")

    manager_id = current_user.manager_id
    if not manager_id:
        manager_row = (
            await db.execute(select(User.id).where(User.role == UserRole.MANAGER, User.department == current_user.department).limit(1))
        ).first()
        manager_id = int(manager_row[0]) if manager_row else None
    if not manager_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Manager not found")

    file_path: str | None = None
    file_name: str | None = None
    if certificate and certificate.filename:
        ext = Path(certificate.filename).suffix.lower()
        if ext not in {".pdf", ".png", ".jpg", ".jpeg"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only pdf/png/jpg are allowed")
        safe_name = f"{uuid4().hex}{ext}"
        target = _uploads_dir() / safe_name
        data = await certificate.read()
        target.write_bytes(data)
        file_path = str(target)
        file_name = certificate.filename

    record = SkillRequest(
        employee_user_id=current_user.id,
        manager_user_id=manager_id,
        skill_name=skill_name.strip(),
        requested_level=requested_level,
        reason_type=reason_norm,
        description=description.strip() if description else None,
        certificate_file_path=file_path,
        certificate_file_name=file_name,
        status=SkillRequestStatus.PENDING,
    )
    db.add(record)
    db.add(
        Notification(
            user_id=manager_id,
            title="Новая заявка на навык",
            body=f"{current_user.full_name} запросил навык '{record.skill_name}' (уровень {record.requested_level}).",
        )
    )
    await db.commit()
    await db.refresh(record)
    return SkillRequestOut(
        id=record.id,
        employee_user_id=record.employee_user_id,
        manager_user_id=record.manager_user_id,
        skill_name=record.skill_name,
        requested_level=record.requested_level,
        reason_type=record.reason_type,
        description=record.description,
        certificate_file_name=record.certificate_file_name,
        status=record.status,
        review_comment=record.review_comment,
        manager_course_link=record.manager_course_link,
        created_at=record.created_at,
        reviewed_at=record.reviewed_at,
    )


@router.get("/skill-requests/team", response_model=list[SkillRequestOut])
async def team_skill_requests(
    manager: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
) -> list[SkillRequestOut]:
    await _ensure_skill_requests_schema(db)
    rows = (
        await db.execute(
            select(SkillRequest).where(SkillRequest.manager_user_id == manager.id).order_by(SkillRequest.created_at.desc())
        )
    ).scalars().all()
    return [
        SkillRequestOut(
            id=row.id,
            employee_user_id=row.employee_user_id,
            manager_user_id=row.manager_user_id,
            skill_name=row.skill_name,
            requested_level=row.requested_level,
            reason_type=row.reason_type,
            description=row.description,
            certificate_file_name=row.certificate_file_name,
            status=row.status,
            review_comment=row.review_comment,
            manager_course_link=row.manager_course_link,
            created_at=row.created_at,
            reviewed_at=row.reviewed_at,
        )
        for row in rows
    ]


@router.put("/skill-requests/{request_id}/review", response_model=SkillRequestOut)
async def review_skill_request(
    request_id: int,
    decision: str = Form(...),
    comment: str | None = Form(default=None),
    manager: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
) -> SkillRequestOut:
    await _ensure_skill_requests_schema(db)
    row = await db.get(SkillRequest, request_id)
    if not row or row.manager_user_id != manager.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Request not found")
    decision_norm = decision.strip().lower()
    if decision_norm not in {"approved", "rejected"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Decision must be approved/rejected")
    row.status = SkillRequestStatus.APPROVED if decision_norm == "approved" else SkillRequestStatus.REJECTED
    row.review_comment = comment.strip() if comment else None
    row.manager_course_link = comment.strip() if (comment and row.reason_type == "training" and decision_norm == "approved") else None
    row.reviewed_at = datetime.utcnow()
    if decision_norm == "approved" and row.reason_type == "add":
        legacy_employee_id = await _legacy_employee_id_by_user_id(db, row.employee_user_id)
        if legacy_employee_id:
            t = _t(db)
            try:
                await db.execute(
                    text(
                        f"""
                        INSERT INTO {t['employees'].replace('employees', 'employee_competency_level')} (employee_id, competency, level)
                        VALUES (:employee_id, :competency, :level)
                        ON CONFLICT (employee_id, competency) DO UPDATE SET level = EXCLUDED.level
                        """
                    ),
                    {"employee_id": legacy_employee_id, "competency": row.skill_name, "level": row.requested_level},
                )
            except Exception:
                pass
    db.add(
        Notification(
            user_id=row.employee_user_id,
            title="Заявка на навык рассмотрена",
            body=(
                f"Менеджер {manager.full_name} одобрил заявку '{row.skill_name}'. "
                f"{('Ссылка на курс: ' + row.manager_course_link) if row.manager_course_link else ''}".strip()
                if decision_norm == "approved" and row.reason_type == "training"
                else (
                    f"Менеджер {manager.full_name} одобрил добавление компетенции '{row.skill_name}' в профиль."
                    if decision_norm == "approved" and row.reason_type == "add"
                    else f"Менеджер {manager.full_name} отклонил заявку '{row.skill_name}'."
                )
            ),
        )
    )
    await db.commit()
    await db.refresh(row)
    return SkillRequestOut(
        id=row.id,
        employee_user_id=row.employee_user_id,
        manager_user_id=row.manager_user_id,
        skill_name=row.skill_name,
        requested_level=row.requested_level,
        reason_type=row.reason_type,
        description=row.description,
        certificate_file_name=row.certificate_file_name,
        status=row.status,
        review_comment=row.review_comment,
        manager_course_link=row.manager_course_link,
        created_at=row.created_at,
        reviewed_at=row.reviewed_at,
    )


@router.get("/team-competencies", response_model=list[str])
async def team_competency_names(
    manager: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
) -> list[str]:
    members = await _manager_team_member_map(db, manager)
    names: set[str] = set()
    for m in members:
        for c in m["competencies"]:
            names.add(c)
    return sorted(names)


@router.get("/team", response_model=list[UserOut])
async def get_team(
    manager: User = Depends(require_role(UserRole.MANAGER)),
    db: AsyncSession = Depends(get_db),
) -> list[UserOut]:
    team = (
        await db.execute(select(User).where(User.manager_id == manager.id, User.role == UserRole.EMPLOYEE, User.is_active.is_(True)))
    ).scalars().all()

    result: list[UserOut] = []
    for member in team:
        legacy_employee_id = await _legacy_employee_id(db, member.username)
        profile = await _legacy_employee_profile(db, legacy_employee_id)
        result.append(
            UserOut(
                id=member.id,
                username=member.username,
                full_name=str(profile["employee_full_name"]) if profile and profile["employee_full_name"] else member.full_name,
                role=member.role,
                department=str(profile["department"]) if profile and profile["department"] else member.department,
                manager_id=member.manager_id,
                position=str(profile["position"]) if profile and profile.get("position") else None,
                managed_team_label=None,
            )
        )
    return result
