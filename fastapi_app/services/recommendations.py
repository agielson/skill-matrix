from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import EmployeeCompetency, Task, TaskStatus, User, UserRole
from ..schemas import CandidateOut, RecommendedTaskOut, TaskOut


async def _competencies_by_employee(db: AsyncSession, employee_ids: list[int]) -> dict[int, dict[int, int]]:
    if not employee_ids:
        return {}
    rows = (
        await db.execute(
            select(EmployeeCompetency.employee_id, EmployeeCompetency.competency_id, EmployeeCompetency.level).where(
                EmployeeCompetency.employee_id.in_(employee_ids)
            )
        )
    ).all()
    result: dict[int, dict[int, int]] = {}
    for employee_id, competency_id, level in rows:
        result.setdefault(employee_id, {})[competency_id] = level
    return result


def _required_ids(task: Task) -> dict[int, int]:
    requirements = task.required_competencies or {}
    return {int(k): int(v) for k, v in requirements.items() if str(k).isdigit()}


async def _workload_map(db: AsyncSession, employee_ids: list[int]) -> dict[int, int]:
    if not employee_ids:
        return {}
    workload_rows = (
        await db.execute(
            select(Task.assigned_to_id, func.count(Task.id))
            .where(
                Task.assigned_to_id.in_(employee_ids),
                Task.status.in_([TaskStatus.NEW, TaskStatus.IN_PROGRESS, TaskStatus.BLOCKED]),
            )
            .group_by(Task.assigned_to_id)
        )
    ).all()
    return {row[0]: int(row[1]) for row in workload_rows if row[0] is not None}


async def build_manager_candidates(db: AsyncSession, manager: User, task: Task) -> list[CandidateOut]:
    team = (
        await db.execute(select(User).where(User.manager_id == manager.id, User.role == UserRole.EMPLOYEE, User.is_active.is_(True)))
    ).scalars().all()

    team_ids = [person.id for person in team]
    competency_map = await _competencies_by_employee(db, team_ids)
    workload = await _workload_map(db, team_ids)
    required = _required_ids(task)
    required_items = list(required.items())

    candidates: list[CandidateOut] = []
    for employee in team:
        person_competencies = competency_map.get(employee.id, {})
        matched = 0
        missing: list[str] = []
        for competency_id, level in required_items:
            person_level = person_competencies.get(competency_id, 0)
            if person_level >= level:
                matched += 1
            else:
                missing.append(str(competency_id))
        total = len(required_items)
        competency_score = matched / total if total else 1.0
        current_workload = workload.get(employee.id, 0)
        load_score = max(0.0, 1 - (current_workload / 10))
        final_score = round((competency_score * 0.65) + (load_score * 0.35), 3)
        candidates.append(
            CandidateOut(
                employee_id=employee.id,
                employee_name=employee.full_name,
                workload=current_workload,
                score=final_score,
                matched=matched,
                total_required=total,
                missing_competencies=missing,
            )
        )
    candidates.sort(key=lambda x: x.score, reverse=True)
    return candidates[:5]


async def build_employee_recommendations(db: AsyncSession, employee: User) -> list[RecommendedTaskOut]:
    tasks = (
        await db.execute(
            select(Task).where(
                Task.assigned_to_id.is_(None),
                Task.status == TaskStatus.NEW,
                Task.created_by_id != employee.id,
            )
        )
    ).scalars().all()

    competencies = (
        await db.execute(
            select(EmployeeCompetency.competency_id, EmployeeCompetency.level).where(EmployeeCompetency.employee_id == employee.id)
        )
    ).all()
    employee_levels = {competency_id: level for competency_id, level in competencies}

    recommendations: list[RecommendedTaskOut] = []
    for task in tasks:
        required = _required_ids(task)
        missing = [str(cid) for cid, level in required.items() if employee_levels.get(cid, 0) < level]
        if not missing:
            category = "ready_now"
        elif len(missing) == 1:
            category = "stretch_plus_one"
        else:
            continue
        recommendations.append(
            RecommendedTaskOut(
                task=TaskOut.model_validate(task),
                category=category,
                missing_competencies=missing,
            )
        )
    return recommendations
