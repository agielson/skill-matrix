from __future__ import annotations

from datetime import datetime
from enum import Enum

from sqlalchemy import JSON, Boolean, DateTime, Enum as SqlEnum, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


def _enum_values(enum_cls: type[Enum]) -> list[str]:
    return [m.value for m in enum_cls]


class UserRole(str, Enum):
    MANAGER = "manager"
    EMPLOYEE = "employee"


class TaskStatus(str, Enum):
    NEW = "new"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    BLOCKED = "blocked"


class ProposalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class SkillRequestStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class User(Base):
    __tablename__ = "sm_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    full_name: Mapped[str] = mapped_column(String(200))
    department: Mapped[str | None] = mapped_column(String(100), nullable=True)
    role: Mapped[UserRole] = mapped_column(
        SqlEnum(UserRole, values_callable=_enum_values),
        index=True,
    )
    manager_id: Mapped[int | None] = mapped_column(ForeignKey("sm_users.id"), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    manager: Mapped[User | None] = relationship("User", remote_side=[id], back_populates="team_members")
    team_members: Mapped[list[User]] = relationship("User", back_populates="manager")


class Competency(Base):
    __tablename__ = "sm_competencies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True)


class EmployeeCompetency(Base):
    __tablename__ = "sm_employee_competencies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    employee_id: Mapped[int] = mapped_column(ForeignKey("sm_users.id"), index=True)
    competency_id: Mapped[int] = mapped_column(ForeignKey("sm_competencies.id"))
    level: Mapped[int] = mapped_column(Integer, default=1)


class Task(Base):
    __tablename__ = "sm_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(200))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    department: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[TaskStatus] = mapped_column(
        SqlEnum(TaskStatus, values_callable=_enum_values),
        default=TaskStatus.NEW,
        index=True,
    )
    priority: Mapped[int] = mapped_column(Integer, default=3)
    required_competencies: Mapped[dict[str, int]] = mapped_column(JSON, default=dict)
    created_by_id: Mapped[int] = mapped_column(ForeignKey("sm_users.id"))
    assigned_to_id: Mapped[int | None] = mapped_column(ForeignKey("sm_users.id"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Notification(Base):
    __tablename__ = "sm_notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("sm_users.id"), index=True)
    title: Mapped[str] = mapped_column(String(200))
    body: Mapped[str] = mapped_column(Text)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class TaskProposal(Base):
    __tablename__ = "sm_task_proposals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[int] = mapped_column(Integer, index=True)
    task_legacy_id: Mapped[str] = mapped_column(String(50), index=True)
    proposer_user_id: Mapped[int] = mapped_column(ForeignKey("sm_users.id"), index=True)
    manager_user_id: Mapped[int] = mapped_column(ForeignKey("sm_users.id"), index=True)
    status: Mapped[ProposalStatus] = mapped_column(
        SqlEnum(ProposalStatus, values_callable=_enum_values),
        default=ProposalStatus.PENDING,
        index=True,
    )
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    decision_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class SkillRequest(Base):
    __tablename__ = "sm_skill_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    employee_user_id: Mapped[int] = mapped_column(ForeignKey("sm_users.id"), index=True)
    manager_user_id: Mapped[int] = mapped_column(ForeignKey("sm_users.id"), index=True)
    skill_name: Mapped[str] = mapped_column(String(120), index=True)
    requested_level: Mapped[int] = mapped_column(Integer, default=1)
    reason_type: Mapped[str] = mapped_column(String(40), default="обучение")
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    certificate_file_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    certificate_file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[SkillRequestStatus] = mapped_column(
        SqlEnum(SkillRequestStatus, values_callable=_enum_values),
        default=SkillRequestStatus.PENDING,
        index=True,
    )
    review_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    manager_course_link: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
