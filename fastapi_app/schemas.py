from datetime import date, datetime

from pydantic import BaseModel, Field

from .models import ProposalStatus, SkillRequestStatus, TaskStatus, UserRole


class LoginRequest(BaseModel):
    username: str
    password: str


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(min_length=6, max_length=128)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserOut(BaseModel):
    id: int
    username: str
    full_name: str
    role: UserRole
    department: str | None = None
    manager_id: int | None = None
    legacy_employee_id: str | None = None
    team: str | None = None
    position: str | None = None
    managed_team_label: str | None = Field(
        default=None,
        description="Для менеджера: отдел / команда, которой руководит.",
    )

    model_config = {"from_attributes": True}


class CompetencyOut(BaseModel):
    competency: str
    level: int


class VacationOut(BaseModel):
    employee_id: str
    employee_name: str
    planned_vacation_date: date | None = None
    reason: str | None = None


class VacationCreate(BaseModel):
    planned_vacation_date: date
    reason: str


class ProfileOut(BaseModel):
    user: UserOut
    competencies: list[CompetencyOut]
    active_tasks: int
    completed_tasks: int
    vacations: list[VacationOut] = Field(default_factory=list)


class TaskCreate(BaseModel):
    title: str = Field(min_length=3, max_length=200)
    required_position: str | None = None
    deadline: date | None = None
    department: str | None = None
    required_competencies: list[str] = Field(default_factory=list)


class TaskAssign(BaseModel):
    employee_id: int


class TaskStatusUpdate(BaseModel):
    status: TaskStatus


class TaskOut(BaseModel):
    id: int
    title: str
    description: str | None
    department: str | None
    team: str | None = None
    factory: str | None = None
    required_position: str | None = None
    status: TaskStatus
    priority: int
    required_competencies: dict[str, int]
    created_by_id: int
    assigned_to_id: int | None
    assigned_employee_id: str | None = None
    assigned_employee_name: str | None = None
    created_at: datetime
    deadline: date | datetime | None = None

    model_config = {"from_attributes": True}


class CandidateOut(BaseModel):
    employee_id: int
    employee_name: str
    workload: int
    score: float
    matched: int
    total_required: int
    missing_competencies: list[str]


class RecommendedTaskOut(BaseModel):
    task: TaskOut
    category: str
    missing_competencies: list[str]


class NotificationOut(BaseModel):
    id: int
    title: str
    body: str
    is_read: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class TaskProposalCreate(BaseModel):
    message: str | None = Field(default=None, max_length=1000)


class TaskProposalReview(BaseModel):
    decision: ProposalStatus
    comment: str | None = Field(default=None, max_length=1000)


class TaskProposalOut(BaseModel):
    id: int
    task_id: int
    task_legacy_id: str
    task_title: str
    proposer_user_id: int
    proposer_employee_id: str | None = None
    proposer_name: str
    manager_user_id: int
    status: ProposalStatus
    message: str | None
    decision_comment: str | None
    created_at: datetime
    reviewed_at: datetime | None


class TaskTimelineOut(BaseModel):
    id: int
    title: str
    status: TaskStatus
    priority: int
    start_at: datetime | None
    end_at: datetime
    phase: str


class SkillRequestOut(BaseModel):
    id: int
    employee_user_id: int
    manager_user_id: int
    skill_name: str
    requested_level: int
    reason_type: str
    description: str | None = None
    certificate_file_name: str | None = None
    status: SkillRequestStatus
    review_comment: str | None = None
    manager_course_link: str | None = None
    created_at: datetime
    reviewed_at: datetime | None = None
