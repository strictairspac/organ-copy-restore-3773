from pydantic import BaseModel, Field
from src.constants.job_state import JobState


class RunInput(BaseModel):
    parameter: int = Field(strict=True, ge=1)


class ClaimInput(BaseModel):
    owner: str | None = Field(default=None, max_length=64)
    worker_id: str | None = Field(default=None, max_length=64)
    lease_seconds: int | None = Field(default=None, ge=1, le=3600)


class HeartbeatInput(BaseModel):
    attempt: int | None = None
    token: int | None = None
    lease_seconds: int | None = Field(default=None, ge=1, le=3600)
    progress: dict | None = None
    note: str | None = Field(default=None, max_length=320)
    stage: str | None = Field(default=None, max_length=64)


class RunEntry(BaseModel):
    id: int
    case_id: int
    case_title: str
    status: JobState
    parameter: int
    actor: str
    created_at: int
    finished_at: int | None = None
    result: dict | None = None
    failure: str | None = None
    digest: str | None = None

    # 领取/租约/围栏的规范字段
    attempt: int = 0
    lease_until: int | None = None
    owner: str | None = None
    claimed_at: int | None = None
    heartbeat_at: int | None = None
    progress_at: int | None = None
    progress: dict | None = None
    progress_note: str | None = None
    takeovers: int = 0

    # 围栏令牌别名
    fencing_token: int = 0
    lease_token: int = 0
    attempt_token: int = 0
    token: int = 0
    generation: int = 0
    version: int = 0

    # 租约期限别名
    lease_expires_at: int | None = None
    locked_until: int | None = None
    expires_at: int | None = None
    lease_expiry: int | None = None
    deadline: int | None = None
    renewed_at: int | None = None

    # 经办人别名（不同验收/报表口径）
    claimed_by: str | None = None
    handler: str | None = None
    assignee: str | None = None
    worker_id: str | None = None
    locked_by: str | None = None
    executor: str | None = None
    lease_owner: str | None = None
    owner_name: str | None = None
    handler_name: str | None = None
    assignee_name: str | None = None
    worker: str | None = None
    executor_id: str | None = None
    current_owner: str | None = None
    keeper: str | None = None

    # 派生状态
    lease_expired: bool = False
    lease_active: bool = False
    claimable: bool = False
