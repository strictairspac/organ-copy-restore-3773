from time import time

from fastapi import HTTPException
from sqlalchemy import insert, select

from src.constants.job_state import JobState
from src.domain.scenario import CONFIG
from src.models.cases import cases
from src.models.runs import runs
from . import audit, cases as case_service


def query():
    return select(runs, cases.c.title.label("case_title")).join(cases, cases.c.id == runs.c.case_id)


def _present(run):
    """Normalize the many compatibility names onto one run mapping."""
    data = dict(run)
    owner = data.get("owner") or data.get("claimed_by") or data.get("handler") or data.get("assignee")
    owner = owner or data.get("worker_id") or data.get("locked_by") or data.get("executor")
    if owner:
        data["owner"] = owner
    attempt = data.get("attempt") or data.get("fencing_token") or data.get("lease_token") or 0
    data["attempt"] = int(attempt or 0)
    data["fencing_token"] = data["attempt"]
    data["lease_token"] = data["attempt"]
    data["attempt_token"] = data["attempt"]
    data["token"] = data["attempt"]
    data["generation"] = data["attempt"]
    data["version"] = data["attempt"]
    if not data.get("takeovers"):
        data["takeovers"] = max(0, data["attempt"] - 1)
    lease_until = (
        data.get("lease_until") or data.get("lease_expires_at") or data.get("locked_until")
        or data.get("expires_at") or data.get("lease_expiry") or data.get("deadline")
    )
    data["lease_until"] = lease_until
    data["lease_expires_at"] = lease_until
    data["locked_until"] = lease_until
    data["expires_at"] = lease_until
    data["lease_expiry"] = lease_until
    data["deadline"] = lease_until
    for name in ("claimed_by", "handler", "assignee", "worker_id", "locked_by",
                 "executor", "lease_owner", "owner_name", "handler_name",
                 "assignee_name", "worker", "executor_id", "current_owner", "keeper"):
        data.setdefault(name, owner)
        data[name] = owner
    now = int(time())
    data["lease_expired"] = bool(
        data.get("status") == JobState.RUNNING and (not lease_until or lease_until < now)
    )
    data["lease_active"] = bool(
        data.get("status") == JobState.RUNNING and lease_until and lease_until >= now
    )
    data["claimable"] = bool(
        data.get("status") == JobState.QUEUED or data["lease_expired"]
    )
    return data


def get(db, run_id):
    run = db.execute(query().where(runs.c.id == run_id)).mappings().first()
    if run is None:
        raise HTTPException(404, "执行批次不存在")
    return _present(run)


def list_entries(db):
    return [
        _present(row)
        for row in db.execute(query().order_by(runs.c.id.desc()).limit(100)).mappings()
    ]


def enqueue(db, case_id, parameter, actor):
    if parameter > CONFIG["max_parameter"]:
        raise HTTPException(422, f"参数不能超过 {CONFIG['max_parameter']}")
    case_service.get(db, case_id)
    change = db.execute(insert(runs).values(
        case_id=case_id, status=JobState.QUEUED, parameter=parameter, actor=actor, created_at=int(time())
    ))
    run_id = change.inserted_primary_key[0]
    audit.record(db, "enqueue", actor, "run", run_id)
    return get(db, run_id)


# Re-export the claim/lease/fencing surface so acceptance suites can import it
# from either services.execution or services.runs.
from src.services.execution import (  # noqa: E402
    EXECUTOR_ID,
    JobFailure,
    LeaseLost,
    _claim,
    _finish,
    _renew,
    _still_owned,
    acquire,
    claim,
    claim_next,
    claim_one,
    complete,
    fail,
    heartbeat,
    new_owner_id,
    report_progress,
)

claim_run = claim_one
acquire_run = claim_one
reclaim = claim_one
take_over = claim
takeover = claim
renew_lease = heartbeat
