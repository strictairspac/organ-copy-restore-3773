from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import select

from src.config.resources import engine
from src.constants.job_state import JobState
from src.controllers.access import identity, reviewer
from src.controllers.workbench import new_review
from src.models.runs import runs as runs_table
from src.schemas.reviews import ReviewEntry, ReviewInput
from src.schemas.runs import ClaimInput, HeartbeatInput, RunEntry
from src.services import execution, reviews, runs
from src.utils.serial import canonical_json

router = APIRouter(prefix="/runs", tags=["Execution"])


@router.get("", response_model=list[RunEntry], dependencies=[Depends(identity)])
def list_runs():
    with engine.connect() as db:
        return runs.list_entries(db)


@router.get("/{run_id}", response_model=RunEntry, dependencies=[Depends(identity)])
def run_detail(run_id: int):
    with engine.connect() as db:
        return runs.get(db, run_id)


@router.post("/{run_id}/claim", response_model=RunEntry, dependencies=[Depends(identity)])
@router.post("/{run_id}/acquire", response_model=RunEntry, dependencies=[Depends(identity)])
@router.post("/{run_id}/takeover", response_model=RunEntry, dependencies=[Depends(identity)])
def claim_run(run_id: int, form: ClaimInput | None = None, user=Depends(identity)):
    owner = None
    lease_seconds = None
    if form is not None:
        owner = form.owner or form.worker_id
        lease_seconds = form.lease_seconds
    owner = owner or user["username"]
    with engine.begin() as db:
        claimed = execution.claim_one(db, run_id, owner=owner, lease_seconds=lease_seconds)
        if claimed is None:
            current = runs.get(db, run_id)
            if current["status"] in (JobState.DONE, JobState.FAILED):
                raise HTTPException(409, "批次已结束，不能再领取")
            raise HTTPException(409, "批次已有经办人且经办期限未到，到期后才能接手")
    with engine.connect() as db:
        return runs.get(db, run_id)


@router.post("/{run_id}/heartbeat", response_model=RunEntry, dependencies=[Depends(identity)])
@router.post("/{run_id}/renew", response_model=RunEntry, dependencies=[Depends(identity)])
@router.post("/{run_id}/progress", response_model=RunEntry, dependencies=[Depends(identity)])
def heartbeat_run(run_id: int, form: HeartbeatInput | None = None):
    attempt = None
    token = None
    lease_seconds = None
    progress = None
    note = None
    if form is not None:
        attempt = form.attempt
        token = form.token
        lease_seconds = form.lease_seconds
        progress = form.progress
        note = form.note or form.stage
    with engine.begin() as db:
        if attempt is None and token is not None:
            attempt = token
        if attempt is None:
            # No fencing token supplied: extend the current attempt directly.
            attempt = db.scalar(
                select(runs_table.c.attempt).where(runs_table.c.id == run_id)
            )
        renewed = execution._renew(
            db, run_id, attempt=attempt, lease_seconds=lease_seconds,
            progress=progress, note=note,
        )
        if not renewed:
            raise HTTPException(409, "经办令牌已失效，批次可能已由别人接手")
    with engine.connect() as db:
        return runs.get(db, run_id)


@router.get("/{run_id}/review", response_model=ReviewEntry | None, dependencies=[Depends(identity)])
def review_detail(run_id: int):
    with engine.connect() as db:
        runs.get(db, run_id)
        return reviews.get(db, run_id)


@router.post("/{run_id}/review", response_model=ReviewEntry, status_code=201)
def sign(run_id: int, form: ReviewInput, user=Depends(reviewer)):
    return new_review(run_id, form, user)


@router.get("/{run_id}/export", dependencies=[Depends(identity)])
def export(run_id: int):
    with engine.connect() as db:
        run = runs.get(db, run_id)
        if run["status"] != JobState.DONE:
            raise HTTPException(409, "没有可导出的执行结果")
        review = reviews.get(db, run_id)
    # Keep the restoration proof stable and free of internal lease bookkeeping.
    evidence = {key: run.get(key) for key in (
        "id", "case_id", "case_title", "status", "parameter", "actor",
        "created_at", "finished_at", "result", "failure", "digest",
    )}
    evidence["review"] = review
    return Response(canonical_json(evidence), media_type="application/json", headers={
        "Content-Disposition": f"attachment; filename=evidence-{run_id}.json",
    })
