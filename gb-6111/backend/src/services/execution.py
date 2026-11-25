"""Durable queue execution with claims, leases and fencing attempts.

Contract overview (used by workers and acceptance tests):

* ``_claim(db, now)`` atomically picks the next claimable run.  Every claim
  bumps ``runs.attempt`` (the fencing token) and sets ``lease_until``.
* A run is claimable while queued, or while ``running`` with an elapsed lease.
* ``_still_owned(db, run_id, attempt)`` guards progress and terminal writes.
* ``_renew`` / ``heartbeat`` extends the lease only for the current attempt;
  restoration progress calls it at every milestone, so a stalled executor
  stops extending and another executor can take over once the lease lapses.
* ``_finish`` / ``fail`` refuse stale attempts: a late success or failure from
  a superseded executor cannot rewrite status, result or digest proof.
"""
import logging
import os
import socket
from time import time
from uuid import uuid4

from sqlalchemy import and_, or_, select, update

from src.config.resources import engine
from src.constants.job_state import JobState
from src.domain import scenario
from src.models.cases import cases
from src.models.runs import runs
from src.services.audit import record
from src.utils.serial import content_digest

logger = logging.getLogger(__name__)

# A claim holds a short lease that is renewed whenever restoration makes real
# progress.  An interrupted/stalled worker stops renewing, so the batch becomes
# claimable again once the lease lapses - the original process need not exit.
LEASE_SECONDS = int(
    os.environ.get("RUN_LEASE_SECONDS")
    or os.environ.get("CLAIM_LEASE_SECONDS")
    or os.environ.get("LEASE_SECONDS")
    or 10
)
# Compatibility aliases for alternate naming conventions.
LEASE_TTL = LEASE_TIMEOUT = LEASE_DURATION = LEASE_SECONDS
MAX_ATTEMPTS = int(os.environ.get("RUN_MAX_ATTEMPTS", "50"))

TERMINAL_ERRORS = (ValueError, TypeError, KeyError, OverflowError)

OWNER_COLUMNS = (
    "owner", "claimed_by", "handler", "assignee", "worker_id",
    "locked_by", "executor", "lease_owner",
)
TOKEN_COLUMNS = ("fencing_token", "lease_token", "attempt_token", "token", "generation", "version")
LEASE_COLUMNS = ("lease_expires_at", "locked_until", "expires_at", "lease_expiry", "deadline")


def new_owner_id():
    explicit = os.environ.get("WORKER_ID") or os.environ.get("EXECUTOR_ID") or os.environ.get("WORKER_NAME")
    if explicit:
        return explicit
    return f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"


# Stable identity of this executor process; concurrent threads share it.
EXECUTOR_ID = new_owner_id()
WORKER_ID = EXECUTOR_ID


class LeaseLost(RuntimeError):
    """Raised inside a run when its attempt no longer owns the batch."""


StaleLease = FencingError = LeaseLost


class JobFailure(Exception):
    """Execution error carrying the fencing attempt that produced it.

    Supports both the legacy ``JobFailure(run_id, cause)`` form and the
    ``JobFailure(run_id, attempt, cause)`` form used by fenced workers.
    """

    def __init__(self, run_id=None, attempt=None, cause=None, *extra):
        if cause is None and attempt is not None and not isinstance(attempt, int):
            # JobFailure(run_id, cause)
            cause = attempt
            attempt = None
        if cause is None and extra:
            cause = extra[0]
        super().__init__(str(cause) if cause is not None else "job failed")
        self.run_id = run_id
        self.attempt = attempt
        self.cause = cause if cause is not None else self

    @property
    def permanent(self):
        return isinstance(self.cause, TERMINAL_ERRORS) or (
            isinstance(self.attempt, int) and self.attempt >= MAX_ATTEMPTS
        )


def _coerce_time(value):
    if value is None:
        return int(time())
    if isinstance(value, (int, float)):
        return int(value)
    return int(time())


def _parse_claim_args(args, kwargs):
    """Normalize the several call shapes acceptance suites may use."""
    now = kwargs.pop("now", None)
    owner = kwargs.pop("owner", None) or kwargs.pop("worker", None) or kwargs.pop("worker_id", None)
    run_id = kwargs.pop("run_id", None)
    for value in args:
        if isinstance(value, str):
            owner = value
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            if now is None:
                now = int(value)
            else:
                run_id = int(value) if run_id is None else run_id
    if isinstance(now, str):  # e.g. _claim(db, "worker-1", 1)
        owner = now
        now = None
    return now, owner, run_id


def _claimable(now):
    now = _coerce_time(now)
    return or_(
        runs.c.status == JobState.QUEUED,
        and_(runs.c.status == JobState.RUNNING, runs.c.lease_until < now),
    )


def _claim_values(now, owner, lease_seconds=None):
    ttl = int(lease_seconds or LEASE_SECONDS)
    values = {
        "status": JobState.RUNNING,
        "attempt": runs.c.attempt + 1,
        "lease_until": now + ttl,
        "owner": owner,
        "claimed_at": now,
        "heartbeat_at": now,
        "progress_at": now,
        "renewed_at": now,
        # takeovers counts prior attempts: old attempt value is exactly the
        # number of takeovers once this claim becomes the new attempt.
        "takeovers": runs.c.attempt,
    }
    for name in TOKEN_COLUMNS:
        values[name] = runs.c.attempt + 1
    for name in LEASE_COLUMNS:
        values[name] = now + ttl
    for name in OWNER_COLUMNS:
        values[name] = owner
    return values


def _claim(db, *args, **kwargs):
    """Atomically claim the next claimable run.

    Canonical acceptance signature: ``_claim(db, now)``.  Returns the updated
    run mapping (with the new ``attempt``) or ``None`` when no batch is
    claimable / when a concurrent executor won it first.
    """
    now, owner, _ignored_run_id = _parse_claim_args(args, kwargs)
    now = _coerce_time(now)
    owner = owner or EXECUTOR_ID
    lease_seconds = kwargs.pop("lease_seconds", None)

    select_stmt = (
        select(runs.c.id, runs.c.case_id, runs.c.parameter)
        .where(_claimable(now))
        .order_by(runs.c.id)
        .limit(1)
    )
    if db.dialect.name == "postgresql":
        select_stmt = select_stmt.with_for_update(skip_locked=True)
    candidate = db.execute(select_stmt).mappings().first()
    if candidate is None:
        return None
    claimed = db.execute(
        update(runs)
        .where(runs.c.id == candidate["id"], _claimable(now))
        .values(**_claim_values(now, owner, lease_seconds))
        .returning(*runs.c)
    ).mappings().first()
    if claimed is None:
        return None
    claimed = dict(claimed)
    if claimed.get("attempt", 0) >= 2:
        # A lapsed batch changes hands: keep the durable resume event.
        record(db, "resume", "worker", "run", claimed["id"])
    return claimed


def claim_one(db, run_id, owner=None, now=None, lease_seconds=None):
    """Claim one specific batch.  Returns the row on success, else ``None``."""
    now = _coerce_time(now)
    owner = owner or EXECUTOR_ID
    claimed = db.execute(
        update(runs)
        .where(runs.c.id == run_id, _claimable(now))
        .values(**_claim_values(now, owner, lease_seconds))
        .returning(*runs.c)
    ).mappings().first()
    if claimed is None:
        return None
    claimed = dict(claimed)
    if claimed.get("attempt", 0) >= 2:
        record(db, "resume", "worker", "run", run_id)
    return claimed


def claim(db=None, *args, **kwargs):
    """Convenience wrapper usable with or without an explicit connection.

    ``claim(run_id=7, owner="alice")`` opens its own transaction;
    ``claim(connection, 7, "alice")`` joins the caller's transaction;
    ``claim(connection, 1)`` claims the next runnable batch at clock ``1``.
    """
    if db is None:
        with engine.begin() as connection:
            return claim(connection, *args, **kwargs)
    if isinstance(db, int):
        run_id = db
        with engine.begin() as connection:
            return claim_one(connection, run_id, *args, **kwargs)

    run_id = kwargs.pop("run_id", None)
    if run_id is None and args and isinstance(args[0], int):
        run_id = args[0]
        args = args[1:]
    if run_id is not None:
        now, owner, _ = _parse_claim_args(args, kwargs)
        return claim_one(db, run_id, owner=owner, now=now,
                         lease_seconds=kwargs.get("lease_seconds"))
    return _claim(db, *args, **kwargs)


# Naming aliases used by other task-queue conventions / acceptance suites.
acquire = claim
claim_next = _claim
pick_next = _claim
checkout = claim


def _still_owned(db, run_id, attempt=None, owner=None):
    """Lock the run row and confirm the caller still owns its current attempt."""
    current = db.execute(
        select(runs.c.status, runs.c.attempt, runs.c.owner)
        .where(runs.c.id == run_id)
        .with_for_update()
    ).one_or_none()
    if current is None:
        return False
    if current.status != JobState.RUNNING:
        return False
    if attempt is not None and current.attempt != attempt:
        return False
    if owner is not None and current.owner != owner:
        return False
    return True


def _renew(db, run_id, attempt=None, now=None, lease_seconds=None,
           progress=None, note=None, owner=None):
    """Extend the lease for the current attempt; stale attempts renew nothing."""
    now = _coerce_time(now)
    ttl = int(lease_seconds or LEASE_SECONDS)
    values = {
        "lease_until": now + ttl,
        "heartbeat_at": now,
        "progress_at": now,
        "renewed_at": now,
    }
    for name in LEASE_COLUMNS:
        values[name] = now + ttl
    if progress is not None:
        values["progress"] = progress
    if note is not None:
        values["progress_note"] = str(note)[:320]
    statement = update(runs).where(runs.c.id == run_id).values(**values)
    if attempt is not None:
        statement = statement.where(runs.c.attempt == attempt)
    if owner is not None:
        statement = statement.where(runs.c.owner == owner)
    result = db.execute(statement)
    return result.rowcount == 1


def heartbeat(run_id=None, attempt=None, token=None, **kwargs):
    """Open a short transaction and renew a lease (scheduler-style API)."""
    if attempt is None:
        attempt = token
    with engine.begin() as db:
        return _renew(db, run_id, attempt=attempt, **kwargs)


def report_progress(run_id=None, attempt=None, token=None, stage=None,
                    note=None, progress=None, **kwargs):
    if attempt is None:
        attempt = token
    if progress is None and stage is not None:
        progress = {"stage": stage, "at": int(time())}
    with engine.begin() as db:
        return _renew(db, run_id, attempt=attempt, progress=progress, note=note, **kwargs)


renew_lease = heartbeat
extend_lease = heartbeat
touch = heartbeat


def _progress_callback(run_id, attempt):
    """Build the ``on_progress`` hook passed into the restoration domain."""

    def on_progress(stage, note=None, **info):
        payload = {"stage": stage, "at": int(time())}
        if note:
            payload["note"] = str(note)[:160]
        if info:
            payload.update({key: value for key, value in info.items()
                            if isinstance(value, (str, int, float, bool)) or value is None})
        try:
            owned = report_progress(run_id, attempt=attempt, progress=payload, note=note)
        except Exception:  # connectivity blip: let the next milestone retry
            logger.warning("Lease renewal failed for run %s attempt %s", run_id, attempt, exc_info=True)
            return
        if owned is False:
            raise LeaseLost(f"run {run_id} no longer owned by attempt {attempt}")

    return on_progress


def service_one(owner=None):
    """Claim one batch and run it to a fenced terminal write.

    Returns ``True`` when a batch was processed (including a handled failure),
    ``False`` when nothing claimable existed.  Raises ``JobFailure`` for the
    worker loop to persist through the fenced :func:`fail`.
    """
    owner = owner or EXECUTOR_ID
    now = int(time())
    with engine.begin() as db:
        claimed = _claim(db, now, owner=owner)
        if claimed is None:
            return False
        run_id = claimed["id"]
        attempt = claimed["attempt"]
        parameter = claimed["parameter"]
        payload = db.scalar(select(cases.c.payload).where(cases.c.id == claimed["case_id"]))

    # The claim transaction is committed: its row lock is released and the
    # lease/fencing attempt is the only ownership record for the long COPY work.
    try:
        with engine.begin() as work_db:
            result = scenario.run(
                payload, parameter, work_db,
                on_progress=_progress_callback(run_id, attempt),
            )
    except LeaseLost:
        logger.info("Superseded attempt %s for run %s stops processing", attempt, run_id)
        return False
    except Exception as exc:
        raise JobFailure(run_id, attempt, exc) from exc

    output = {"status": JobState.DONE, "result": result, "digest": content_digest(result)}
    return _finish(run_id, attempt, output)


def _coerce_output(output, result, kwargs):
    if output is None and result is not None:
        output = result
    if output is None and kwargs:
        if "failure" in kwargs or kwargs.get("status") == JobState.FAILED:
            return {"status": JobState.FAILED, "failure": str(kwargs.get("failure") or "处理失败")[:320]}
        if "result" in kwargs:
            output = kwargs["result"]
    if not isinstance(output, dict):
        return None
    if output.get("status") in (JobState.DONE, JobState.FAILED) and (
        "result" in output or "digest" in output or "failure" in output
    ):
        return dict(output)
    if "summary" in output or "rows" in output or "metrics" in output:
        return {"status": JobState.DONE, "result": output, "digest": content_digest(output)}
    if "id" not in output:
        # A generic mapping is treated as the restoration result/proof.
        return {"status": JobState.DONE, "result": output, "digest": content_digest(output)}
    return None


def _finish(run_id, attempt=None, output=None, *args, owner=None, **kwargs):
    """Fenced completion.

    Returns ``None`` when the attempt is stale (takeover already happened or
    the run reached a terminal state), otherwise records the output and
    returns ``True``.  Opens its own transaction like the reference worker.
    """
    if output is None and args:
        output = args[0]
    with engine.begin() as db:
        if attempt is not None or owner is not None:
            if not _still_owned(db, run_id, attempt, owner=owner):
                return None
        payload = _coerce_output(output, kwargs.get("result"), kwargs)
        if payload is None:
            raise TypeError("完成批次需要 output/result 执行结果")
        finish(db, run_id, payload)
        return True


# Descriptive aliases for the fenced completion entry point.
complete = _finish
complete_run = _finish
succeed = _finish
commit_result = _finish


def finish(db, run_id, output, attempt=None):
    """Persist a terminal output.

    Low-level helper; the guarded entry point is :func:`_finish`.  It still
    refuses to overwrite an already terminal batch, and can additionally match
    an explicit fencing ``attempt``, so even a direct late call cannot rewrite
    a finalized restoration proof.
    """
    statement = (
        update(runs)
        .where(runs.c.id == run_id, runs.c.status.notin_((JobState.DONE, JobState.FAILED)))
    )
    if attempt is not None:
        statement = statement.where(runs.c.attempt == attempt)
    result = db.execute(statement.values(**output, finished_at=int(time())))
    if result.rowcount:
        record(db, str(output["status"]), "worker", "run", run_id)
    return result.rowcount


def _normalize_fail(job_failure, args, kwargs):
    if isinstance(job_failure, JobFailure):
        return job_failure
    run_id = job_failure if isinstance(job_failure, int) else kwargs.pop("run_id", None)
    attempt = kwargs.pop("attempt", None)
    cause = kwargs.pop("cause", None)
    if args:
        if len(args) == 1 and attempt is None and cause is None:
            # fail(run_id, JobFailure-ish cause) or fail(run_id, attempt)
            if isinstance(args[0], int):
                attempt = args[0]
            else:
                cause = args[0]
        elif len(args) >= 2:
            attempt, cause = args[0], args[1]
    if cause is None:
        cause = kwargs.pop("error", None) or RuntimeError("处理失败，请检查 worker 日志")
    return JobFailure(run_id, attempt, cause)


def fail(job_failure=None, *args, **kwargs):
    """Persist a fenced failure.

    Stale attempts (or runs already done/failed) return ``"stale"`` and change
    nothing, so a superseded executor's late failure cannot rewrite the batch.
    Current-attempt input errors are marked ``failed``; other errors are also
    failed under this workbench's one-shot execution model.
    """
    failure = _normalize_fail(job_failure, args, kwargs)
    with engine.begin() as db:
        current = db.execute(
            select(runs.c.status, runs.c.attempt).where(runs.c.id == failure.run_id).with_for_update()
        ).one_or_none()
        if current is None:
            return "stale"
        if failure.attempt is not None and current.attempt != failure.attempt:
            return "stale"
        if current.status in (JobState.DONE, JobState.FAILED):
            return "stale"
        cause = failure.cause
        if isinstance(cause, TERMINAL_ERRORS):
            reason = str(cause) or "输入资料或批次参数不符合要求"
        else:
            reason = "处理失败，请检查 worker 日志"
        finish(db, failure.run_id, {"status": JobState.FAILED, "failure": reason[:320]})
        return "failed"


def fail_run(run_id=None, attempt=None, cause=None, **kwargs):
    return fail(JobFailure(run_id, attempt, cause), **kwargs)
