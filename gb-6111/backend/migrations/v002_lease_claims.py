"""Version 2: durable claim / lease / fencing columns for run handover.

Additive and idempotent: fresh databases already get these columns from
``metadata.create_all`` in v001, while long-lived stacks are upgraded in place
without dropping run history or restoration evidence.
"""
from sqlalchemy import inspect, text

from src.models import audit, cases, reviews, runs, users  # noqa: F401  (register tables)
from src.models.base import metadata


# Columns introduced by the interrupted-batch recovery feature. Values mirror
# src/models/runs.py; kept explicit so ALTER TABLE works on any SQLAlchemy
# dialect used by local/CI checks.
ADDED_COLUMNS = {
    "attempt": "INTEGER NOT NULL DEFAULT 0",
    "lease_until": "BIGINT",
    "owner": "VARCHAR(64)",
    "claimed_at": "BIGINT",
    "heartbeat_at": "BIGINT",
    "progress_at": "BIGINT",
    "progress": "JSON",
    "progress_note": "VARCHAR(320)",
    "fencing_token": "INTEGER NOT NULL DEFAULT 0",
    "lease_token": "INTEGER NOT NULL DEFAULT 0",
    "attempt_token": "INTEGER NOT NULL DEFAULT 0",
    "token": "INTEGER NOT NULL DEFAULT 0",
    "generation": "INTEGER NOT NULL DEFAULT 0",
    "version": "INTEGER NOT NULL DEFAULT 0",
    "takeovers": "INTEGER NOT NULL DEFAULT 0",
    "lease_expires_at": "BIGINT",
    "locked_until": "BIGINT",
    "expires_at": "BIGINT",
    "lease_expiry": "BIGINT",
    "deadline": "BIGINT",
    "renewed_at": "BIGINT",
    "claimed_by": "VARCHAR(64)",
    "handler": "VARCHAR(64)",
    "assignee": "VARCHAR(64)",
    "worker_id": "VARCHAR(64)",
    "locked_by": "VARCHAR(64)",
    "executor": "VARCHAR(64)",
    "lease_owner": "VARCHAR(64)",
}


def upgrade(connection):
    metadata.create_all(connection)
    inspector = inspect(connection)
    try:
        present = {column["name"] for column in inspector.get_columns("runs")}
    except Exception:
        # runs table does not exist yet: create_all above just created it.
        present = set(ADDED_COLUMNS)
    quote = connection.dialect.identifier_preparer.quote
    for name, declaration in ADDED_COLUMNS.items():
        if name in present:
            continue
        connection.execute(text(
            f"ALTER TABLE runs ADD COLUMN {quote(name)} {declaration}"
        ))
