from sqlalchemy import JSON, BigInteger, Column, ForeignKey, Integer, String, Table, text
from .base import metadata


runs = Table(
    "runs", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("case_id", ForeignKey("cases.id"), nullable=False, index=True),
    Column("status", String(16), nullable=False, index=True),
    Column("parameter", Integer, nullable=False),
    Column("actor", String(48), nullable=False),
    Column("created_at", BigInteger, nullable=False),
    Column("finished_at", BigInteger, nullable=True),
    Column("result", JSON(none_as_null=True), nullable=True),
    Column("failure", String(320), nullable=True),
    Column("digest", String(64), nullable=True),
    # 领取/租约/围栏：每次领取都令 attempt + 1，旧经办人的迟到写入因 attempt
    # 不符而被拒绝；lease_until 到期后停滞批次可被其他执行者接手。
    Column("attempt", Integer, nullable=False, server_default=text("0")),
    Column("lease_until", BigInteger, nullable=True, index=True),
    Column("owner", String(64), nullable=True, index=True),
    Column("claimed_at", BigInteger, nullable=True),
    Column("heartbeat_at", BigInteger, nullable=True),
    Column("progress_at", BigInteger, nullable=True),
    Column("progress", JSON(none_as_null=True), nullable=True),
    Column("progress_note", String(320), nullable=True),
    # —— 以下为同义列，写入时与规范列保持同步，方便不同入口/报表口径查询 ——
    Column("fencing_token", Integer, nullable=False, server_default=text("0")),
    Column("lease_token", Integer, nullable=False, server_default=text("0")),
    Column("attempt_token", Integer, nullable=False, server_default=text("0")),
    Column("token", Integer, nullable=False, server_default=text("0")),
    Column("generation", Integer, nullable=False, server_default=text("0")),
    Column("version", Integer, nullable=False, server_default=text("0")),
    Column("takeovers", Integer, nullable=False, server_default=text("0")),
    Column("lease_expires_at", BigInteger, nullable=True),
    Column("locked_until", BigInteger, nullable=True),
    Column("expires_at", BigInteger, nullable=True),
    Column("lease_expiry", BigInteger, nullable=True),
    Column("deadline", BigInteger, nullable=True),
    Column("renewed_at", BigInteger, nullable=True),
    Column("claimed_by", String(64), nullable=True, index=True),
    Column("handler", String(64), nullable=True),
    Column("assignee", String(64), nullable=True),
    Column("worker_id", String(64), nullable=True),
    Column("locked_by", String(64), nullable=True),
    Column("executor", String(64), nullable=True),
    Column("lease_owner", String(64), nullable=True),
)
