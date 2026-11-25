from enum import StrEnum


class JobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


# 仍在占用（或等待）经办的批次状态：巡修组据此判断批次是否还在处理中。
ACTIVE_STATES = (JobState.QUEUED, JobState.RUNNING)
TERMINAL_STATES = (JobState.DONE, JobState.FAILED)
