import logging
import time

from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.config.resources import cache, engine
from src.services.execution import EXECUTOR_ID, JobFailure, fail, service_one

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")
logger.info("Worker executor identity: %s", EXECUTOR_ID)


def check():
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    heartbeat = cache.get("workbench:heartbeat")
    return heartbeat is not None and time.time() - float(heartbeat) < 90


def work():
    while True:
        try:
            cache.set("workbench:heartbeat", time.time(), ex=90)
            try:
                if service_one(owner=EXECUTOR_ID):
                    continue
            except JobFailure as exc:
                logger.exception("Execution %s failed (attempt %s)", exc.run_id, exc.attempt)
                outcome = fail(exc)
                if outcome == "stale":
                    # A newer attempt owns the batch; this late failure is ignored.
                    logger.info("Late failure from attempt %s on run %s rejected", exc.attempt, exc.run_id)
                continue
            cache.blpop("workbench:wake", timeout=1)
        except (RedisError, SQLAlchemyError):
            logger.exception("Worker connection unavailable")
            time.sleep(2)


if __name__ == "__main__":
    work()
