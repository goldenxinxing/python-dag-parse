from loguru import logger

from job.dag import Step
from job.dag import JOBS


def step(
        job_name: str = "default",
        resources: str = "gpu=1,cpu=2",
        concurrency: bool = False,
        concurrency_level: str = "1",
        task_num: int = 2,
        dependency: str = ""):
    def decorator(func):
        __func_name = func.__qualname__

        JOBS.setdefault(job_name, {})
        job = JOBS[job_name]

        _step = Step(job_name, __func_name, resources, concurrency, concurrency_level, task_num, dependency)

        logger.debug(_step)
        job[__func_name] = _step
        return func

    return decorator
