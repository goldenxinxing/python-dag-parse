
from job.dag import Step
from job.dag import is_parse_stage, add_job


def step(
        job_name: str = "default",
        resources: str = "gpu=1,cpu=2",
        concurrency: int = 1,
        task_num: int = 2,
        dependency: str = ""):
    def decorator(func):
        if is_parse_stage():
            _step = Step(job_name, func.__qualname__, resources, concurrency, task_num, dependency)
            add_job(job_name, _step)

        return func

    return decorator
