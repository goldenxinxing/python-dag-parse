
from job.model import Parser, Step


def step(
        job_name: str = "default",
        resources: str = "cpu=1",
        concurrency: int = 1,
        task_num: int = 1,
        dependency: str = ""):
    def decorator(func):
        if Parser.is_parse_stage():
            _step = Step(job_name, func.__qualname__, resources, concurrency, task_num, dependency)
            Parser.add_job(job_name, _step)

        return func

    return decorator
