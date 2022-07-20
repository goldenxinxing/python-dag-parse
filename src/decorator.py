from loguru import logger

jobs = {}


class Step:
    def __init__(self,
                 job_name: str,
                 func_name: str,
                 resources: str = "cpu=1",
                 concurrency: bool = False,
                 concurrency_level: str = "1",
                 task_num: int = 1,
                 dependency: str = ""):
        self.job_name = job_name
        self.func_name = func_name
        self.resources = resources
        self.concurrency = concurrency
        self.concurrency_level = concurrency_level
        self.task_num = task_num
        self.dependency = dependency

    def __repr__(self):
        return "name:{0},dependency:{1}".format(self.func_name, self.dependency)

    def get_func(self):
        return self.func_name

    def get_dependency(self):
        return self.dependency


def step(
        job_name: str = "default",
        resources: str = "gpu=1,cpu=2",
        concurrency: bool = True,
        concurrency_level: str = "1/2/3/4",
        task_num: int = 2,
        dependency: str = ""):
    def decorator(func):
        global jobs
        __func_name = func.__qualname__

        jobs.setdefault(job_name, {})
        job = jobs[job_name]

        _step = Step(job_name, __func_name, resources, concurrency, concurrency_level, task_num, dependency)

        logger.debug(_step)
        job[__func_name] = _step
        return func

    return decorator
