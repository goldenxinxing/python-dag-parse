
import importlib
import sys
from pathlib import Path

import pkg_resources
import yaml
from loguru import logger
import typing as t

from job.model import Context


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


# not thread safe
parse_config = {
    "parse_stage": False,
    "jobs": {}
}


def set_parse_stage(parse_stage: bool):
    parse_config["parse_stage"] = parse_stage


def is_parse_stage():
    return parse_config["parse_stage"]


def add_job(job_name: str, step: Step) -> None:
    parse_config["jobs"].setdefault(job_name, {})

    logger.debug(step)
    parse_config["jobs"][job_name][step.get_func()] = step


def get_jobs():
    return parse_config["jobs"]

# load is unique,so don't need to think multi load and clean
# def clear_config():
#     global parse_config
#     parse_config = {
#         "parse_stage": False,
#         "jobs": {}
#     }


def parse_job_from_module(module: str, path: str):
    """
    parse @step from module
    :param module: module name
    :param path: abs path
    :return: jobs
    """
    set_parse_stage(True)
    # parse DAG
    logger.debug("parse @step for module:{}", module)
    load_module(module, path)
    _jobs = get_jobs()

    return _jobs


def generate_job_yaml(module: str, path: str, target_dir: str) -> None:
    """
    generate job yaml
    :param target_dir: yaml target path
    :param module: module name
    :param path: abs path
    :return: None
    """
    _jobs = parse_job_from_module(module, path)
    # generate DAG
    logger.debug("generator DAG")
    if check(_jobs):
        # dump to target
        with open(target_dir + '/jobs.yaml', 'w') as file:
            yaml.dump(_jobs, file)
        logger.debug("generator DAG success!")
    else:
        logger.error("generator DAG error! reason:{}", "check is failed.")


def check(jobs: dict) -> bool:
    # check
    checks = []
    for job in jobs.items():
        all_steps = []
        dependencies = []
        for step in job[1].items():
            all_steps.append(step[1].get_func())
            if step[1].get_dependency():
                dependencies.append(step[1].get_dependency())
        _check = all(item in all_steps for item in dependencies)
        if not _check:
            logger.error("job:{} check error!", job[0])
        checks.append(_check)
    # all is ok
    if all(c is True for c in checks):
        logger.debug("check success! \n{}", yaml.dump(jobs))
        return True
    else:
        return False


def call_function(step_name: str, module: str, path: str) -> t.Any:
    """
    call function from module
    :param step_name: step name
    :param module: module name
    :param path: abs path
    :return: function results
    """
    logger.debug("call function:{}", step_name)

    _module = load_module(module, path)

    # todo instance by actual param
    _context = Context()
    # test execute
    if "." in step_name:
        _cls_name, _func_name = step_name.split(".")
        _cls = getattr(_module, _cls_name, None)
        # need an instance
        cls = _cls()
        func = getattr(cls, _func_name, None)
    else:
        _func_name = step_name
        func = getattr(_module, _func_name, None)

    return func(_context)


def load_module(module: str, path: str):
    """
    load module from path
    :param module: module name
    :param path: abs path
    :return: module
    """
    workdir = Path(path)
    workdir_path = str(workdir.absolute())
    # add module path to sys path
    external_paths = [workdir_path]
    for _path in external_paths[::-1]:
        if _path not in sys.path:
            logger.debug(f"insert sys.path: '{_path}'")
            sys.path.insert(0, _path)
            pkg_resources.working_set.add_entry(_path)

    return importlib.import_module(module, package=workdir_path)
