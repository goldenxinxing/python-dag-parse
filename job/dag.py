import importlib
import sys
from pathlib import Path

import pkg_resources
import yaml
from loguru import logger

from job.model import Context

# not thread safe
JOBS = {}


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


def parse_from_module(module: str, path: str):
    """
    parse @step from module
    :param module: module name
    :param path: abs path
    :return: jobs
    """
    global JOBS
    # parse DAG
    logger.debug("parse @step for module:{}", module)
    load_module(module, path)
    _jobs = JOBS
    JOBS = {}
    return _jobs


def generate_job_yaml(module: str, path: str, target_dir: str):
    """
    generate job yaml
    :param target_dir: yaml target path
    :param module: module name
    :param path: abs path
    :return: None
    """
    _jobs = parse_from_module(module, path)
    # generate DAG
    logger.debug("generator DAG")
    # check
    checks = []
    for job in _jobs.items():
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
        logger.debug("check success! \n{}", yaml.dump(_jobs))
    # todo dump to target
    with open(target_dir + '/jobs.yaml', 'w') as file:
        yaml.dump(_jobs, file)


def call_function(function: str, module: str, path: str):
    """
    call function from module
    :param function: function name
    :param module: module name
    :param path: abs path
    :return: function results
    """
    logger.debug("call function:{}", function)

    _module = load_module(module, path)

    # test execute
    func = getattr(_module, function, None)
    _context = Context()
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
