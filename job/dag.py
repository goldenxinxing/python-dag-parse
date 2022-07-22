
import importlib
import sys
from pathlib import Path

import pkg_resources
import yaml
from loguru import logger
import typing as t

from job.model import Context, Step, STATUS, Task

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
    parse_config["jobs"][job_name][step.step_name] = step


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


def check(jobs: dict[str, dict[str, Step]]) -> bool:
    # check
    checks = []
    for job in jobs.items():
        all_steps = []
        dependencies = []
        for item in job[1].items():
            step = item[1]
            all_steps.append(step.step_name)
            for d in step.dependency:
                if d:
                    dependencies.append(d)
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


def parse_job_from_yaml(path: str) -> dict:
    with open(path, 'r') as file:
        return yaml.unsafe_load(file)


def call_function(step_name: str, module: str, path: str, task: Task) -> bool:
    """
    call function from module
    :param task: task info
    :param step_name: step name
    :param module: module name
    :param path: abs path
    :return: function results
    """
    logger.debug("call function:{}", step_name)

    _module = load_module(module, path)

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

    try:
        task.status = STATUS.RUNNING
        func(task.context)
    except RuntimeError:
        task.status = STATUS.FAILED
        logger.error("call step:{} error", step_name)
        return False
    else:
        task.status = STATUS.SUCCESS
        return True


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
