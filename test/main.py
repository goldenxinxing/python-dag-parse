# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import yaml
from loguru import logger

from job import decorator
from job.model import Context


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    logger.debug(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('starwhale!')

    # parse DAG
    logger.debug("1.parse DAG:")
    _module = __import__("step")

    # generate DAG
    logger.debug("2.generator DAG")
    # check
    checks = []
    for job in decorator.JOBS.items():
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
        logger.debug("check success! \n{}", yaml.dump(decorator.JOBS))

    # test decorator
    logger.debug("3.call function")
    for job in decorator.JOBS.items():
        # test decorator
        for step in job[1].items():
            logger.debug("job is:{}, function:{}, details:{}", job[0], step[0], step[1])
            # test execute
            func = getattr(_module, step[0], None)
            _context = Context()
            func(_context)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
