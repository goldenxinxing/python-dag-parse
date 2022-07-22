# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from job import Scheduler
from job.dag import *


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    logger.debug(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('starwhale!')

    # parse DAG
    logger.debug("1.parse DAG:")
    jobs = parse_job_from_module("step-class", "./")

    # generate DAG
    logger.debug("2.generator DAG")
    generate_job_yaml("step-class", "./", "./")

    # test decorator
    logger.debug("3.call function")
    call_function("TestStep.evaluate_ppl2", "step-class", "./", Task(Context(), STATUS.INIT))

    logger.debug("4.parse yaml for job run")

    _jobs = parse_job_from_yaml("jobs.yaml")
    logger.debug("yaml result:\n{}", _jobs)

    # steps of job
    logger.debug("5.run job")

    _steps = _jobs["default-2"]
    scheduler = Scheduler(_steps)
    scheduler.schedule()


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
