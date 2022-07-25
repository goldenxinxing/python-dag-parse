# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from job import Scheduler
from job.model import *


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    logger.debug(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('starwhale!')

    # parse DAG
    logger.debug("1.parse and generator DAG")
    JobDAG.generate_job_yaml("step-class", "./", "./")

    # test decorator
    logger.debug("2.call function")
    _task = Task(Context(step="TestStep.evaluate_ppl2", total=1, index=0), STATUS.INIT)
    _task.execute("step-class", "./")

    logger.debug("3.parse yaml for job run")

    _jobs = JobDAG.parse_job_from_yaml("jobs.yaml")
    logger.debug("yaml result:\n{}", _jobs)

    # steps of job
    logger.debug("4.run job")

    _steps = _jobs["default-2"]
    scheduler = Scheduler(_steps)
    scheduler.schedule()


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
