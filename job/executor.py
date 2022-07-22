import concurrent.futures
import threading
import time

from loguru import logger

from job.dag import Step, call_function
from job.model import STATUS


class Scheduler:
    def __init__(self, steps: dict[Step]):
        self.steps = steps
        self.__split_tasks()
        self._lock = threading.RLock()

    def __split_tasks(self):
        for item in self.steps.items():
            _step = item[1]
            # update step status = init
            _step.status = STATUS.INIT
            for index in range(_step.task_num):
                _step.gen_task(index)

    def schedule(self) -> None:
        _threads = []
        with self._lock:
            _wait_steps = []
            _finished_step_names = []
            for item in self.steps.items():
                step = item[1]
                if step.status is STATUS.FAILED:
                    # todo break processing
                    pass
                if step.status is STATUS.SUCCESS:
                    _finished_step_names.append(step.step_name)
                if step.status is STATUS.INIT:
                    _wait_steps.append(step)
            # judge whether a step's dependency all in finished
            for wait in _wait_steps:
                if all(d in _finished_step_names for d in wait.dependency if d):
                    executor = Executor(self, wait)
                    executor.start()
                    # executor.setDaemon()
                    _threads.append(executor)

        for t in _threads:
            t.join()


class Executor(threading.Thread):
    def __init__(self, scheduler: Scheduler, step: Step):
        super().__init__()
        self.scheduler = scheduler
        self.step = step

    def run(self):
        self.step.status = STATUS.RUNNING
        # processing pool
        start_time = time.time()
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.step.concurrency) as executor:
            futures = [executor.submit(call_function, self.step.step_name, "step-class", "./", task)
                       for task in self.step.tasks]
            if all(future.result() for future in concurrent.futures.as_completed(futures)):
                self.step.status = STATUS.SUCCESS
            else:
                self.step.status = STATUS.FAILED

        end_time = time.time()
        logger.debug("step:{} finished, status:{}, run time:{}",
                     self.step.step_name, self.step.status, end_time - start_time)
        # trigger next schedule
        self.scheduler.schedule()

