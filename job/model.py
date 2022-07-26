import yaml
from loguru import logger

from job.loader import load_module


class Step:

    def __init__(self,
                 job_name: str,
                 step_name: str,
                 resources: str = "cpu=1",
                 concurrency: int = 1,
                 task_num: int = 1,
                 dependency: str = ""):
        self.job_name = job_name
        self.step_name = step_name
        self.resources = resources.strip().split(",")
        self.concurrency = concurrency
        self.task_num = task_num
        self.dependency = dependency.strip().split(",")
        self.status = ""
        self.tasks = []

    def __repr__(self):
        return "step_name:{0}, dependency:{1}, status: {2}".format(self.step_name, self.dependency, self.status)

    def gen_task(self, index):
        self.tasks.append(
            Task(Context(step=self.step_name, total=self.task_num, index=index, dataset_uri=""), STATUS.INIT)
        )


# shared memory, not thread safe
parse_config = {
    "parse_stage": False,
    "jobs": {}
}


class Parser:

    @staticmethod
    def set_parse_stage(parse_stage: bool):
        parse_config["parse_stage"] = parse_stage

    @staticmethod
    def is_parse_stage():
        return parse_config["parse_stage"]

    @staticmethod
    def add_job(job_name: str, step: Step) -> None:
        parse_config["jobs"].setdefault(job_name, {})

        logger.debug(step)
        parse_config["jobs"][job_name][step.step_name] = step

    @staticmethod
    def get_jobs():
        return parse_config["jobs"]

    # load is unique,so don't need to think multi load and clean
    @staticmethod
    def clear_config():
        global parse_config
        parse_config = {
            "parse_stage": False,
            "jobs": {}
        }

    @staticmethod
    def parse_job_from_module(module: str, path: str):
        """
        parse @step from module
        :param module: module name
        :param path: abs path
        :return: jobs
        """
        Parser.set_parse_stage(True)
        # parse DAG
        logger.debug("parse @step for module:{}", module)
        load_module(module, path)
        _jobs = Parser.get_jobs().copy()
        Parser.clear_config()
        return _jobs


class JobDAG:

    @staticmethod
    def generate_job_yaml(module: str, path: str, target_dir: str) -> None:
        """
        generate job yaml
        :param target_dir: yaml target path
        :param module: module name
        :param path: abs path
        :return: None
        """
        _jobs = Parser.parse_job_from_module(module, path)
        # generate DAG
        logger.debug("generate DAG")
        if JobDAG.check(_jobs):
            # dump to target
            with open(target_dir + '/jobs.yaml', 'w') as file:
                yaml.dump(_jobs, file)
            logger.debug("generator DAG success!")
        else:
            logger.error("generator DAG error! reason:{}", "check is failed.")

    @staticmethod
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

    @staticmethod
    def parse_job_from_yaml(file: str) -> dict:
        with open(file, 'r') as file:
            return yaml.unsafe_load(file)


# Runtime concept
class Context:
    def __init__(self,
                 step: str = "",
                 total: int = 0,
                 index: int = 0,
                 dataset_uri: str = ""
                 ):
        self.step = step
        self.total = total
        self.index = index
        self.dataset_uri = dataset_uri

    def __repr__(self):
        return "step:{}, total:{}, index:{}".format(self.step, self.total, self.index)


class STATUS:
    INIT = "init"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class Task:
    def __init__(self, context: Context, status: str):
        self.context = context
        self.status = status
        # todo

    def execute(self, module: str, path: str) -> bool:
        """
        call function from module
        :param module: module name
        :param path: abs path
        :return: function results
        """
        logger.debug("execute step:{} start.", self.context)

        _module = load_module(module, path)

        # instance method
        if "." in self.context.step:
            _cls_name, _func_name = self.context.step.split(".")
            _cls = getattr(_module, _cls_name, None)
            # need an instance
            cls = _cls()
            func = getattr(cls, _func_name, None)
        else:
            _func_name = self.context.step
            func = getattr(_module, _func_name, None)

        try:
            self.status = STATUS.RUNNING
            # The standard implementation does not return results
            func(self.context)
        except RuntimeError:
            self.status = STATUS.FAILED
            logger.error("execute step:{} error", self.context)
            return False
        else:
            self.status = STATUS.SUCCESS
            logger.debug("execute step:{} success", self.context)
            return True

