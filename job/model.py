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
        return "name:{0},dependency:{1}".format(self.step_name, self.dependency)

    def gen_task(self, index):
        self.tasks.append(
            Task(Context(step=self.step_name, total=self.task_num, index=index, dataset_uri=""), STATUS.INIT)
        )
