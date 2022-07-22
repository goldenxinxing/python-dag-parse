from loguru import logger

import click

from job import decorator
from job.dag import call_function, parse_job_from_yaml
from job.dag import generate_job_yaml
from job.executor import Scheduler
from job.model import Context, Task, STATUS

__version__ = "0.0.1"


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


@click.command("gen")
@click.option("--module", help="scan modules.")
@click.option("--path", help="scan path.")
def generator(module: str, path: str):
    # parse DAG
    logger.debug("generator DAG")
    generate_job_yaml(module, path, path)


@click.command("call")
@click.option("--function", help="call function.")
@click.option("--module", help="scan modules.")
@click.option("--path", help="scan path.")
def parser(function: str, module: str, path: str):
    # parse DAG
    logger.debug("call function")
    res = call_function(function, module, path, Task(Context(), STATUS.INIT))
    print("result:{}".format(res))


@click.command("run")
@click.option("--file", help="job yaml file.")
@click.option("--job", default="default", help="job to run.")
def run(file: str, job: str):
    # parse DAG
    logger.debug("run job from yaml")
    _jobs = parse_job_from_yaml(file)
    # steps of job
    _steps = _jobs[job]
    scheduler = Scheduler(_steps)
    scheduler.schedule()
    print("result:\n{}".format(_jobs))


def create_cli() -> click.core.Group:
    @click.group()
    @click.version_option(version=__version__)
    @click.pass_context
    def cli_group(ctx: click.Context) -> None:
        ctx.ensure_object(dict)

    cli_group.add_command(parser)
    cli_group.add_command(generator)
    cli_group.add_command(run)

    return cli_group


cli = create_cli()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    p = parser("step", "/mnt/c/Users/gaoxinxing/PycharmProjects/swtest/test")
    # print_hi('PyCharm')
    # # print(step.TestStep().get_methods())
    # dd = __import__("step")
    #
    # # test decorator
    # for step in decorator.steps.items():
    #     print(step[0], step[1])
    # # test execute
    # evaluate_ppl = getattr(dd, "evaluate_ppl", None)
    # _context = Context()
    # evaluate_ppl(_context)
