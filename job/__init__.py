import importlib
import sys
from pathlib import Path

import pkg_resources
from loguru import logger

import click

from job import decorator
from job.dag import call_function
from job.dag import generate_job_yaml
from job.model import Context

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
    res = call_function(function, module, path)
    print("result:{}".format(res))


def create_cli() -> click.core.Group:
    @click.group()
    @click.version_option(version=__version__)
    @click.pass_context
    def cli_group(ctx: click.Context) -> None:
        ctx.ensure_object(dict)

    cli_group.add_command(parser)
    cli_group.add_command(generator)

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
