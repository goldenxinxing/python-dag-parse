import importlib
import sys
from pathlib import Path

import pkg_resources
from loguru import logger

from src import decorator
from src.ppl import Context
import click


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


@click.command()
@click.option("--pkg", help="scan pkgs.")
@click.option("--target", help="scan pkgs.")
def parser(pkg: str, target: str):
    workdir = Path(target)
    workdir_path = str(workdir.absolute())
    # add module path to sys path
    external_paths = [workdir_path]
    for _path in external_paths[::-1]:
        if _path not in sys.path:
            logger.debug(f"insert sys.path: '{_path}'")
            sys.path.insert(0, _path)
            pkg_resources.working_set.add_entry(_path)
    # parse DAG
    logger.debug("parse Dag:")
    _module = importlib.import_module(pkg, package=workdir_path)

    logger.debug("call function")
    # test decorator
    for step in decorator.jobs.items():
        print(step[0], step[1])
        # test execute
        func = getattr(_module, step[0], None)
        _context = Context()
        func(_context)


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
