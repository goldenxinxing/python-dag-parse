from loguru import logger

from src.decorator import step
from src.ppl import Context
import inspect


# class TestStep:
@step(
    resources="gpu=1,cpu=2",
    concurrency=True,
    concurrency_level="1/2/3/4",
    task_num=2
)
def evaluate_ppl(_context: Context):
    """
    step 'ppl' demo
    :param _context: common param
    """
    logger.debug("test ppl")
    # store.set(f"xxx/{context['id']}")
    # return {'eval_res': results, 'label': label}


@step(
    resources="cpu=1",
    concurrency=False,
    dependency='evaluate_ppl'
)
def evaluate_cmp(_context: Context):
    """
        step 'cmp' demo
        :param _context: common param
        """
    logger.debug("test cmp")


def get_methods(self):
    return filter(lambda x: inspect.isfunction(getattr(self, x)) and callable(getattr(self, x)), dir(self))
