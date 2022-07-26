import threading
import time

from loguru import logger

from job.decorator import step
from job.model import Context


class TestStep:
    @step(
        job_name='default-2',
        resources="gpu=1,cpu=2",
        concurrency=2,
        task_num=6
    )
    def evaluate_ppl(self, _context: Context):
        """
        step 'ppl' demo
        :param _context: common param
        """
        logger.debug("thread:{}, test ppl start, index:{}", threading.currentThread().getName(), _context.index)
        time.sleep(1)
        # raise RuntimeError("test error in ppl")
        logger.debug("thread:{}, test ppl end, index:{}", threading.currentThread().getName(), _context.index)
        # store.set(f"xxx/{context['id']}")
        # return {'eval_res': results, 'label': label}

    @step(
        job_name='default-2',
        resources="cpu=1",
        dependency=''
    )
    def evaluate_ppl1(self, _context: Context):
        """
            step 'cmp' demo
            :param _context: common param
            """
        logger.debug("thread:{}, test ppl1 start, index:{}", threading.currentThread().getName(), _context.index)
        time.sleep(1)
        logger.debug("thread:{}, test ppl1 end, index:{}", threading.currentThread().getName(), _context.index)

    @step(
        job_name='default-2',
        resources="cpu=1",
        dependency='TestStep.evaluate_ppl,TestStep.evaluate_ppl1'
    )
    def evaluate_cmp(self, _context: Context):
        """
            step 'cmp' demo
            :param _context: common param
            """
        logger.debug("thread:{}, test cmp start, index:{}", threading.currentThread().getName(), _context.index)
        time.sleep(1)
        logger.debug("thread:{}, test cmp end, index:{}", threading.currentThread().getName(), _context.index)

    @step(
        job_name='default-2',
        resources="cpu=1",
        dependency='TestStep.evaluate_cmp'
    )
    def evaluate_final(self, _context: Context):
        """
            step 'cmp' demo
            :param _context: common param
            """
        logger.debug("thread:{}, test final start, index:{}", threading.currentThread().getName(), _context.index)
        time.sleep(1)
        logger.debug("thread:{}, test final end, index:{}", threading.currentThread().getName(), _context.index)

    @step(
        job_name='second-2',
        resources="gpu=1,cpu=2",
        concurrency=1,
        task_num=2
    )
    def evaluate_ppl2(self, _context: Context):
        """
        step 'ppl' demo
        :param _context: common param
        """
        logger.debug("test ppl2")
        # store.set(f"xxx/{context['id']}")
        # return {'eval_res': results, 'label': label}

    @step(
        job_name='second-2',
        resources="cpu=1",
        concurrency=1,
        dependency='TestStep.evaluate_ppl2'
    )
    def evaluate_cmp2(self, _context: Context):
        """
            step 'cmp' demo
            :param _context: common param
            """
        logger.debug("test cmp2")

    #
    # def get_methods(self):
    #     return filter(lambda x: inspect.isfunction(getattr(self, x)) and callable(getattr(self, x)), dir(self))
