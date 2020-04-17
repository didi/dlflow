from dlflow.utils.locale import i18n
from time import perf_counter
from absl import logging
import abc


class TaskTag(object):
    def __init__(self):
        self.tags = None

    def __iter__(self):
        iter_obj = []
        if self.tags:
            iter_obj = list(self.tags)

        return iter(iter_obj)

    def set_tags(self, *args):
        self.tags = args

    def get_tags(self):
        return list(self.tags)


class TaskNode(metaclass=abc.ABCMeta):
    parent_tag = TaskTag()
    output_tag = TaskTag()
    bind_tasks = []

    def __init__(self):
        self.status = "Init"

    @abc.abstractmethod
    def run(self):
        pass

    @staticmethod
    def set_tag(*args):
        tag = TaskTag()
        tag.set_tags(*args)

        return tag

    @staticmethod
    def timeit(func):
        task_name = func.__module__

        def _timeit(*args, **kwargs):
            logging.info(i18n("Start running: {}...")
                         .format(task_name))

            time_enter = perf_counter()
            func(*args, **kwargs)
            time_elapsed = perf_counter() - time_enter

            logging.info(
                i18n("Task <{}> spend time: {:>.6f}s")
                .format(task_name, time_elapsed))
        return _timeit
