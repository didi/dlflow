"""
from dlflow.manager.workflow import TaskNode
from dlflow.manager.regmgr import task


@task.reg(alias=("foo1", ))
class _UDT(TaskNode):
    predecessor = ("_Predict", )
    successor = ("_Evaluate", )

    def __init__(self):
        super(self.__class__, self).__init__()

    def run(self):
        pass


@task.reg(alias=("foo2", ))
class _UDT1(TaskNode):
    predecessor = ("_Encode", "_Fe")
    successor = ("_UDT",)

    def __init__(self):
        super(self.__class__, self).__init__()

    def run(self):
        pass
"""

from dlflow.utils.locale import i18n
from dlflow.utils.utilclass import SingletonMeta
from dlflow.mgr.errors import InstantiateNotAllowed
from dlflow.mgr.errors import RegisterKeyDuplicate
from dlflow.utils import DLFLOW_TASKS, DLFLOW_MODELS

from pathlib import Path
from absl import logging
import sys
import os
import re


class _RegMeta(type):
    """
    可调用对象注册元类，用于生成注册类。
    """

    def __init__(cls, *args, **kwargs):
        """
        设置了双层映射，以便于用户通过自定义名称找到类实体
            1. 自定义名(alias name) -> 类标准名(class name)
            2. 类标准名(class name) -> 类实体
        """

        super(_RegMeta, cls).__init__(*args, **kwargs)
        cls.__MASKS__ = dict()
        cls.__REGS__ = dict()

    def __getreg__(cls, key):
        """
        注册是包含了 reg_key -> reg_key 的映射
        """

        if key in cls.__MASKS__:
            reg_key = cls.__MASKS__[key]
        else:
            raise KeyError(i18n("Unknown key '{}'").format(key))

        return cls.__REGS__[reg_key]

    def __getattr__(cls, key):
        return cls.__getreg__(key)

    def __getitem__(cls, key):
        return cls.__getreg__(key)

    @property
    def regs(cls):
        return cls.__REGS__.copy()

    @property
    def mask_keys(cls):
        return list(cls.__MASKS__.keys())

    @property
    def reg_keys(cls):
        return list(cls.__REGS__.keys())

    def reg(cls, *mask_keys):
        """
        注册方法
        """

        act_alias = set(mask_keys)

        def _reg_act(obj):
            # obj是一个function或者class
            reg_key = obj.__name__
            act_alias.add(reg_key)

            for name in act_alias:
                if name not in cls.__MASKS__:
                    cls.__MASKS__[name] = reg_key
                else:
                    raise RegisterKeyDuplicate()

            cls.__REGS__[reg_key] = obj

            return obj

        return _reg_act


class task(metaclass=_RegMeta):
    def __init__(self):
        raise InstantiateNotAllowed(
            i18n("Task register is not allowed instantiating!"))


class model(metaclass=_RegMeta):
    def __init__(self):
        raise InstantiateNotAllowed(
            i18n("Model register is not allowed instantiating!"))


class Collector(metaclass=SingletonMeta):

    def __init__(self):
        self.module_pattern = re.compile(
            r"^(?!__)([a-zA-Z_][\w]*?)(?<!__)\.py$")

        self._internal_collect()

    def __call__(self, src_dir, desc):
        src_dir = Path(src_dir).resolve()
        logging.info(i18n("Loading {}. From: {}").format(desc, src_dir))
        self._collect(src_dir)

    def _internal_collect(self):
        standard_task_dir = Path(DLFLOW_TASKS)
        internal_model_dir = Path(DLFLOW_MODELS)

        logging.info(
            i18n("Loading DLFlow standard task. From: {}")
            .format(standard_task_dir))
        self._collect(standard_task_dir)

        logging.info(
            i18n("Loading DLFlow internal model. From: {}")
            .format(internal_model_dir))
        self._collect(internal_model_dir)

    def _collect(self, src_dir):
        """
        _collect()目的在于import具体的任务和模型。

        需要注册的任务和模型需要被特定的装饰器装饰，注册过程中装饰器首先被触发，
        将类或者函数注册到对应的容器内。
        """

        src_dir = Path(src_dir).resolve()

        for cur, _, files in os.walk(src_dir):
            logging.debug(i18n("Scanning directory: {}").format(cur))

            if files:
                sys.path.append(cur)

                for file_name in files:
                    for module_name in self.module_pattern.findall(file_name):
                        _ = __import__(module_name)
                        logging.info(
                            i18n("  * Loading module: {}").format(module_name))

                sys.path.remove(cur)
