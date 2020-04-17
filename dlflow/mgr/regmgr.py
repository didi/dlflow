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

    def __init__(cls, *args, **kwargs):
        super(_RegMeta, cls).__init__(*args, **kwargs)
        cls.__MASKS__ = dict()
        cls.__REGS__ = dict()

    def __getreg__(cls, key):
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
        act_alias = set(mask_keys)

        def _reg_act(obj):
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
