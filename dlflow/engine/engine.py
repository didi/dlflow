from dlflow.engine.workflow import WorkflowRunner
from dlflow.mgr import config, Collector
from dlflow.utils.logger import set_logging_level, set_logging_writer
from dlflow.utils.logger import get_logging_info_str
from dlflow.utils.locale import i18n

from absl import logging
from pathlib import Path
import abc
import os


class BaseEngine(metaclass=abc.ABCMeta):

    def __init__(self):
        pass

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        pass


class LocalEngine(BaseEngine):

    def __init__(self):
        super(LocalEngine, self).__init__()

    def run(self, steps):
        with WorkflowRunner(steps) as runner:
            runner.run_task()


ENGINES = {
    "local": LocalEngine,
}


class Engine(object):

    def __init__(self):
        self._engine = None

        self._steps = None
        self._lang = None
        self._log_level = None
        self._log_dir = None
        self._mode = None

        config.setting(
            config.sys("STEPS"),
        )("_SYS")

    @property
    def engine(self):
        return self._engine

    def initialize(self,
                   file=None,
                   udc=None,
                   log_level=None,
                   log_dir=None,
                   lang=None,
                   mode="local"):

        config.load(file, udc)

        if lang:
            self._lang = lang
        elif "LANG" in os.environ:
            self._lang = os.environ["LANG"].split(".")[0].split("_")[0]

        i18n.initialize(self._lang)

        if log_level:
            self._log_level = log_level
        elif "LOG_LEVEL" in config:
            self._log_level = config.LOG_LEVEL

        if self._log_level:
            set_logging_level(self._log_level)
            logging_info = get_logging_info_str()
            logging.warning(i18n("Logging level is changed to '{}'")
                            .format(self._log_level)
                            .join("", logging_info))

        if log_dir:
            self._log_dir = log_dir
        elif "LOG_DIR" in config:
            self._log_dir = config.LOG_DIR

        if self._log_dir:
            log_name = Path(file).parts[-1].split(".")[0]
            set_logging_writer(log_name, self._log_dir)
            logging.info(i18n("Start writing log to {}").format(self._log_dir))

        collect = Collector()
        for key, desc in [("TASKS_DIR", "UDT"),
                          ("MODELS_DIR", "Models")]:
            if key in config:
                collect(config[key], desc)

        self._steps = config.STEPS
        self._mode = mode.lower()

    def run(self):
        engine_cls = ENGINES[self._mode]
        engine = engine_cls()

        if isinstance(engine, BaseEngine):
            engine.run(self._steps)

        logging.info(i18n("All task done."))
