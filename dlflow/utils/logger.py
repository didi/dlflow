import os
import logging
import absl.logging as absl_logging


_ABSL_LOGGING_LEVEL = {
    "debug": absl_logging.DEBUG,
    "info": absl_logging.INFO,
    "warn": absl_logging.WARNING,
    "error": absl_logging.ERROR
}

DEFAULT_INFO_FMT = "%(asctime)s [%(levelname)s] - %(message)s"
DEFAULT_DEBUG_FMT = "%(asctime)s %(filename)s:%(lineno)d " \
                    "[%(levelname)s] - %(message)s"


def logging_initialize(log_level=None, fmt_str=None):
    absl_handler = absl_logging.get_absl_handler()

    if log_level is None:
        log_level = "info"

    if fmt_str is None:
        fmt_str = DEFAULT_DEBUG_FMT

    formatter = logging.Formatter(fmt_str)
    absl_handler.setFormatter(formatter)

    set_logging_level(log_level)


def set_logging_level(log_level):
    if log_level.lower() not in _ABSL_LOGGING_LEVEL:
        raise ValueError("Logging initialize error. Can not recognize value of"
                         " <log_level> which by given '{}' , except 'debug',"
                         " 'info', 'warn', 'error'.".format(log_level))
    absl_handler = absl_logging.get_absl_handler()

    if absl_handler in logging.root.handlers:
        logging.root.removeHandler(absl_handler)

    absl_logging.set_verbosity(_ABSL_LOGGING_LEVEL[log_level])
    absl_logging.set_stderrthreshold(_ABSL_LOGGING_LEVEL[log_level])

    absl_logging._warn_preinit_stderr = False
    logging.root.addHandler(absl_handler)


def set_logging_writer(log_name, log_dir):
    absl_handler = absl_logging.get_absl_handler()

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    absl_handler.use_absl_log_file(program_name=log_name, log_dir=log_dir)


def get_logging_info_str():
    info = ["\n=== === LOGGING INFO === ==="]
    for _name, _logger in logging.Logger.manager.loggerDict.items():
        if hasattr(_logger, "handlers"):
            _handler = _logger.handlers
        else:
            _handler = "None"
        _str = " * {} : {} - {}".format(_name, _logger, _handler)
        info.append(_str)
    info.append("=== ===  LOGGING INFO  === ===\n")
    return "\n".join(info)
