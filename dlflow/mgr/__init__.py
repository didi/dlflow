from .regmgr import task, model
from .regmgr import Collector
from .cfgmgr import config


VERSION = "1.0.1"

MOTW = """
Welcome to
            _______  .___   _________.__
            \____  \ |   |  \_   ____/  |   ___.__  _  __
             |  |\  \|   |   |   __) |  |  /  _ \ \/ \/ /
             |  |/   \   |___|   \   |  |_(  (_) )     /
            /______  /______ \_  /   |____/\____/ \/\_/
                   \/       \/ \/           Version {version}

A Powerful Deep Learning Workflow!
""".format(version=VERSION)


__all__ = [
    "task",
    "model",
    "config",
    "Collector",
    "MOTW",
    "VERSION"
]
