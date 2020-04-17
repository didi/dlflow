from pathlib import Path


_ROOT = Path(__file__).parent.parent.resolve()
DLFLOW_ROOT = _ROOT.as_posix()
DLFLOW_TASKS = _ROOT.joinpath("tasks", 'internal').as_posix()
DLFLOW_MODELS = _ROOT.joinpath("models", "internal").as_posix()
DLFLOW_RESOURCES = _ROOT.joinpath("resources").as_posix()
DLFLOW_LOCALE = _ROOT.joinpath("resources", "locale").as_posix()
DLFLOW_LIB = _ROOT.joinpath("lib").as_posix()


__all__ = [
    "DLFLOW_ROOT",
    "DLFLOW_TASKS",
    "DLFLOW_MODELS",
    "DLFLOW_RESOURCES",
    "DLFLOW_LOCALE",
    "DLFLOW_LIB"
]
