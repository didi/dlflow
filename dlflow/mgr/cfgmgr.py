from dlflow.utils.locale import i18n
from dlflow.utils.template import ConfigTemplate
from dlflow.mgr.errors import NotInitializeError
from dlflow.utils.utilclass import SingletonMeta

from pyhocon import ConfigFactory, ConfigTree
from random import sample
from uuid import uuid4
from ast import literal_eval
from absl import logging
import functools
import os
import re


LEVEL_SYSTEM = 0
LEVEL_REQUIRE = 1
LEVEL_OPTION = 2

LEVEL_SYSTEM_STR = "system"
LEVEL_REQUIRE_STR = "require"
LEVEL_OPTION_STR = "option"

LEVEL_MAPPING_C2S = {
    LEVEL_SYSTEM: LEVEL_SYSTEM_STR,
    LEVEL_REQUIRE: LEVEL_REQUIRE_STR,
    LEVEL_OPTION: LEVEL_OPTION_STR
}


def _to_dict(config_tree: ConfigTree):
    def _parser(obj):
        if isinstance(obj, (ConfigTree, dict)):
            value = {}
            for k, v in obj.items():
                value[k] = _parser(v)

        else:
            value = obj

        return value

    return _parser(config_tree)


def _to_dense_dict(config_tree: ConfigTree):
    dense_dict = {}

    def _parser(obj, prefix=None):
        if isinstance(obj, (ConfigTree, dict)):
            for k, v in obj.items():
                _k = k if prefix is None else config.sep.join([prefix, k])
                _parser(v, prefix=_k)

        else:
            dense_dict[prefix] = obj

    _parser(config_tree)
    return dense_dict


def _replace(config_tree: ConfigTree, obj):
    if isinstance(obj, (ConfigTree,)):
        dense_dict = _to_dense_dict(obj)

    elif isinstance(obj, (dict,)):
        dense_dict = obj

    else:
        raise ValueError()

    res = _to_dense_dict(config_tree)
    for key, value in dense_dict.items():
        res[key] = value

    return ConfigFactory.from_dict(res)


def _to_str(config_tree: ConfigTree, indent=4):
    def _s(d, n):
        res = ""
        keys = list(d.keys())
        keys.sort()

        for k in keys:
            v = d[k]

            if isinstance(v, (dict,)):
                res += "{}{} :\n".format(" " * n * indent, k)
                res += _s(v, n + 1)
            else:
                res += "{}{} : {}\n".format(" " * n * indent, k, v)
        return res

    return "".join(["{\n", _s(_to_dict(config_tree), 1), "}"])


ConfigTree.dict = property(_to_dict)
ConfigTree.dense_dict = property(_to_dense_dict)
ConfigTree.replace = _replace
ConfigTree.__str__ = _to_str


class _ConfigLoader(ConfigFactory):

    def __init__(self):
        super(_ConfigLoader, self).__init__()

        self._parse = {
            "file": self.parse_file,
            "str": self.parse_string,
            "url": self.parse_URL,
            "dict": self.from_dict
        }

    def load(self, f, ftype, **kwargs):
        return self._parse[ftype.lower()](f, **kwargs)


class _ConfigManager(object):
    def __init__(self):
        self._conf = {}
        self._desc = {}

    def __getitem__(self, item):
        return getattr(self, item)

    def __setstate__(self, state):
        self.__dict__ = state

    def __getstate__(self):
        return self.__dict__

    def set_conf(self, name, default, level, desc, field):
        if not hasattr(self, field):
            setattr(self, field, [])
        getattr(self, field).append(name)

        if not hasattr(self, level):
            setattr(self, level, [])
        getattr(self, level).append(name)

        self._conf[name] = default
        self._desc[name] = desc

    def get_conf(self):
        return self._conf


class _ConfigMeta(metaclass=SingletonMeta):

    _UDC_PATTERN = re.compile(r"^([\w][\w.]*?(?<!\.))=(.*)$")

    _cfg_mgr = _ConfigManager()
    _cfg_loader = _ConfigLoader()
    _uuid = "".join(sample(uuid4().hex, 8))
    _conf = None

    def __getattr__(self, key):
        return self.__getconf__(key)

    def __getitem__(self, key):
        return self.__getconf__(key)

    def __iter__(self):
        if self._conf is None:
            raise NotInitializeError(i18n("ConfigTree is not set!"))

        def _generator():
            for k, v in self._conf.dense_dict.items():
                yield (k, v)

        return _generator()

    def __contains__(self, key):
        if self._conf is None:
            raise NotInitializeError(i18n("ConfigTree is not set!"))

        return key in self._conf

    def __setstate__(self, state):
        _ConfigMeta._uuid, _ConfigMeta._conf, _ConfigMeta._cfg_mgr = state

    def __getstate__(self):
        return self._uuid, self._conf, self._cfg_mgr

    @property
    def conf(self):
        return self._conf

    @property
    def uuid(self):
        return self._uuid

    @property
    def sep(self):
        return "."

    def __getconf__(self, key):
        if self._conf is None:
            raise NotInitializeError(i18n("ConfigTree is not set!"))

        if key not in self._conf:
            raise KeyError(key)

        return self._conf[key]

    def get_field(self, item):
        return self._cfg_mgr[item]

    def get_level(self, level):
        return self._cfg_mgr[level]

    def sys(self, name, desc=None):
        return functools.partial(self._cfg_mgr.set_conf,
                                 name,
                                 None,
                                 LEVEL_SYSTEM_STR,
                                 desc)

    def req(self, name, default=None, desc=None):
        return functools.partial(self._cfg_mgr.set_conf,
                                 name,
                                 default,
                                 LEVEL_REQUIRE_STR,
                                 desc)

    def opt(self, name, default=None, desc=None):
        return functools.partial(self._cfg_mgr.set_conf,
                                 name,
                                 default,
                                 LEVEL_OPTION_STR,
                                 desc)

    def load_file(self, f):
        return self._cfg_loader.load(f, "file")

    def load_udc(self, udc):
        udc_conf = {}
        for param in udc:
            for p in param.split(";"):
                for pk, pv in self._UDC_PATTERN.findall(p):
                    _pv = self.infer_type(pv)
                    udc_conf[pk] = _pv

        return self._cfg_loader.load(udc_conf, "dict")

    def load_regs(self):
        reg_conf = self._cfg_mgr.get_conf()

        return self._cfg_loader.load(reg_conf, "dict")

    @staticmethod
    def infer_type(value):
        try:
            res = literal_eval(value)
        except (ValueError, SyntaxError):
            res = value

        return res

    @staticmethod
    def setting(*reg_funcs):
        def _setting(field):
            for conf_func in reg_funcs:
                conf_func(field)

        return _setting


class _Config(_ConfigMeta):

    def __init__(self):
        super(_Config, self).__init__()

    def load(self, file=None, udc=None):
        outer_conf = ConfigTree()

        _ = self.load_regs()

        if file:
            file_conf = self.load_file(file)
            outer_conf = outer_conf.replace(file_conf)

        if udc:
            udc_conf = self.load_udc(udc)
            outer_conf = outer_conf.replace(udc_conf)

        if hasattr(self._cfg_mgr, LEVEL_SYSTEM_STR):
            for sys_key in self._cfg_mgr[LEVEL_SYSTEM_STR]:
                if sys_key not in outer_conf:
                    raise KeyError(
                        i18n("Key '{}' isn't found in the configurations!")
                        .format(sys_key))

                setattr(self, sys_key, outer_conf[sys_key])

        if hasattr(self._cfg_mgr, LEVEL_REQUIRE_STR):
            for req_key in self._cfg_mgr[LEVEL_REQUIRE_STR]:
                if req_key in outer_conf:
                    setattr(self, req_key, outer_conf[req_key])
                else:
                    logging.warning(
                        i18n("Level-REQUIRE key '{}' has been set, "
                             "but not provide in configurations.")
                        .format(req_key))

        self._conf = outer_conf

    def initialize(self):
        outer_conf = self._conf.copy()

        default_conf = self.load_regs()

        self._conf = default_conf.replace(self._conf)

        self._solver()
        logging.info(
            i18n("All parsed configurations:\n{}").format(self._conf))

        if hasattr(self._cfg_mgr, LEVEL_REQUIRE_STR):
            for req_key in self._cfg_mgr[LEVEL_REQUIRE_STR]:
                if req_key not in outer_conf:
                    logging.warning(
                        i18n("Level-REQUIRE key '{}' has been set, "
                             "but not provide in configurations.")
                        .format(req_key))

    def _solver(self):
        if self._conf is None:
            raise ValueError()

        dense_dict = self._conf.dense_dict
        total_key_set = set([i for i in dense_dict.keys()])

        G = {}
        for k in total_key_set:
            G[k] = {"tail": [], "tpl": None, "in": 0}

        for cur_key, cur_value in dense_dict.items():
            if isinstance(cur_value, (str,)):
                conf_template = ConfigTemplate(cur_value)
                G[cur_key]["tpl"] = conf_template
                G[cur_key]["in"] = len(conf_template.vars)
                for head_key in conf_template.vars:
                    G[head_key]["tail"].append(cur_key)

        sort_keys = []
        while total_key_set:
            _kick_set = set()

            for key in total_key_set:
                _ref_key = G[key]
                if _ref_key["in"] == 0:
                    for tail_key in _ref_key["tail"]:
                        G[tail_key]["in"] -= 1
                    sort_keys.append(key)
                    _kick_set.add(key)

            if not _kick_set:
                err_info = i18n(
                    "Circular references are found in the "
                    "following keys, please check it!\n{}") \
                    .format(total_key_set)
                logging.error(err_info)
                raise RuntimeError(err_info)

            total_key_set -= _kick_set

        for key in sort_keys:
            if G[key]["tpl"]:
                conf_template = G[key]["tpl"]

                kws = {}
                for _k in conf_template.vars:
                    kws[_k] = dense_dict[_k]

                dense_dict[key] = conf_template.render(kws)

        self._conf = self._cfg_loader.load(dense_dict, "dict")


config = _Config()

config.setting(
    config.opt("ROOT", os.getcwd())
)("_SYS")
