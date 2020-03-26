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


# 参数级别定义
LEVEL_SYSTEM = 0  # 框架级别重要参数，缺少将导致DLFlow无法运行，禁用模板
LEVEL_REQUIRE = 1  # 一般重要参数，是否允许模板呢？
LEVEL_OPTION = 2  # 常规参数，允许使用模板

LEVEL_SYSTEM_STR = "system"
LEVEL_REQUIRE_STR = "require"
LEVEL_OPTION_STR = "option"

LEVEL_MAPPING_C2S = {
    LEVEL_SYSTEM: LEVEL_SYSTEM_STR,
    LEVEL_REQUIRE: LEVEL_REQUIRE_STR,
    LEVEL_OPTION: LEVEL_OPTION_STR
}


def _to_dict(config_tree: ConfigTree):
    """
    ConfigTree类的外部扩展方法
    """

    def _parser(obj):
        if isinstance(obj, (ConfigTree, dict)):
            value = {}
            for k, v in obj.items():
                value[k] = _parser(v)

        # 忽略迭代式的参数，如：[{"key1": value1}, {"key2":value2}]
        # elif isinstance(obj, (list, tuple)):
        #     value = []
        #     for item in obj:
        #         value.append(_parser(item))

        else:
            value = obj

        return value

    return _parser(config_tree)


def _to_dense_dict(config_tree: ConfigTree):
    """
    ConfigTree类的外部扩展方法
    """

    dense_dict = {}

    def _parser(obj, prefix=None):
        if isinstance(obj, (ConfigTree, dict)):
            for k, v in obj.items():
                _k = k if prefix is None else config.sep.join([prefix, k])
                _parser(v, prefix=_k)

        # 忽略迭代式的参数，如：[{"key1": value1}, {"key2":value2}]
        # elif isinstance(obj, (list, tuple)):
        #     for item in obj:
        #         _parser(item, prefix=prefix)

        else:
            dense_dict[prefix] = obj

    _parser(config_tree)
    return dense_dict


def _replace(config_tree: ConfigTree, obj):
    """
    ConfigTree类的外部扩展方法
    """

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
    """
    用于替换ConfigTree类默认的__str__方法，进行格式化输出。
    日志输出是会惰性调用该方法。
    """

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


# 扩展ConfigTree的方法
ConfigTree.dict = property(_to_dict)
ConfigTree.dense_dict = property(_to_dense_dict)
ConfigTree.replace = _replace
ConfigTree.__str__ = _to_str


class _ConfigLoader(ConfigFactory):
    """
    配置加载类。

    扩展自ConfigFactory，封装多种类型的配置加载方法。
    """

    def __init__(self):
        super(_ConfigLoader, self).__init__()

        self._parse = {
            "file": self.parse_file,
            "str": self.parse_string,
            "url": self.parse_URL,
            "dict": self.from_dict
        }

    def load(self, f, ftype, **kwargs):
        """
        加载配置，返回ConfigTree。

        :param f: 配置源，可以是字符串或者字典。
                  字符串可以是文件路径，URL，hocon字符串。
        :param ftype: 指定配置源，支持四种值 'file'，'str', 'url', 'dict'。
        :param kwargs: 为了支持ConfigFactory原生方法预留的参数。
                       一般不用，用的话可以参考ConfigFactor中相应的方法。
        :return: ConfigTree
        """

        return self._parse[ftype.lower()](f, **kwargs)


class _ConfigManager(object):
    """
    配置管理。

    所有配置信息都会记录在这里。
    包括：
        - 配置项的级别信息。配置项名称放到了一个级别相同名称的类属性中。
        - 配置项的定义位置信息。配置项名称放到了和定义类相同名称的类属性中。
        - 定义配置项的 Key-DefaultValue。
        - 定义配置项的 Key-Describe。
    """

    def __init__(self):
        self._conf = {}
        self._desc = {}

    def __getitem__(self, item):
        """
        获取一个field或者一个level下全部的配置key
        如果key不存在，则返回一个空列表
        """

        return getattr(self, item)

    def __setstate__(self, state):
        self.__dict__ = state

    def __getstate__(self):
        return self.__dict__

    def set_conf(self, name, default, level, desc, field):
        """
        实际设置参数的方法。
        :param name: 参数名。
        :param default: 默认值。
        :param level: 参数等级。
        :param desc: 参数描述。
        :param field: 参数所属的域，即参数被定义的位置（所在类的名字）。

        该方法不推荐直接使用，除非很明确需要做什么。
        设置参数时推荐使用 _ConfigMeta中包装成偏函数的方法（sys, req, opt)。
        """

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
    """
    MetaClass，创造配置类的类。

    - 使用 _ConfigManager对参数进行管理。
    - 使用 _ConfigLoader做参数加载。
    - 使用 _ConfigTree作为内部配置项的载体。
    - 生成uuid，作为全局唯一标识。
    """

    _cfg_mgr = _ConfigManager()
    _cfg_loader = _ConfigLoader()
    _uuid = "".join(sample(uuid4().hex, 8))
    _conf = None

    def __getattr__(self, key):
        return self.__getconf__(key)

    def __getitem__(self, key):
        return self.__getconf__(key)

    def __iter__(self):
        """
        生成config的可迭代对象。

        迭代返回(dense_key, value)形式的tuple对象。
        """

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
        """
        获取一个具体的参数。

        被__getattr__和__getitem__调用的方法，让该类产生的类实例可以像ConfigTree一样
        灵活的调用设置的配置项。
        """

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
        """
        System 级别的配置项
        """

        return functools.partial(self._cfg_mgr.set_conf,
                                 name,
                                 None,
                                 LEVEL_SYSTEM_STR,
                                 desc)

    def req(self, name, default=None, desc=None):
        """
        Require 级别的配置项
        """

        return functools.partial(self._cfg_mgr.set_conf,
                                 name,
                                 default,
                                 LEVEL_REQUIRE_STR,
                                 desc)

    def opt(self, name, default=None, desc=None):
        """
        Option 级别的配置项
        """

        return functools.partial(self._cfg_mgr.set_conf,
                                 name,
                                 default,
                                 LEVEL_OPTION_STR,
                                 desc)

    def load_file(self, f):
        """
        加载文件配置项。
        """

        return self._cfg_loader.load(f, "file")

    def load_udc(self, udc):
        """
        加载运行时配置项。
        """

        udc_conf = {}
        for param in udc:
            for p in param.split(";"):
                pk, pv = [i.strip() for i in p.split("=")]
                _pv = self.infer_type(pv)
                udc_conf[pk] = _pv

        return self._cfg_loader.load(udc_conf, "dict")

    def load_regs(self):
        """
        加载定义的配置项。
        """

        reg_conf = self._cfg_mgr.get_conf()

        return self._cfg_loader.load(reg_conf, "dict")

    # def load_meta(self, meta: ):
    #     state = meta

    @staticmethod
    def infer_type(value):
        """
        利用ast动态执行推断参数类型。

        value只能接受简单表达式，支持多种内置数据类型。
        value 是字符串，如果解析成功，则返回相应的值，如果无法解析则返回原字符串。
        """

        try:
            res = literal_eval(value)
        except ValueError:
            res = value

        return res

    @staticmethod
    def setting(*reg_funcs):
        """
        用于配置模块默认的配置参数，在Task和Model相关的参数定义中使用。

        setting 本身接受任意数量的参数，参数为函数，更具体的说，
        该类为MetaClass，其后继的类可以直接使用参数定义方法(sys, req, opt)，
        设置方式如下：
        ```
            # config 为该类创建的类实例
            class config(metaclass=_ConfigMeta)
                ...

            config.setting(
                config.req("conf_1", "default_1"),
                config.opt("conf_2", "default_2")
            )
        ```
        """

        def _setting(field):
            for conf_func in reg_funcs:
                conf_func(field)

        return _setting


class _Config(_ConfigMeta):

    """
    直接对外暴露的配置类。

    config是_ConfigMeta的类实例，其本身并且禁止实例化，以保证全局唯一性。
    由于该类直接对外暴露，具体的参数加载和渲染过程均在该类中实现。

    使用时只需调用 config.load 加载基本配置即可。由于在构建Workflow后才会注册全部参数，
    因此初始化方法 config._]initialize 不要手动调用，启动WorkflowRunner时会自动初始化。
    """

    def __init__(self):
        super(_Config, self).__init__()

    def load(self, file=None, udc=None):
        """
        加载文件配置和UDC。

        此处只加载文件配置、UDC以及本文件中定义的'_SYS'域的注册配置。
        其余的注册配置会在config.initialize()调用时加载，
        load调用应在config.initialize()之前，以保证基础配置已加载。
        """

        outer_conf = ConfigTree()

        # 这里调用只是为了注册LEVEL_SYSTEM级别的配置项
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
        """
        该方法会被WorkflowRunner调用，实际调用顺序是在配置load和workflow建立之后。
        """

        outer_conf = self._conf.copy()

        # 加载Workflow中定义的配置项
        default_conf = self.load_regs()

        # 生成全部配置
        self._conf = default_conf.replace(self._conf)

        # 渲染并生成最终配置
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
        """
        配置处理，生成最终配置。

        包括两方面内容：
            - 按照配置项相互的引用次序进行排序
            - 对排序后的配置项依次填充配置中的模板

        当前只定义了两种类型的模板，一种是普通变量模板，另一种是日期模板，
        普通模板会把原本key对应的值以字符串形式替换进去，
        日期模板只接受'20200101'形式的字符串，会替换对应的yyyy，mm，dd。
            - 普通模板 {t[CONFIT_KEY]}
            - 日期模板 <dt[DATA_STRING]yyyymmdd>
                      <dt[DATA_STRING]yyyy/mm/dd>
                      <dt[DATA_STRING]dt=yyyy-mm-dd>
                      ...
        """

        if self._conf is None:
            raise ValueError()

        dense_dict = self._conf.dense_dict
        total_key_set = set([i for i in dense_dict.keys()])

        # Build Reference Graph (Very Simple DAG)
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

        # Sorting for DAG Node
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
                # 存在循环引用
                err_info = i18n(
                    "Circular references are found in the "
                    "following keys, please check it!\n{}") \
                    .format(total_key_set)
                logging.error(err_info)
                raise RuntimeError(err_info)

            total_key_set -= _kick_set

        # Render
        for key in sort_keys:
            if G[key]["tpl"]:
                conf_template = G[key]["tpl"]

                kws = {}
                for _k in conf_template.vars:
                    kws[_k] = dense_dict[_k]

                dense_dict[key] = conf_template.render(kws)

        # 最终配置
        self._conf = self._cfg_loader.load(dense_dict, "dict")


# System Level Configuration Setting
config = _Config()

config.setting(
    config.sys("STEPS"),
)("_SYS")
