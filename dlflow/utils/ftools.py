from dlflow.mgr.errors import FileTypeNotSupport
from dlflow.utils.locale import i18n

import os
import json
import pickle
import yaml
import zipfile
import shutil
import abc


def zipdir(srcpath, dstpath=None, pmid=None):
    srcpath_abs = os.path.abspath(srcpath)
    if not os.path.exists(srcpath_abs):
        return None

    _l = len(srcpath_abs.split(os.sep))
    len_src = _l - 1 if _l >= 1 else 0

    fname = srcpath_abs.split(os.sep)[-1]
    if pmid is not None:
        fname += "_{}".format(pmid)

    fname_zip = ".".join([fname, "zip"])

    if dstpath is None:
        save_path = os.path.abspath(
            os.path.join(srcpath_abs, "..", fname_zip))
    else:
        dstpath_abs = os.path.abspath(dstpath)
        if not os.path.isdir(dstpath_abs):
            os.makedirs(dstpath_abs)
        save_path = os.path.join(dstpath_abs, fname_zip)

    zp = zipfile.ZipFile(save_path, "w", zipfile.ZIP_DEFLATED)
    for curr_dir, _, filenames in os.walk(srcpath_abs):
        prefix_dirs = curr_dir.split(os.sep)[len_src:]
        if prefix_dirs[0] != fname:
            prefix_dirs[0] = fname
        for filename in filenames:
            rlt_path = os.path.join(*prefix_dirs, filename)
            curr_file = os.path.join(curr_dir, filename)
            zp.write(curr_file, rlt_path)
    zp.close()

    return save_path


def remove(path):
    _path = os.path.abspath(path)
    if os.path.islink(_path):
        os.unlink(_path)
    elif os.path.isdir(_path):
        shutil.rmtree(_path)
    elif os.path.isfile(_path):
        os.remove(_path)
    else:
        raise RuntimeError(
            i18n("Can not delete file {}").format(_path))


def dict_format(d):
    return json.dumps(d, indent=4, sort_keys=True)


class _FBaseTool(metaclass=abc.ABCMeta):
    __tools__ = dict()

    @classmethod
    @abc.abstractmethod
    def load(cls, fpath):
        pass

    @classmethod
    @abc.abstractmethod
    def dump(cls, obj, fpath):
        pass

    @classmethod
    def tools_collector(cls, *alias):
        names = set(alias)

        def _inner(tool):
            for name in names:
                cls.__tools__[name] = tool

        return _inner

    @classmethod
    def get_tools(cls):
        return cls.__tools__


@_FBaseTool.tools_collector("txt", "text")
class _TEXT(_FBaseTool):

    @classmethod
    def load(cls, fpath):
        with open(fpath, "r") as f:
            context = f.read()
        return context

    @classmethod
    def dump(cls, obj, fpath):
        if not isinstance(obj, str):
            obj = str(obj)

        with open(fpath, "w+") as w:
            w.write(obj)


@_FBaseTool.tools_collector("json")
class _JSON(_FBaseTool):

    @classmethod
    def load(cls, fpath):
        with open(fpath, "r") as f:
            context = json.load(f)
        return context

    @classmethod
    def dump(cls, obj, fpath):
        with open(fpath, "w+") as w:
            json.dump(obj, w)


@_FBaseTool.tools_collector("pickle", "pkl", "meta")
class _PICKLE(_FBaseTool):

    @classmethod
    def load(cls, fpath):
        with open(fpath, "rb") as f:
            context = pickle.load(f)
        return context

    @classmethod
    def dump(cls, obj, fpath):
        with open(fpath, "wb") as w:
            pickle.dump(obj, w)


@_FBaseTool.tools_collector("yaml")
class _YAML(_FBaseTool):

    @classmethod
    def load(cls, fpath):
        with open(fpath, "r") as f:
            context = yaml.load(f, Loader=yaml.SafeLoader)
        return context

    @classmethod
    def dump(cls, obj, fpath):
        with open(fpath, "w+") as w:
            yaml.dump(obj, w)


@_FBaseTool.tools_collector("conf", "hocon")
class _CONF(_FBaseTool):

    @classmethod
    def load(cls, fpath):
        raise NotImplementedError()

    @classmethod
    def dump(cls, obj, fpath):
        raise NotImplementedError()


class FTools(object):
    _type2cls = _FBaseTool.get_tools()

    @classmethod
    def load(cls, fpath, ftype=None):
        ftype = ftype if isinstance(ftype, str) else fpath.suffix.strip(".")
        loader = cls._type2cls.get(ftype, None)

        if loader is None:
            raise FileTypeNotSupport(
                i18n("Can't recognize file type '{}' for given file {}")
                .format(ftype, fpath))
        return loader.load(fpath)

    @classmethod
    def dump(cls, obj, fpath, ftype=None):
        ftype = ftype if isinstance(ftype, str) else fpath.suffix.strip(".")
        ftype = ftype.lower()
        dumper = cls._type2cls.get(ftype, None)

        if dumper is None:
            raise FileTypeNotSupport(
                i18n("Can't recognize file type '{}' for given file {}")
                .format(ftype, fpath))
        dumper.dump(obj, fpath)
