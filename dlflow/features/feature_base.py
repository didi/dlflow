from dlflow.utils.ftools import FTools
from dlflow.utils.locale import i18n

from collections import OrderedDict
from hashlib import md5
from pathlib import Path
import abc


DEFAULT_NAME = "default"
PKEY_NAME = "primary_keys"
LABEL_NAME = "labels"
PRESET_BUCKETS = [PKEY_NAME, LABEL_NAME]


class _FeContainer(object):

    def __init__(self, name, contype):
        self._name = name
        self._type = contype
        self._container = OrderedDict()

        _md5 = md5(name.encode("utf-8")).digest()[::4]
        self._pmcs = int.from_bytes(_md5, "little")
        self._mcs = self._pmcs

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def checksum(self):
        return int.to_bytes(self._mcs, 4, "little").hex()

    @property
    def mcs_dec(self):
        return self._mcs

    def __getstate__(self):
        return self._name, self._type, self._container, self._pmcs, self._mcs

    def __setstate__(self, state):
        self._name, self._type, self._container, self._pmcs, self._mcs = state

    def __getattr__(self, key):
        if key not in self._container:
            raise KeyError(i18n("Unknown key '{}'").format(key))

        return self._container[key]

    def __getitem__(self, key):
        item = self
        for k in key.split("."):
            item = getattr(item, k)

        return item

    def __iter__(self):
        return iter(self._container.items())

    def to_dict(self):
        d = OrderedDict()
        for name, value in self._container.items():
            d[name] = value.to_dict()

        return d

    def _update_mcs(self, mcs_dec):
        return self._mcs ^ mcs_dec


class Feature(_FeContainer):

    def __init__(self,
                 name=None,
                 fetype=None,
                 size=None,
                 shape=None,
                 offset=None,
                 meta=None,
                 flag=None):

        super(Feature, self).__init__(name, "feature")

        self._container["fetype"] = fetype
        self._container["size"] = size
        self._container["shape"] = shape
        self._container["offset"] = offset
        self._container["meta"] = meta
        self._container["flag"] = flag

        self._field = None

        _property_mcs = self._property_mcs()
        self._mcs = self._update_mcs(_property_mcs)

    @property
    def field(self):
        return self._field

    @field.setter
    def field(self, field):
        if isinstance(field, FeField):
            self._field = field
        else:
            raise TypeError(
                i18n("Parameter type expected {}, but got {}")
                .format(FeField, type(field)))

    def _property_mcs(self):
        s = self.name
        for key, value in self._container.items():
            s += "".join([str(key), str(value)])

        _md5 = md5(s.encode("utf-8")).digest()[::4]

        return int.from_bytes(_md5, "little")

    def to_dict(self):
        return self._container

    def set_offset(self, offset):
        if offset >= 0:
            self._container["offset"] = offset
        else:
            raise ValueError(i18n("Value of 'offset' must >= 0"))


class FeField(_FeContainer):

    def __init__(self, name):
        super(FeField, self).__init__(name, "field")
        self._bucket = None
        self._offset = 0

    @property
    def fe_count(self):
        return len(self._container)

    @property
    def fe_size(self):
        fe_list = self.get_features()
        if fe_list:
            fe = fe_list[-1]
            size = fe.offset + fe.size
        else:
            size = 0

        return size

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, bucket):
        if isinstance(bucket, FeBucket):
            self._bucket = bucket
        else:
            raise TypeError(
                i18n("Parameter type expected {}, but got {}")
                .format(FeBucket, type(bucket)))

    def add_feature(self, feature: Feature):
        if isinstance(feature, Feature):
            feature.field = self
            feature.set_offset(self._offset)

            self._offset += feature.size
            self._container[feature.name] = feature
            self._mcs = self._update_mcs(feature.mcs_dec)
        else:
            raise TypeError(
                i18n("Parameter type expected {}, but got {}")
                .format(Feature, type(feature)))

    def get_features(self):
        return list(self._container.values())


class FeBucket(_FeContainer):

    def __init__(self, name):
        super(FeBucket, self).__init__(name, "bucket")
        self.create_field("nums")
        self.create_field("ctgs")

    def create_field(self, field_name):
        field = FeField(field_name)
        field.bucket = self
        self._container[field_name] = field

    def add_feature(self, feature: Feature):

        if feature.fetype == "FeStr":
            _field = self._container["ctgs"]
        else:
            _field = self._container["nums"]

        _field.add_feature(feature)
        self._mcs = self._update_mcs(_field.mcs_dec)

    def get_fields(self):
        return list(self._container.values())

    def get_features(self):
        fe_list = []
        for _field in self._container.values():
            fe_list.extend(_field.get_features())

        return fe_list


class Fmap(_FeContainer):
    def __init__(self):
        super(Fmap, self).__init__("fmap", "fmap")
        self._fitted = False

    def add_bucket(self, field_name):
        self._container[field_name] = FeBucket(field_name)

    def add_feature(self, feature: Feature, bucket=None):
        _bucket = bucket
        if bucket is None:
            _bucket = DEFAULT_NAME

        if _bucket not in self._container:
            self.add_bucket(_bucket)

        _bucket = self._container[_bucket]
        _bucket.add_feature(feature)
        self._mcs = self._update_mcs(_bucket.mcs_dec)

    def get_buckets(self, drop=None):
        buckets = list(self._container.values())

        if isinstance(drop, str):
            drop = [drop]

        if isinstance(drop, (tuple, list, set)):
            for name in drop:
                if name in self._container:
                    buckets.remove(self._container[name])

        return buckets

    def get_features(self, bucket=None):
        fe_list = []

        if bucket is None:
            for _bucket in self._container.values():
                fe_list.extend(_bucket.get_features())
        else:
            fe_list.extend(self._container[bucket].get_features())

        return fe_list

    def save(self, save_dir: Path):
        save_dir = Path(save_dir)
        if not save_dir.is_dir():
            save_dir.mkdir(parents=True)

        meta_file = save_dir.joinpath("fmap.meta")
        FTools.dump(self, meta_file)

        json_file = save_dir.joinpath("fmap.json")
        FTools.dump(self.to_dict(), json_file)

    @staticmethod
    def load(load_dir: Path):
        load_dir = Path(load_dir)

        meta_file = load_dir.joinpath("fmap.meta")

        return FTools.load(meta_file)


class FeType(metaclass=abc.ABCMeta):
    __fetype__ = "FeType"

    def __init__(self, name):
        self._name = name
        self._fe = None

    @property
    def name(self):
        return self._name

    @property
    def fe(self):
        return self._fe

    @property
    def fetype(self):
        return self.__fetype__

    @abc.abstractmethod
    def parser(self, *args, **kwargs) -> Feature:
        pass

    @abc.abstractmethod
    def transform(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def schema(self):
        pass

    def bind_feature(self, fe: Feature):
        if isinstance(fe, Feature):
            self._fe = fe


class FeInt(FeType):
    __fetype__ = "FeInt"

    def __init__(self, name):
        super(FeInt, self).__init__(name)


class FeFloat(FeType):
    __fetype__ = "FeFloat"

    def __init__(self, name):
        super(FeFloat, self).__init__(name)


class FeStr(FeType):
    __fetype__ = "FeStr"

    def __init__(self, name):
        super(FeStr, self).__init__(name)


class FeMap(FeType):
    __fetype__ = "FeMap"

    def __init__(self, name):
        super(FeMap, self).__init__(name)


class FeArr(FeType):
    __fetype__ = "FeArr"

    def __init__(self, name):
        super(FeArr, self).__init__(name)


class FeParser(metaclass=abc.ABCMeta):
    def __init__(self, *args, **kwargs):
        self._fmap = None

    @property
    def fmap(self):
        return self._fmap

    @abc.abstractmethod
    def fit(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def transform(self, *args, **kwargs):
        pass

    def save(self, fmap_dir):
        self._fmap.save(fmap_dir)

    def load(self, fmap_dir):
        fmap = Fmap.load(fmap_dir)
        if isinstance(fmap, Fmap):
            self._fmap = fmap
        else:
            raise TypeError(
                i18n("Parameter type expected {}, but got {}")
                .format(Fmap, type(fmap)))

    def reset(self):
        self._fmap = None


class FeNormalizer(metaclass=abc.ABCMeta):
    def __init__(self, *args, **kwargs):
        self._normalizer = None

    @abc.abstractmethod
    def fit(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def transform(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def save(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def load(self, *args, **kwargs):
        pass
