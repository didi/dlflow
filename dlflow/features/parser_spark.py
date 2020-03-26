from dlflow.features.feature_base import *
from dlflow.utils.sparkapp import SparkBaseApp
from dlflow.mgr.errors import FeatureNotBind

from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import IntegerType, LongType
from pyspark.sql.types import FloatType, DoubleType, DecimalType
from pyspark.sql.types import StringType, MapType, ArrayType
from pyspark.ml.linalg import Vectors, VectorUDT

from pyspark.ml.feature import Normalizer
from pyspark.ml.feature import StandardScaler, StandardScalerModel
from pyspark.ml.feature import MinMaxScaler, MinMaxScalerModel

from collections import Counter
from pathlib import Path
from absl import logging
import json


class SparkFeInt(FeInt):

    def __init__(self, name):
        super(SparkFeInt, self).__init__(name)

    def parser(self, df, is_encode):
        return Feature(name=self.name,
                       fetype=self.fetype,
                       size=1,
                       flag=is_encode)

    def transform(self, value):
        if self.fe is None:
            raise FeatureNotBind()

        return SparkFeInt.static_transform(value,
                                           self.fe.size,
                                           self.fe.meta,
                                           self.fe.flag)

    def schema(self):
        if self.fe is None:
            raise FeatureNotBind()

        return SparkFeInt.static_schema(self.name, self.fe.flag)

    @staticmethod
    def static_schema(*args):
        name, is_encode = args
        _type = DoubleType() if is_encode else LongType()

        return StructField(name, _type)

    @staticmethod
    def static_transform(*args):
        value, _, _, is_encode = args

        _value = None
        if value is not None:
            _value = float(value) if is_encode else int(value)

        return _value


class SparkFeFloat(FeFloat):

    def __init__(self, name):
        super(SparkFeFloat, self).__init__(name)

    def parser(self, df, is_encode):
        return Feature(name=self.name,
                       fetype=self.fetype,
                       size=1,
                       flag=is_encode)

    def transform(self, value):
        if self.fe is None:
            raise FeatureNotBind()

        return SparkFeFloat.static_transform(value,
                                             self.fe.size,
                                             self.fe.meta,
                                             self.fe.flag)

    def schema(self):
        if self.fe is None:
            raise FeatureNotBind()

        return SparkFeFloat.static_schema(self.name, self.fe.flag)

    @staticmethod
    def static_schema(*args):
        name, _ = args

        return StructField(name, DoubleType())

    @staticmethod
    def static_transform(*args):
        value, _, _, _ = args

        _value = None
        if value is not None:
            _value = float(value)

        return _value


class SparkFeStr(FeStr):

    def __init__(self, name):
        super(SparkFeStr, self).__init__(name)

    def parser(self, df, is_encode):
        if is_encode:
            _name = self.name

            def map_func(row):
                if row[_name] is None:
                    res = "FeNull"
                else:
                    res = str(row[_name])
                return res

            counter = Counter(
                df.select(_name)
                  .rdd
                  .map(map_func)
                  .countByValue()
            )
            size = len(counter)

            if size > 1000:
                logging.warning(
                    i18n("Too many categories on column '{name}'!\n"
                         "Column '{name}' takes {size} categories!")
                    .format(name=_name, size=size)
                    .join("\n", "-" * 60))

            meta = {
                k: c for (k, _), c in zip(counter.most_common(), range(size))}

        else:
            size = 1
            meta = None

        return Feature(name=self.name,
                       fetype=self.fetype,
                       size=size,
                       meta=meta,
                       flag=is_encode)

    def transform(self, value):
        if self.fe is None:
            raise FeatureNotBind()

        return SparkFeStr.static_transform(value,
                                           self.fe.size,
                                           self.fe.meta,
                                           self.fe.flag)

    def schema(self):
        if self.fe is None:
            raise FeatureNotBind()

        return SparkFeStr.static_schema(self.name, self.fe.flag)

    @staticmethod
    def static_schema(*args):
        name, is_encode = args
        _type = LongType() if is_encode else StringType()

        return StructField(name, _type)

    @staticmethod
    def static_transform(*args):
        value, _, meta, is_encode = args

        _value = None
        if value is not None:
            _value = int(meta.get(value, 0)) if is_encode else str(value)

        return _value


class SparkFeMap(FeMap):

    def __init__(self, name):
        super(SparkFeMap, self).__init__(name)

    def parser(self, df, is_encode):
        _name = self.name

        counter = Counter(
            df.select(_name)
              .filter(df[self.name].isNotNull())
              .rdd
              .map(lambda x: list(x[_name].keys()))
              .flatMap(lambda x: x)
              .map(lambda x: str(x) if x is not None else "FeNull")
              .countByValue()
        )
        size = len(counter)
        meta = {
            k: c for (k, _), c in zip(counter.most_common(), range(size))}

        if not is_encode:
            logging.warning(
                i18n("Enforced encoding for column '{name}'!\n"
                     "Ignore encode flag: False. Type of '{name}' "
                     "is 'MapType', it must be encoded!")
                .format(name=_name)
                .join("\n", "-" * 60))

        return Feature(name=self.name,
                       fetype=self.fetype,
                       size=size,
                       meta=meta,
                       flag=is_encode)

    def transform(self, value):
        if self.fe is None:
            raise FeatureNotBind()

        return SparkFeMap.static_transform(value,
                                           self.fe.size,
                                           self.fe.meta,
                                           self.fe.flag)

    def schema(self):
        if self.fe is None:
            raise FeatureNotBind()

        return SparkFeMap.static_schema(self.name, self.fe.flag)

    @staticmethod
    def static_schema(*args):
        name, _ = args

        return StructField(name, ArrayType(DoubleType()))

    @staticmethod
    def static_transform(*args):
        value, size, meta, _ = args

        _value = [None for _ in range(size)]
        if value is not None:
            for k, v in value.items():
                if k in meta:
                    _value[meta.get(str(k))] = float(v)

        return _value


class SparkFeArr(FeArr):

    def __init__(self, name):
        super(SparkFeArr, self).__init__(name)

    def parser(self, df, is_encode):
        _name = self.name

        size = len(
            df.select(_name)
              .rdd
              .filter(lambda x: (x[_name] is not None) and (len(x[_name]) > 0))
              .map(lambda x: x[_name])
              .first()
        )

        return Feature(name=self.name,
                       fetype=self.fetype,
                       size=size,
                       flag=is_encode)

    def transform(self, value):
        if self.fe is None:
            raise FeatureNotBind()

        return SparkFeArr.static_transform(value,
                                           self.fe.size,
                                           self.fe.meta,
                                           self.fe.flag)

    def schema(self):
        if self.fe is None:
            raise FeatureNotBind()

        return SparkFeArr.static_schema(self.name, self.fe.flag)

    @staticmethod
    def static_schema(*args):
        name, _ = args

        return StructField(name, ArrayType(DoubleType()))

    @staticmethod
    def static_transform(*args):
        value, size, _, _ = args

        if value is not None:
            _value = [float(i) for i in value]
        else:
            _value = [0.0 for _ in range(size)]

        return _value


class SparkFeParser(FeParser):

    _F2S = {
        SparkFeInt: ("IntegerType", "LongType"),
        SparkFeFloat: ("FloatType", "DoubleType", "DecimalType"),
        SparkFeStr: ("StringType",),
        SparkFeMap: ("MapType",),
        SparkFeArr: ("ArrayType",)
    }
    _S2F = {st: ft for ft, sts in _F2S.items() for st in sts}

    def __init__(self):
        super(SparkFeParser, self).__init__()
        self._spark_app = SparkBaseApp()
        self._spark = self._spark_app.spark

    def _parse_bucket(self, df, feature_names, bucket=None, is_encode=True):
        for fe_name in feature_names:
            fe_schema = df.schema[fe_name]
            fe_dtype = fe_schema.dataType.__class__.__name__
            logging.info(
                i18n("Parsing column '{}' with type <{}>")
                .format(fe_name, fe_dtype))

            fe_parser = self._S2F[fe_dtype](fe_name)
            feature = fe_parser.parser(df, is_encode)
            self._fmap.add_feature(feature, bucket)

    def _parse_single(self, df, feature_name, bucket=None, is_encode=True):
        fe_schema = df.schema[feature_name]
        fe_dtype = fe_schema.dataType.__class__.__name__
        logging.info(
            i18n("Parsing column '{}' with type <{}>")
            .format(feature_name, fe_dtype))

        fe_parser = self._S2F[fe_dtype](feature_name)
        feature = fe_parser.parser(df, is_encode)
        self._fmap.add_feature(feature, bucket)

    def fit(self,
            df,
            buckets=None,
            drop_columns=[],
            primary_keys=[],
            labels=[]):

        if self._fmap is not None:
            logging.warning(i18n("The fmap has been fitted. "
                                 "Please call 'reset' before refitting."))
            return

        self._fmap = Fmap()

        total_fes = set(df.columns) - set(drop_columns)

        for key in primary_keys:
            if key in total_fes:
                self._parse_single(df,
                                   key,
                                   bucket=PKEY_NAME,
                                   is_encode=False)
                total_fes.remove(key)

        for key in labels:
            if key in total_fes:
                self._parse_single(df,
                                   key,
                                   bucket=LABEL_NAME,
                                   is_encode=False)
                total_fes.remove(key)

        if isinstance(buckets, dict):
            for bucket, info in buckets.items():
                fe_names = info.get("features", [])
                is_encode = info.get("is_encode", True)
                self._parse_bucket(df,
                                   fe_names,
                                   bucket=bucket,
                                   is_encode=is_encode)
                total_fes -= set(fe_names)

        if total_fes:
            self._parse_bucket(df, total_fes, is_encode=True)

    def transform(self, df):
        if self._fmap is None:
            raise RuntimeError(
                i18n("The fmap is not fitted. Please call 'fit' first."))

        fmap_bc = self._spark.sparkContext.broadcast(self._fmap)

        def _field_transform(row, field, fe_n2c):
            features = []
            for fe in field.get_features():
                _r = fe_n2c[fe.fetype].static_transform(
                    row[fe.name], fe.size, fe.meta, fe.flag)
                if isinstance(_r, (list, tuple, set)):
                    features.extend(_r)
                else:
                    features.append(_r)

            return features

        def _bucket_transform(row, bucket, fe_n2c):
            feature_nums = _field_transform(row, bucket.nums, fe_n2c)
            feature_ctgs = _field_transform(row, bucket.ctgs, fe_n2c)
            return feature_nums, feature_ctgs

        def _single_transform(row, container, fe_n2c):
            res = []
            for fe in container.get_features():
                _r = fe_n2c[fe.fetype].static_transform(
                    row[fe.name], fe.size, fe.meta, fe.flag)
                res.append(_r)

            return res

        def map_func(row):
            from dlflow.features.parser_spark import FE_N2C
            from dlflow.features.feature_base import PRESET_BUCKETS

            res = []

            res.extend(
                _single_transform(row, fmap_bc.value[PKEY_NAME], FE_N2C))

            res.extend(
                _single_transform(row, fmap_bc.value[LABEL_NAME], FE_N2C))

            buckets = fmap_bc.value.get_buckets(drop=PRESET_BUCKETS)
            for bucket in buckets:
                _nums, _ctgs = _bucket_transform(row, bucket, FE_N2C)

                if _nums:
                    res.append(Vectors.dense(_nums))

                if _ctgs:
                    res.append(_ctgs)

            return res

        schema = self.get_static_schema()

        return df.rdd.map(map_func).toDF(schema=schema)

    def get_static_schema(self):
        schema_list = []

        for fe in self._fmap[PKEY_NAME].get_features():
            schema_list.append(
                FE_N2C[fe.fetype].static_schema(fe.name, fe.flag))

        for fe in self._fmap[LABEL_NAME].get_features():
            schema_list.append(
                FE_N2C[fe.fetype].static_schema(fe.name, fe.flag))

        buckets = self._fmap.get_buckets(drop=PRESET_BUCKETS)
        for bucket in buckets:

            if bucket.nums.get_features():
                name = "_".join([bucket.name, "nums"])
                schema_list.append(StructField(name, VectorUDT()))

            if bucket.ctgs.get_features():
                name = "_".join([bucket.name, "ctgs"])
                schema_list.append(StructField(name, ArrayType(LongType())))

        return StructType(schema_list)


class SparkNormalizer(FeNormalizer):

    def __init__(self):
        super(SparkNormalizer, self).__init__()

        self._normalizer_map = None
        self.default_method = "minmax"

        self._methods = {
            "minmax": MinMaxScaler,
            "zscore": StandardScaler,
            "pnorm": Normalizer
        }

        self._models = {
            "minmax": MinMaxScalerModel,
            "zscore": StandardScalerModel,
            "pnorm": Normalizer
        }

        self._params = {
            "minmax": {"min": 0.0, "max": 1.0},
            "zscore": {"withMean": True, "withStd": True},
            "pnorm": {"p": 2.0}
        }
        self._spark_app = SparkBaseApp()
        self._sc = self._spark_app.sc

    def get_normalizer(self,
                       method="minmax",
                       input_column=None,
                       output_column=None,
                       **kwargs):
        normalizer = None
        normalizer_cls = self._methods.get(method.lower(), None)

        if normalizer_cls is not None:
            param = {
                "inputCol": input_column,
                "outputCol": output_column,
            }
            param.update(kwargs)

            normalizer = normalizer_cls(**param)

        else:
            logging.error(
                i18n("Give up normalizing for '{}'!\n"
                     "Unknown method '{}' for creating normalizer.\n"
                     "Those normalizer are allowed: {}")
                .format(input_column, method, list(self._methods.keys())))

        return normalizer

    def load_normalizer(self, method, model_path):
        normalizer = None
        normalizer_model = self._models.get(method, None)

        if normalizer_model is not None:
            normalizer = normalizer_model.load(model_path)
        else:
            logging.error(
                i18n("Give up loading normalizer!\n"
                     "Unknown method '{}' for loading normalizer.")
                .format(method))

        return normalizer

    def gen_normalizer_info(self, fmap, bucket_conf=None):
        normalizer_info = list()
        buckets = fmap.get_buckets(drop=PRESET_BUCKETS)

        if bucket_conf is None:
            bucket_conf = dict()
            logging.info(i18n("Generating default normalizer."))

        elif not isinstance(bucket_conf, dict):
            logging.error(
                i18n("Parameter Error.\n"
                     "Type of 'bucket_conf' expected <class 'dict'> "
                     "but got {}, the default will be used.")
                .format(type(bucket_conf)))
            bucket_conf = dict()

        else:
            logging.info(
                i18n("Generating normalizer according "
                     "to buckets configuration."))

        for bucket in buckets:
            bucket_name = bucket.name

            if bucket_name not in bucket_conf:
                is_encode = True
                method = self.default_method
                param = self._params[method].copy()

            else:
                is_encode = bucket_conf[bucket_name].get("is_encode", True)
                method = bucket_conf[bucket_name].get(
                    "method", "minmax").lower()
                _param = bucket_conf[bucket_name].get("param", dict())

                if method in self._methods:
                    param = self._params[method].copy()
                    param.update(_param)

                else:
                    logging.warning(
                        i18n("Bucket '{}' was set unknown "
                             "normalize method '{}'!")
                        .format(bucket_name, method))
                    continue

            if is_encode and bucket.nums.fe_size > 0:
                input_name = "_".join([bucket_name, "nums"])
                output_name = "_".join([input_name, method])
                param["inputCol"] = input_name
                param["outputCol"] = output_name

                normalizer_info.append((bucket_name, method, param))

        return normalizer_info

    def fit(self, df, fmap, bucket_conf=None):
        self._normalizer = list()

        normalizer_info = self.gen_normalizer_info(fmap, bucket_conf)

        for bucket_name, method, param in normalizer_info:
            normalizer = self.get_normalizer(method=method, **param)

            if hasattr(normalizer, "fit"):
                normalizer_model = normalizer.fit(df)
            else:
                normalizer_model = normalizer

            _meta = {
                "method": method,
                "param": param,
            }

            self._normalizer.append((bucket_name, _meta, normalizer_model))

    def transform(self, df):
        if self._normalizer is None:
            raise RuntimeError()

        tdf = df
        for _, meta, normalizer in self._normalizer:
            input_column = meta["param"]["inputCol"]
            output_column = meta["param"]["outputCol"]

            tdf = normalizer.transform(tdf) \
                            .drop(input_column) \
                            .withColumnRenamed(output_column, input_column)

        return tdf

    def save(self, save_dir):
        if self._normalizer is None:
            raise RuntimeError()

        save_dir = Path(save_dir)
        meta = dict()

        for bucket_name, _meta, normalizer in self._normalizer:
            _dir = save_dir.joinpath(bucket_name).as_posix()
            normalizer.write().overwrite().save(_dir)

            sub_meta = dict()
            sub_meta.update(_meta)
            sub_meta["save_dir"] = _dir
            meta[bucket_name] = sub_meta

        meta_path = Path(save_dir).joinpath("normalizers_metadata").as_posix()
        meta_jstr = json.dumps(meta)

        self._sc.parallelize([meta_jstr]) \
                .repartition(1) \
                .saveAsTextFile(meta_path)

    def load(self, model_dir):
        model_dir = Path(model_dir)
        meta_path = model_dir.joinpath("normalizers_metadata").as_posix()

        meta_jstr = self._sc.textFile(meta_path).first()
        meta = json.loads(meta_jstr)

        self._normalizer = list()

        for bucket_name, _meta in meta.items():
            method = _meta["method"]
            save_dir = _meta.pop("save_dir")
            normalizer_model = self.load_normalizer(method, save_dir)

            self._normalizer.append((bucket_name, _meta, normalizer_model))


FE_N2C = {
    SparkFeInt.__fetype__: SparkFeInt,
    SparkFeFloat.__fetype__: SparkFeFloat,
    SparkFeStr.__fetype__: SparkFeStr,
    SparkFeMap.__fetype__: SparkFeMap,
    SparkFeArr.__fetype__: SparkFeArr
}
