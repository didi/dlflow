from dlflow.utils.utilclass import SingletonMeta
from dlflow.utils.locale import i18n
from dlflow.mgr.errors import NotInitializeError

from datetime import datetime, timedelta
from pathlib import Path
from time import time
from absl import logging
import os


_UNITS_NAME = ["B",
               "KB",
               "MB",
               "GB",
               "TB",
               "PB",
               "EB",
               "ZB",
               "YB",
               "BB",
               "NB",
               "DB",
               "CB"]
_UNITS_ENUM = {k: v for k, v in zip(_UNITS_NAME, range(len(_UNITS_NAME)))}
_HDFS_TMP_DIR = "/user/dm/compress/tmp/dlflow"
_HDFS_EXCEPT_PARTITION = 100


def init_spark():
    try:
        import pyspark

    except ModuleNotFoundError:

        try:
            logging.warning(
                i18n("No model named 'pyspark'. "
                     "Trying to call 'findspark'..."))
            import findspark
            findspark.init()
            logging.info(i18n("Loading pyspark success."))

        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                i18n("Can not find model 'pysaprk' or 'findspark'."))


class HDFS(metaclass=SingletonMeta):

    @classmethod
    def partof(cls, size, per_size, rt_base="MB", radix=1024):
        for _ in range(_UNITS_ENUM[rt_base.upper()]):
            size /= radix

        parts_num = int(size // per_size + 1)

        return parts_num

    def __init__(self):
        self._sc = None
        self._fs = None
        self._header = None
        self._status = False

    @property
    def fs(self):
        return self._fs

    @property
    def sc(self):
        return self._sc

    @property
    def status(self):
        return self._status

    def initialize_hdfs(self, sc=None):
        if self._status:
            logging.info(i18n("Using exists HDFS Handler."))
            return

        init_spark()
        from pyspark.sql import SparkSession
        from pyspark.context import SparkContext

        if sc is None:
            new_sc = SparkSession.builder \
                                 .appName("DLFlow.HDSF_Handler") \
                                 .master("local[*]") \
                                 .enableHiveSupport() \
                                 .getOrCreate() \
                                 .sparkContext
            logging.info(i18n("Using new local SparkContext."))
        else:
            new_sc = sc
            logging.info(i18n("Using presetting SparkContext."))

        if not isinstance(new_sc, SparkContext):
            raise TypeError(
                i18n("Expected type {} for 'sc', but got {}")
                .format(str(SparkContext), type(new_sc)))

        if new_sc._jsc is None:
            raise RuntimeError(i18n("SparkContext has already closed!"))

        hadoop_conf = new_sc._jsc.hadoopConfiguration()
        self._header = hadoop_conf.get("fs.default.name")

        self._fs = new_sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        self._sc = new_sc
        self._status = True

    def close(self):
        if self._status:
            self._sc.stop()
            self._status = False

    def path(self, path):
        if not self._status:
            raise NotInitializeError(
                i18n("Can not access HDFS! Because "
                     "SparkContext wasn't initialize!"))

        path = Path(path).as_posix()
        jvm_hdfs_path = self._sc._jvm.org.apache.hadoop.fs.Path(path)

        return jvm_hdfs_path

    def exists(self, path):
        _path = self.path(path)
        return self._fs.exists(_path)

    def delete(self, path):
        _path = self.path(path)
        return self._fs.delete(_path)

    def mkdirs(self, path):
        _path = self.path(path)
        return self._fs.mkdirs(_path)

    def get(self, src, dst):
        _src = self.path(src)
        _dst = self.path(dst)
        self._fs.copyToLocalFile(_src, _dst)

    def put(self, src, dst):
        _src = self.path(src)
        _dst = self.path(dst)
        self._fs.copyFromLocalFile(_src, _dst)

    def isdir(self, path):
        _path = self.path(path)
        return self._fs.isDirectory(_path)

    def isfile(self, path):
        _path = self.path(path)
        return self._fs.isFile(_path)

    def sizeof(self, path):
        _path = self.path(path)
        return self._fs.getContentSummary(_path).getLength()

    def hdfs_whole_path(self, path, header=None):
        if header is None:
            prefix = self._header.strip()
        else:
            prefix = header.strip()

        if path[0] == "/":
            new_path = "".join([prefix, path])
        elif prefix == path[:len(prefix)]:
            new_path = path
        else:
            raise ValueError(
                i18n("Illegal HDFS Path: {}").format(path))

        return new_path

    def hdfs_concat_path(self, path1, path2, whole_path=False, header=None):
        if header is None:
            prefix = self._header.strip()
        else:
            prefix = header.strip()

        path1 = path1.replace(prefix, "")
        path2 = path2.replace(prefix, "").strip(os.sep)
        new_path = os.path.join(path1, path2)
        if whole_path:
            new_path = self.hdfs_whole_path(new_path, header=header)

        return new_path

    def _degrade_hdfs(self, base_dt, hdfs_patten, max_degrade):
        valid_dir = None
        valid_date = None
        dt = datetime.strptime(base_dt, "%Y%m%d")
        for i in range(max_degrade + 1):
            test_dt = dt + timedelta(days=-i)
            test_dir = test_dt.strftime(hdfs_patten)
            test_success = os.path.join(test_dir, "_SUCCESS")

            logging.info(i18n("Testing {}").format(test_dir))
            if self.exists(test_success):
                valid_dir, valid_date = test_dir, test_dt
                break

        return valid_dir, valid_date

    def safe_hdfs_path(self, base_dt, hdfs_patten, max_degrade=7):
        valid_dir, _ = self._degrade_hdfs(base_dt, hdfs_patten, max_degrade)

        if valid_dir is None:
            raise ValueError(
                i18n("Can't find valid partition on {}").format(hdfs_patten))

        logging.info(i18n("Find valid partition {}").format(valid_dir))
        return valid_dir

    def safe_hive_date(self, base_dt, hdfs_patten, max_degrade=7,
                       fmt_dt="%Y%m%d"):
        _, valid_dt = self._degrade_hdfs(base_dt, hdfs_patten, max_degrade)

        if valid_dt is None:
            raise ValueError(
                i18n("Can't find valid partition on {}").format(hdfs_patten))

        valid_dt = valid_dt.strftime(fmt_dt)
        logging.info(i18n("Find valid hive date {}").format(valid_dt))
        return valid_dt


class SparkBaseApp(metaclass=SingletonMeta):

    def __init__(self):
        self.spark_conf = None
        self.app_name = None

        self._spark = None
        self._sc = None
        self._hdfs = HDFS()
        self._status = False

    def initialize_spark(self, app_name=None, spark_conf=None, reuse=False):
        if self._status and not reuse:
            logging.info(i18n("Using exists spark session."))
            return
        elif self._status and reuse:
            self.close()

        if self._hdfs.status:
            self._hdfs.close()

        init_spark()
        from pyspark.sql import SparkSession

        _spark = SparkSession.builder

        if app_name is not None:
            _spark = _spark.appName(app_name)
            logging.info(i18n("Initializing Spark App: {}").format(app_name))

        if isinstance(spark_conf, dict):
            spark_conf = spark_conf.copy()
            self.spark_conf = spark_conf.copy()

            if "spark.executorEnv.PYSPARK_PYTHON" in spark_conf:
                os.environ["PYSPARK_PYTHON"] = \
                    spark_conf["spark.executorEnv.PYSPARK_PYTHON"]

            _fmt_conf_str = "{\n"
            for k, v in spark_conf.items():
                _fmt_conf_str += "{}{}: {}\n".format(" "*4, k, v)
            _fmt_conf_str += "}\n"
            logging.info(
                i18n("Using spark config:\n{s}").format(s=_fmt_conf_str))

            if "master" in spark_conf:
                _spark = _spark.master(spark_conf["master"])
                _ = spark_conf.pop("master")

            for conf_key, conf_value in spark_conf.items():
                _spark = _spark.config(conf_key, conf_value)

        self._spark = _spark.enableHiveSupport().getOrCreate()
        self._sc = self._spark.sparkContext
        self.app_name = app_name

        self._hdfs.initialize_hdfs(sc=self._sc)
        self._status = True

        logging.info(i18n("Spark Version: {}").format(self._spark.version))

    @property
    def spark(self):
        return self._spark

    @property
    def sc(self):
        return self._sc

    @property
    def hdfs(self):
        return self._hdfs

    @property
    def status(self):
        return self._status

    def close(self):
        if self._status:
            self._spark.stop()

    def save_compress(self, df, out_dir, use_tfr=False):
        if not self._status:
            raise NotInitializeError(i18n("Spark APP not initialize."))

        start_time = time()

        tmp_dir = self._hdfs.hdfs_concat_path(_HDFS_TMP_DIR, out_dir)
        if self._hdfs.exists(tmp_dir):
            logging.warning(
                i18n("Directory already exists, it will be overwritten: {}")
                .format(tmp_dir))
            self._hdfs.delete(tmp_dir)

        if use_tfr:
            df.write.format("tfrecords").save(tmp_dir)
        else:
            df.write.save(tmp_dir)

        size = self._hdfs.sizeof(tmp_dir)
        num_partition = self._hdfs.partof(
            size=size, per_size=512, rt_base="MB")
        if num_partition > _HDFS_EXCEPT_PARTITION:
            num_partition = self._hdfs.partof(
                size=size, per_size=1, rt_base="GB")
        if num_partition > _HDFS_EXCEPT_PARTITION:
            num_partition = self._hdfs.partof(
                size=size, per_size=2, rt_base="GB")

        if self._hdfs.exists(out_dir):
            logging.warning(
                i18n("Directory already exists, it will be overwritten: {}")
                .format(out_dir))
            self._hdfs.delete(out_dir)

        if use_tfr:
            logging.info("Saving tfrecord to {}".format(out_dir))
            self.spark.read \
                .format("tfrecords") \
                .load(tmp_dir) \
                .repartition(num_partition) \
                .write \
                .format("tfrecords") \
                .save(out_dir)
        else:
            logging.info(i18n("Saving parquet to {}").format(out_dir))
            self.spark.read \
                .parquet(tmp_dir) \
                .repartition(num_partition) \
                .write \
                .save(out_dir)

        save_time_used = time() - start_time
        logging.info(
            i18n("Compress save success with {} parts. "
                 "Time spend: {:>.0f}s")
            .format(num_partition, save_time_used))

        logging.warning(i18n("Delete tmp dir - {}").format(tmp_dir))
        if self._hdfs.delete(tmp_dir):
            logging.info(i18n("Delete success."))
        else:
            logging.info(i18n("Delete fail."))
