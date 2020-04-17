from dlflow.tasks import TaskNode
from dlflow.mgr import task, config
from dlflow.features import Parser
from dlflow.utils.sparkapp import SparkBaseApp
from dlflow.utils.locale import i18n

from absl import logging
from pathlib import Path
import shutil


_DEFAULT_PARALLELISM = 100
_HDFS_ENCODE_DIR = "$HDFS_WORKSPACE/encode/TAG=$FEATURE_TAG" \
                   "/<dt=$FEATURE_DATE:yyyy/mm/dd>"


@task.reg("encode", "encoding")
class _Encode(TaskNode):

    parent_tag = TaskNode.set_tag("RAW_FEATURE")
    output_tag = TaskNode.set_tag("ENCODE_FEATURE", "TFRECORD_FEATURE")

    cfg = config.setting(
        config.req("SPARK"),
        config.req("BUCKET", None),
        config.opt("HDFS_ENCODE_DIR", _HDFS_ENCODE_DIR),
    )

    def __init__(self):
        super(_Encode, self).__init__()

    @TaskNode.timeit
    def run(self):
        spark_app = SparkBaseApp()
        spark = spark_app.spark
        hdfs = spark_app.hdfs

        if "HDFS_TFRECORD_DIR" in config:
            hdsf_tfrecord_dir = Path(config.HDFS_TFRECORD_DIR)
            if hdfs.exists(hdsf_tfrecord_dir / "_SUCCESS"):
                logging.info(
                    i18n("TFRecords already exists, skip encoding."))
                return

        elif "HDFS_ENCODE_DIR" in config:
            hdfs_encode_dir = Path(config.HDFS_ENCODE_DIR)
            if hdfs.exists(hdfs_encode_dir / "_SUCCESS"):
                logging.info(
                    i18n("Encode result already exists, encoding done."))
                return

        fmap_dir = "fmap_{}".format(config.uuid)
        tmp_fmap_dir = Path(config.LOCAL_TMP_DIR).joinpath(fmap_dir)

        local_fmap_dir = Path(config.LOCAL_FEMODEL_DIR).joinpath("fmap")
        local_norm_dir = Path(config.LOCAL_FEMODEL_DIR).joinpath("norm")

        hdfs_fmap_dir = Path(config.HDFS_FEMODEL_DIR).joinpath("fmap")
        hdfs_norm_dir = Path(config.HDFS_FEMODEL_DIR).joinpath("norm")

        if not hdfs.exists(config.HDFS_FEMODEL_DIR):
            hdfs.mkdirs(hdfs_fmap_dir.parent)

        spark_parser = Parser("spark")
        parser_cls = spark_parser.get_parser()
        normalizer_cls = spark_parser.get_normalizer()

        if "SPARK.spark.default.parallelism" in config:
            parallelism = int(config.SPARK.spark.default.parallelism)
        else:
            parallelism = _DEFAULT_PARALLELISM

        df = spark.read \
                  .parquet(config.HDFS_FEATURE_DIR) \
                  .repartition(parallelism)

        parser = parser_cls()

        if hdfs.exists(hdfs_fmap_dir.joinpath("fmap.meta")):
            logging.info(i18n("Using HDFS fmap: {}")
                         .format(hdfs_fmap_dir))
            hdfs.get(hdfs_fmap_dir, tmp_fmap_dir)

        else:
            logging.info(i18n("There is no fmap available, start to "
                              "generate fmap by parsing features."))

            primary_keys = config.PRIMARY_KEYS
            labels = config.LABELS
            drop_columns = config.DROP_COLUMNS
            buckets = None if config.BUCKET is None else config.BUCKET.dict

            parser.fit(df,
                       buckets=buckets,
                       drop_columns=drop_columns,
                       primary_keys=primary_keys,
                       labels=labels)
            parser.save(tmp_fmap_dir)

            logging.info(i18n("Put fmap to HDFS: {}").format(hdfs_fmap_dir))
            hdfs.delete(hdfs_fmap_dir)
            hdfs.put(tmp_fmap_dir, hdfs_fmap_dir)

            if local_fmap_dir.exists():
                logging.warning(
                    i18n("Local directory {} already exists, "
                         "it will be overwritten: {}")
                    .format("fmap", local_fmap_dir))
                shutil.rmtree(local_fmap_dir)
            shutil.copytree(tmp_fmap_dir, local_fmap_dir)

        parser.load(tmp_fmap_dir)
        encode_df = parser.transform(df)

        normalizer = normalizer_cls()

        if hdfs.exists(hdfs_norm_dir.joinpath("normalizers_metadata",
                                              "_SUCCESS")):
            normalizer.load(hdfs_norm_dir)

        else:
            hdfs.mkdirs(hdfs_norm_dir)

            try:
                bucket_conf = config.BUCKET.dict
            except AttributeError:
                bucket_conf = None
                if config.BUCKET is not None:
                    logging.error(
                        i18n("Get wrong type bucket configuration."))

            normalizer.fit(encode_df,
                           parser.fmap,
                           bucket_conf=bucket_conf)
            normalizer.save(hdfs_norm_dir)

            if local_norm_dir.exists():
                logging.warning(
                    i18n("Local directory {} already exists, "
                         "it will be overwritten: {}")
                    .format("norm", local_norm_dir))
                shutil.rmtree(local_norm_dir)

            hdfs.get(hdfs_norm_dir, local_norm_dir)

        norm_df = normalizer.transform(encode_df)

        spark_app.save_compress(norm_df,
                                config.HDFS_ENCODE_DIR,
                                use_tfr=False)

        if "HDFS_TFRECORD_DIR" in config:
            spark_app.save_compress(norm_df,
                                    config.HDFS_TFRECORD_DIR,
                                    use_tfr=True)

        logging.info(i18n("Encoding done."))
