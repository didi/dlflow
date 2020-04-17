from dlflow.tasks import TaskNode
from dlflow.mgr import task, model, config
from dlflow.utils.sparkapp import SparkBaseApp
from dlflow.features import Fmap
from dlflow.utils.locale import i18n

from absl import logging


_HDFS_PREDICT_DIR = "$HDFS_WORKSPACE/predict/TAG=$FEATURE_TAG" \
                    "/<dt=$FEATURE_DATE:yyyy/mm/dd>"


@task.reg("pred", "predict", "predicting")
class _Predict(TaskNode):

    parent_tag = TaskNode.set_tag("_BUILD",
                                  "TRAINED_MODEL",
                                  "ENCODE_FEATURE")
    output_tag = TaskNode.set_tag("PREDICT_RESULT")

    bind_tasks = "_Build"

    cfg = config.setting(
        config.req("SPARK"),
        config.opt("HDFS_PREDICT_DIR", _HDFS_PREDICT_DIR),
    )

    def __init__(self):
        super(_Predict, self).__init__()

    @TaskNode.timeit
    def run(self):
        spark_app = SparkBaseApp()
        spark = spark_app.spark
        sc = spark_app.sc
        hdfs = spark_app.hdfs

        dirs = config._build_dirs
        tmp_fmap_dir = dirs["tmp_fmap_dir"]
        hdfs_ckpt_dir = dirs["hdfs_ckpt_dir"]
        hdfs_static_dir = dirs["hdfs_static_dir"]

        sc.addFile(hdfs.hdfs_whole_path(hdfs_static_dir.as_posix()),
                   recursive=True)
        sc.addFile(hdfs.hdfs_whole_path(hdfs_ckpt_dir.as_posix()),
                   recursive=True)

        fmap = Fmap.load(tmp_fmap_dir)

        bc_static_model_dir = sc.broadcast("static")
        bc_fmap = sc.broadcast(fmap)
        bc_config = sc.broadcast(config)

        def predict_map(rdd):
            from pyspark.files import SparkFiles

            config = bc_config.value
            fmap = bc_fmap.value
            static_dir = SparkFiles.get(bc_static_model_dir.value)
            ckpt_dir = SparkFiles.get("ckpt")

            from dlflow.mgr import Collector, model
            collect = Collector()
            collect(static_dir, "Static models")

            input_cls = model[config.MODEL.input_name]
            dataset = input_cls(fmap).rdd_inputs(rdd, config.MODEL.batch_size)

            model_cls = model[config.MODEL.model_name]
            model_ins = model_cls(fmap)
            model_ins.load_model(ckpt_dir)

            return model_ins.predict_act(dataset)

        local_model = model[config.MODEL.model_name](fmap)
        df_title = local_model.pkey_names
        df_title.extend(local_model.output_names)

        df = spark.read.parquet(config.HDFS_ENCODE_DIR)

        parallelism = spark.conf.get("spark.default.parallelism", None)
        if parallelism:
            num_partitions = int(parallelism)
        else:
            num_partitions = df.rdd.getNumPartitions()

        pred_df = df.repartition(num_partitions) \
                    .rdd \
                    .mapPartitions(predict_map) \
                    .toDF(df_title)

        hdfs_predict_dir = config.HDFS_PREDICT_DIR
        spark_app.save_compress(pred_df, hdfs_predict_dir)

        logging.info(
            i18n("Predicting result save to {}")
            .format(hdfs_predict_dir))
        logging.info(i18n("Predicting Done."))
