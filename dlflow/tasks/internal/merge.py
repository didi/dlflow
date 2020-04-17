from dlflow.tasks import TaskNode
from dlflow.mgr import task, config
from dlflow.utils.locale import i18n
from dlflow.utils import DLFLOW_LIB

from pathlib import Path
from absl import logging
import subprocess
import sys


_JAR = Path(DLFLOW_LIB).joinpath(
    "dmflow-1.0-SNAPSHOT-jar-with-dependencies.jar")

_SPARK_SUBMIT = """
spark-submit
--class com.didi.dm.dmflow.pipeline.stage.FeatureFlowRunner
{spark_conf}
{jar}
--seedSQL "{seed_sql}"
--featureConfig "{feature_config}"
--featureDate "{feature_date}"
--featureModelHDFS "{feature_model_hdfs}"
--featureOutHDFS "{feature_out_hdfs}"
{fit_args}
"""


@task.reg("merge")
class _Merge(TaskNode):

    parent_tag = TaskNode.set_tag("_START")
    output_tag = TaskNode.set_tag("RAW_FEATURE")

    cfg = config.setting(
        config.req("SPARK"),
        config.req("MERGE.config_file"),
        config.req("MERGE.fit"),
        config.req("MERGE.seed_sql")
    )

    def __init__(self):
        super(_Merge, self).__init__()

    @TaskNode.timeit
    def run(self):
        fit = config.MERGE.fit

        seed_sql = config.MERGE.seed_sql

        feature_config_file = Path(config.MERGE.config_file).resolve()

        if fit:
            assert feature_config_file.is_file(), \
                i18n("Input feature configuration is not exists "
                     "when fit=true: {}") \
                .format(feature_config_file)

        _spark_conf = config.SPARK.dense_dict
        params = []
        for k, v in _spark_conf.items():
            if k.startswith("spark."):
                _param = '--conf "{}={}"'.format(k, v)
            else:
                _param = '--{} "{}"'.format(k, v)

            params.append(_param)

        spark_conf = "\n".join(params)

        _model_dir = Path(config.HDFS_FEMODEL_DIR)
        hdfs_merge_dir = _model_dir.joinpath("merge").as_posix()

        out_dir = Path(config.RAW_FEATURES).as_posix()

        spark_submit = SparkJARExec(spark_conf=spark_conf,
                                    seed_sql=seed_sql,
                                    feature_config=feature_config_file,
                                    feature_date=config.FEATURE_DATE,
                                    feature_model_hdfs=hdfs_merge_dir,
                                    feature_out_hdfs=out_dir,
                                    fit=fit)

        spark_submit.run()

        logging.info(i18n("Feature merge done."))


class SparkJARExec(object):
    def __init__(self,
                 spark_conf,
                 seed_sql,
                 feature_config,
                 feature_date,
                 feature_model_hdfs,
                 feature_out_hdfs,
                 fit=False):

        fit_args = "--fit true" if fit else "--fit false"
        jar = _JAR.as_posix()

        self.cmd = _SPARK_SUBMIT.format(
                spark_conf=spark_conf,
                jar=jar,
                seed_sql=seed_sql,
                feature_config=feature_config,
                feature_date=feature_date,
                feature_model_hdfs=feature_model_hdfs,
                feature_out_hdfs=feature_out_hdfs,
                fit_args=fit_args)

    def run(self):
        exec_cmd = self.cmd.replace("\n", " ")

        p = subprocess.Popen(exec_cmd,
                             shell=True,
                             bufsize=1,
                             universal_newlines=True)
        p.wait()
        sys.stdout.flush()

        code = p.returncode
        if code != 0:
            raise RuntimeError(
                i18n("Feature merge fail! Got non-zero return code.")
                .format(code))
