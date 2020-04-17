from dlflow.tasks import TaskNode, UNIVERSAL_TAG
from dlflow.mgr import task, config
from dlflow.utils.sparkapp import SparkBaseApp, HDFS

from pathlib import Path


_HDFS_MODEL_DIR = "$HDFS_WORKSPACE/model/TAG=$MODEL_TAG" \
                  "/<dt=$MODEL_DATE:yyyy/mm/dd>"
_HDFS_FEMODEL_DIR = "$HDFS_WORKSPACE/femodel/TAG=$FEATURE_TAG" \
                    "/<dt=$MODEL_DATE:yyyy/mm/dd>"
_HDFS_FEATURE_DIR = "$HDFS_WORKSPACE/feature/TAG=$FEATURE_TAG" \
                   "/<dt=$FEATURE_DATE:yyyy/mm/dd>"

_LOCAL_MODEL_DIR = "$LOCAL_WORKSPACE/model/TAG=$MODEL_TAG" \
                   "/<dt=$MODEL_DATE:yyyy/mm/dd>"
_LOCAL_FEMODEL_DIR = "$LOCAL_WORKSPACE/femodel/TAG=$FEATURE_TAG" \
                     "/<dt=$MODEL_DATE:yyyy/mm/dd>"
_LOCAL_TMP_DIR = "$LOCAL_WORKSPACE/tmp"


@task.reg()
class _Root(TaskNode):

    parent_tag = TaskNode.set_tag()
    output_tag = TaskNode.set_tag(UNIVERSAL_TAG, "_START", "_ROOT")

    cfg = config.setting(
        config.req("MODEL_TAG"),
        config.req("MODEL_DATE"),

        config.req("HDFS_WORKSPACE"),
        config.req("PRIMARY_KEYS"),

        config.req("FEATURE_TAG"),
        config.req("FEATURE_DATE"),

        config.opt("HDFS_FEATURE_DIR", _HDFS_FEATURE_DIR),
        config.opt("HDFS_MODEL_DIR", _HDFS_MODEL_DIR),
        config.opt("HDFS_FEMODEL_DIR", _HDFS_FEMODEL_DIR),

        config.opt("LOCAL_WORKSPACE", "./dlflow_default"),
        config.opt("LOCAL_MODEL_DIR", _LOCAL_MODEL_DIR),
        config.opt("LOCAL_FEMODEL_DIR", _LOCAL_FEMODEL_DIR),
        config.opt("LOCAL_TMP_DIR", _LOCAL_TMP_DIR),

        config.opt("DROP_COLUMNS", [])
    )

    def __init__(self):
        super(_Root, self).__init__()

    @TaskNode.timeit
    def run(self):
        def solve_local_path(raw_path):
            path = Path(raw_path).resolve()
            if not path.is_dir():
                path.mkdir(parents=True)

            return path.as_posix()

        config.LOCAL_WORKSPACE = solve_local_path(config.LOCAL_WORKSPACE)
        config.LOCAL_MODEL_DIR = solve_local_path(config.LOCAL_MODEL_DIR)
        config.LOCAL_FEMODEL_DIR = solve_local_path(config.LOCAL_FEMODEL_DIR)
        config.LOCAL_TMP_DIR = solve_local_path(config.LOCAL_TMP_DIR)

        def seq_conf_parser(v, sign=","):
            if isinstance(v, str):
                iter_v = v.split(sign)
            elif isinstance(v, list):
                iter_v = v
            else:
                iter_v = []

            return [i for i in map(lambda x: x.strip(), iter_v) if i]

        config.PRIMARY_KEYS = seq_conf_parser(config.PRIMARY_KEYS)
        config.LABELS = seq_conf_parser(config.LABELS)
        config.DROP_COLUMNS = seq_conf_parser(config.DROP_COLUMNS)

        if "SPARK" in config:
            app_name = ".".join(["DLFlow", config.uuid])
            spark_conf = config.SPARK.dense_dict if config.SPARK else {}
            spark_app = SparkBaseApp()
            spark_app.initialize_spark(app_name, spark_conf)

        else:
            hdfs = HDFS()
            hdfs.initialize_hdfs()
