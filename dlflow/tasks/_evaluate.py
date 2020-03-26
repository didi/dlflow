from dlflow.tasks import TaskNode
from dlflow.mgr import task, model, config
from dlflow.features import Fmap
from dlflow.utils.sparkapp import HDFS
from dlflow.utils.locale import i18n

from pathlib import Path
from absl import logging
import tensorflow as tf


@task.reg("test", "eval", "evaluate", "evaluating")
class _Evaluate(TaskNode):

    parent_tag = TaskNode.set_tag("_BUILD",
                                  "TFRECORD_FEATURE",
                                  "ENCODE_FEATURE",
                                  "TRAINED_MODEL")
    output_tag = TaskNode.set_tag("EVALUATE_RESULT")

    bind_tasks = "_Build"

    def __init__(self):
        super(_Evaluate, self).__init__()

    @TaskNode.timeit
    def run(self):
        hdfs = HDFS()

        dirs = config._build_dirs
        tmp_fmap_dir = dirs["tmp_fmap_dir"]
        tmp_ckpt_dir = dirs["tmp_ckpt_dir"]

        fmap = Fmap.load(tmp_fmap_dir)

        input_cls = model[config.MODEL.input_name]

        files_pattern = hdfs.hdfs_whole_path(
            Path(config.HDFS_TFRECORD_DIR).joinpath("part*").as_posix())
        files = tf.io.gfile.glob(files_pattern)
        dataset = input_cls(fmap).tfr_inputs(files)

        model_cls = model[config.MODEL.model_name]
        model_ins = model_cls(fmap)

        if tmp_ckpt_dir.joinpath("h5weights", "weights.h5").exists():
            logging.info(i18n("Loading model weight success."))
            model_ins.load_weights(tmp_ckpt_dir)

        model_ins.evaluate_act(dataset)

        logging.info(i18n("Evaluating done."))
