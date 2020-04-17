from dlflow.tasks import TaskNode
from dlflow.mgr import task, model, config
from dlflow.features import Fmap
from dlflow.utils.sparkapp import HDFS
from dlflow.utils.locale import i18n

from pathlib import Path
from absl import logging
import tensorflow as tf
import shutil


@task.reg("train", "training")
class _Train(TaskNode):
    parent_tag = TaskNode.set_tag("_BUILD", "TFRECORD_FEATURE")
    output_tag = TaskNode.set_tag("TRAINED_MODEL")

    bind_tasks = "_Build"

    def __init__(self):
        super(_Train, self).__init__()

    @TaskNode.timeit
    def run(self):
        gpus = tf.config.experimental.list_physical_devices(
            device_type='GPU')
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(device=gpu,
                                                     enable=True)

        hdfs = HDFS()

        dirs = config._build_dirs
        ckpt_dir = dirs["ckpt_dir"]
        tmp_fmap_dir = dirs["tmp_fmap_dir"]
        tmp_ckpt_dir = dirs["tmp_ckpt_dir"]
        hdfs_ckpt_dir = dirs["hdfs_ckpt_dir"]
        hdfs_static_dir = dirs["hdfs_static_dir"]
        local_ckpt_dir = dirs["local_ckpt_dir"]
        local_ckpt_link = dirs["local_ckpt_link"]
        local_static_dir = dirs["local_static_dir"]

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

        model_ins.train_act(dataset)

        if local_ckpt_dir.exists():
            logging.warning(
                i18n("Local ckpt directory already exists, "
                     "it will be overwritten: {}")
                .format(local_ckpt_dir))
            shutil.rmtree(local_ckpt_dir)
        logging.info(i18n("New ckpt is saved to {}").format(local_ckpt_dir))
        model_ins.save(local_ckpt_dir)

        logging.info(i18n("Creating soft link to local ckpt {}")
                     .format(local_ckpt_link))
        if local_ckpt_link.is_symlink():
            local_ckpt_link.unlink()
        local_ckpt_link.symlink_to(ckpt_dir)

        if hdfs.exists(hdfs_ckpt_dir):
            logging.warning(i18n("The ckpt already exists on HDFS, "
                                 "the old one will be overwritten."))
            hdfs.delete(hdfs_ckpt_dir)
        logging.info(i18n("Put ckpt to HDFS: {}").format(hdfs_ckpt_dir))
        hdfs.put(local_ckpt_dir, hdfs_ckpt_dir)

        if hdfs.exists(hdfs_static_dir):
            hdfs.delete(hdfs_static_dir)
        hdfs.put(local_static_dir, hdfs_static_dir)

        logging.info(i18n("Training don."))
