from dlflow.tasks import TaskNode
from dlflow.mgr import task, config
from dlflow.mgr.errors import FmapNotExists
from dlflow.utils.sparkapp import HDFS
from dlflow.utils.locale import i18n

from absl import logging
from pathlib import Path
import shutil


_HDFS_ENCODE_DIR = "$HDFS_WORKSPACE/encode/TAG=$FEATURE_TAG" \
                   "/<dt=$FEATURE_DATE:yyyy/mm/dd>"
_HDFS_TFRECORD_DIR = "$HDFS_WORKSPACE/tfrecord/TAG=$FEATURE_TAG" \
                     "/<dt=$FEATURE_DATE:yyyy/mm/dd>"


@task.reg()
class _Build(TaskNode):

    parent_tag = TaskNode.set_tag("ENCODE_FEATURE", "TFRECORD_FEATURE")
    output_tag = TaskNode.set_tag("_BUILD")

    cfg = config.setting(
        config.req("MODELS_DIR"),
        config.req("MODEL.model_name"),
        config.req("MODEL.input_name"),

        config.opt("HDFS_ENCODE_DIR", _HDFS_ENCODE_DIR),
        config.opt("HDFS_TFRECORD_DIR", _HDFS_TFRECORD_DIR),
    )

    def __init__(self):
        super(_Build, self).__init__()

    @TaskNode.timeit
    def run(self):
        hdfs = HDFS()

        local_static_dir = Path(config.MODELS_DIR).resolve()
        hdfs_static_dir = Path(config.HDFS_MODEL_DIR).joinpath("static")

        fmap_dir = "fmap_{}".format(config.uuid)
        tmp_fmap_dir = Path(config.LOCAL_TMP_DIR).joinpath(fmap_dir)
        local_fmap_dir = Path(config.LOCAL_FEMODEL_DIR).joinpath("fmap")
        hdfs_fmap_dir = Path(config.HDFS_FEMODEL_DIR).joinpath("fmap")

        ckpt_dir = "ckpt_{}".format(config.uuid)
        tmp_ckpt_dir = Path(config.LOCAL_TMP_DIR).joinpath(ckpt_dir)
        local_ckpt_link = Path(config.LOCAL_MODEL_DIR).joinpath("ckpt")
        local_ckpt_dir = Path(config.LOCAL_MODEL_DIR).joinpath(ckpt_dir)
        hdfs_ckpt_dir = Path(config.HDFS_MODEL_DIR).joinpath("ckpt")

        if hdfs.exists(hdfs_fmap_dir.joinpath("fmap.meta")):
            logging.info(
                i18n("Using HDFS fmap: {}").format(hdfs_fmap_dir))
            hdfs.get(hdfs_fmap_dir, tmp_fmap_dir)

        elif local_fmap_dir.joinpath("fmap.meta").exists():
            logging.info(
                i18n("Using local fmap: {}").format(local_fmap_dir))
            shutil.copytree(local_fmap_dir, tmp_fmap_dir)

        else:
            raise FmapNotExists(
                i18n("No available fmap found, pleas "
                     "process feature encoding first."))

        if hdfs.exists(hdfs_ckpt_dir):
            logging.info(
                i18n("Getting ckpt from HDFS: {}").format(hdfs_ckpt_dir))
            hdfs.get(hdfs_ckpt_dir, tmp_ckpt_dir)

        elif local_ckpt_link.resolve().exists():
            logging.info(
                i18n("Using local ckpt: {}").format(local_ckpt_link))
            shutil.copytree(local_ckpt_link.resolve(), tmp_ckpt_dir)

        else:
            logging.info(
                i18n("No available ckpt found, reinitializing..."))

        config._build_dirs = {
            "fmap_dir": fmap_dir,
            "ckpt_dir": ckpt_dir,

            "hdfs_fmap_dir": hdfs_fmap_dir,
            "hdfs_ckpt_dir": hdfs_ckpt_dir,
            "hdfs_static_dir": hdfs_static_dir,

            "tmp_fmap_dir": tmp_fmap_dir,
            "tmp_ckpt_dir": tmp_ckpt_dir,

            "local_fmap_dir": local_fmap_dir,
            "local_ckpt_dir": local_ckpt_dir,
            "local_static_dir": local_static_dir,

            "local_ckpt_link": local_ckpt_link
        }
