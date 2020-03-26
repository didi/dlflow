from dlflow.utils.sparkapp import SparkBaseApp
from dlflow.mgr import task, config
from dlflow.tasks import TaskNode

from pathlib import Path
from absl import logging
import pandas as pd
import requests


_HDFS_FEATURE_DIR = "$HDFS_WORKSPACE/features/TAG=$FEATURE_TAG" \
                    "/<dt=$FEATURE_DATE:yyyy/mm/dd>"

training_url = "http://download.tensorflow.org/data/iris_training.csv"
test_url = "http://download.tensorflow.org/data/iris_test.csv"

data_header = ["sepal_length",
               "sepal_width",
               "petal_length",
               "petal_width",
               "species"]


def get_data():
    data_dir = Path(config.DATA_DIR).resolve()
    if not data_dir.is_dir():
        data_dir.mkdir(parents=True)

    train_data_path = data_dir.joinpath("iris_training.csv")
    test_data_path = data_dir.joinpath("iris_test.csv")

    if not train_data_path.exists():
        req_train = requests.get(training_url)
        txt_train = req_train.content.decode("utf8").split("\n")[1:]

        train_data_path.write_text("\n".join(txt_train))

    if not test_data_path.exists():
        req_test = requests.get(test_url)
        txt_test = req_test.content.decode("utf8").split("\n")[1:]

        test_data_path.write_text("\n".join(txt_test))

    return train_data_path, test_data_path


@task.reg("data_prepare")
class DataPrepare(TaskNode):

    parent_tag = TaskNode.set_tag("_START")
    output_tag = TaskNode.set_tag("RAW_FEATURE")

    cfg = config.setting(
        config.req("SPARK"),
        config.req("IRIS_DATASET_FOR"),
        config.opt("DATA_DIR", "$LOCAL_WORKSPACE/data_dir"),
        config.opt("HDFS_FEATURE_DIR", _HDFS_FEATURE_DIR),
    )

    def __init__(self):
        super(DataPrepare, self).__init__()

    @TaskNode.timeit
    def run(self):

        spark_app = SparkBaseApp()
        spark = spark_app.spark
        hdfs = spark_app.hdfs

        hdfs_save_path = Path(config.HDFS_FEATURE_DIR)

        if hdfs.exists(hdfs_save_path / "_SUCCESS"):
            logging.info("Exists data found. {}".format(hdfs_save_path))
            logging.info("Data is ready! Skip prepare...")
            return

        train_data_path, test_data_path = get_data()
        if config.IRIS_DATASET_FOR == "train":
            local_data_path = train_data_path
        else:
            local_data_path = test_data_path

        pdf = pd.read_csv(local_data_path, header=None)
        pdf.columns = data_header
        pdf["idx"] = [i for i in range(1, len(pdf) + 1)]

        df = spark.createDataFrame(pdf)

        logging.info("Saving Dataset to {}".format(hdfs_save_path))
        df.repartition(1).write.save(hdfs_save_path.as_posix())
