# -*- coding: utf-8 -*-

'''执行 FeatureFlowRunner SparkJob的一个例子

fitDate="20200229"
transDate="20200303"

# seed人群的SQL
seedSQL="select pid from db.label where concat(year,month,day)='20200229'"

# HDFS上的模型地址和产出地址
model_pt="/YOUR_HDFS_PREFIX/dmflow_demo.model"
fit_out_pt="/YOUR_HDFS_PREFIX/dmflow_feature.${fitDate}"
trans_out_pt="/YOUR_HDFS_PREFIX/dmflow_feature.${transDate}"

# 步骤一：生成FeatureFlow模型，存在HDFS上
python run.py -f FeatureFlow.conf \
              -s "${seedSQL}" \
              -d ${fitDate} \
              -m "${model_pt}" \
              -o "${fit_out_pt}" \
              --fit

# 步骤二：读取模型，合并新的特征
python run.py -f FeatureFlow.conf \
              -s "${seedSQL}" \
              -d ${transDate} \
              -m "${model_pt}" \
              -o "${trans_out_pt}"
'''

from __future__ import print_function

import os
import sys
import logging
import subprocess
import argparse
import pprint


SPARK_PREFIX = "dmflow"
JAR_NAME = "dmflow-1.0.0-SNAPSHOT.jar"

DEBUG = False
PROD = False

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s [%(levelname)s] - %(message)s',
                    datefmt='%Y-%m-%d %a %H:%M:%S',
                    stream=sys.stdout)


class Config:
    def __init__(self):
        self.MaxExecutors = 300
        self.parallelism = 1000
        self.cores = 2

        self.APP_PREFIX = SPARK_PREFIX
        self.YARN_QUEUE = "root.my_yarn_queue"

    def __repr__(self):
        return """MLflow Runner Config (
        -------------  Project Config  ------------
        YARN_QUEUE: {0}
        -------------  Spark Runtime Config  ------------
        MaxExecutors: {1}
        parallelism: {2}
        cores: {3}
        )
        """.format(self.YARN_QUEUE,
                   self.MaxExecutors,
                   self.parallelism,
                   self.cores)


class HDFS:

    @staticmethod
    def exists(hdfs_path):
        cmd = "hadoop fs -test -e " + hdfs_path
        logging.debug("run shell: " + cmd)

        ret = subprocess.call(cmd, shell=True)
        return True if ret == 0 else False

    @staticmethod
    def copyTolocal(hdfs_path):
        cmd = "hadoop fs -get {0} .".format(hdfs_path)
        logging.debug("run shell: " + cmd)
        subprocess.call(cmd, shell=True)


class ExecWrapper:
    cmd = 'echo "replace cmd as your command"'

    def run(self):
        cmd = self.cmd
        logging.debug(r'exec cmd: %s' % cmd)

        p = subprocess.Popen(cmd,
                             shell=True,
                             bufsize=1,
                             universal_newlines=True)
        p.wait()
        sys.stdout.flush()

        code = p.returncode
        if code != 0:
            raise RuntimeError(
                "subprocess run shell failed! ret=" + str(code))


class ShowSparkVersion(ExecWrapper):
    def __init__(self):
        self.cmd = "spark-submit --version"


class FeatureFlowRunner(ExecWrapper):
    TEMPLATE = '''
    spark-submit  --queue {YARN_QUEUE}  \
        --class com.didi.dm.dmflow.FeatureFlowRunner \
        --name "{appName}" \
        --driver-memory 2g \
        --executor-memory 12g \
        --conf "spark.dynamicAllocation.enabled=true" \
        --conf "spark.driver.maxResultSize=1g" \
        --conf "spark.dynamicAllocation.minExecutors=100" \
        --conf "spark.dynamicAllocation.maxExecutors={MaxExecutors}" \
        --conf "spark.yarn.executor.memoryOverhead=3g" \
        --conf "spark.sql.shuffle.partitions={parallelism}" \
        --conf "spark.default.parallelism={parallelism}" \
        --conf "spark.executor.cores={cores}" \
        --driver-java-options "-Dlog4j.configuration=file:log4j.properties"   \
        {JAR_NAME} \
        --seedSQL "{seedSQL}" \
        --featureConfig "{featureConfig}" \
        --featureDate "{featureDate}" \
        --featureModelHDFS "{featureModelHDFS}" \
        --featureOutHDFS "{featureOutHDFS}" {fit_args}
    '''

    def __init__(self,
                 seedSQL,
                 featureConfig,
                 featureDate,
                 featureModelHDFS,
                 featureOutHDFS,
                 fit):
        fit_args = "--fit false"
        if fit is True:
            assert os.path.exists(featureConfig), \
                "input featureConfig is not exists when fit=true: " \
                + featureConfig
            fit_args = "--fit true"

        def isNullOrEmpty(seedSQL):
            assert isinstance(seedSQL, str), "输入的参数不是字符串"
            return True \
                if not seedSQL or len(seedSQL.strip()) == 0 else False

        params = {
            "YARN_QUEUE": config.YARN_QUEUE,
            "appName": ".".join([SPARK_PREFIX, featureDate]),
            "JAR_NAME": JAR_NAME,
            "MaxExecutors": config.MaxExecutors,
            "parallelism": config.parallelism,
            "cores": config.cores,
            "seedSQL": seedSQL,
            "featureConfig": featureConfig,
            "featureDate": featureDate,
            "featureModelHDFS": featureModelHDFS,
            "featureOutHDFS": featureOutHDFS,
            "fit_args": fit_args
        }
        self.cmd = self.TEMPLATE.replace("\n", " ").format(**params)


if __name__ == '__main__':
    print("[RUN]", ' '.join(sys.argv))

    parser = argparse.ArgumentParser(description='MLflow Binary Runner')

    parser.add_argument('-f',
                        '--conf',
                        help='特征合并的配置文件地址',
                        required=True)
    parser.add_argument('-s',
                        '--sql',
                        help='label人群的SQL字符串',
                        default='')
    parser.add_argument('-d',
                        '--date',
                        help='执行的特征日期',
                        default='')
    parser.add_argument('-m',
                        '--featureModelHDFS',
                        help='模型保存的HDFS地址',
                        default='')
    parser.add_argument("-o",
                        '--featureOutHDFS',
                        help='合并后的特征产出HDFS地址',
                        default='')
    parser.add_argument('--fit',
                        help='是否根据配置生成模型？否的情况会直接读取模型',
                        action='store_true')
    parser.add_argument("-v",
                        "--verbosity",
                        help="increase output verbosity",
                        action="store_true")

    opt = parser.parse_args()

    logging.debug("输入的CLI参数如下:")
    pprint.pprint(vars(opt))

    config = Config()
    logging.debug(config)

    FeatureFlowRunner(
        seedSQL=opt.sql,
        featureConfig=opt.conf,
        featureDate=opt.date,
        featureModelHDFS=opt.featureModelHDFS,
        featureOutHDFS=opt.featureOutHDFS,
        fit=opt.fit
    ).run()
