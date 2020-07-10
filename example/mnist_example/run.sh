#!/bin/bash

spark-submit \
    --master "" \
    --queue "" \
    --name "" \
    --driver-memory 4g \
    --executor-memory 10g \
    --archives "" \
    --conf "spark.submit.deployMode=" \
    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=" \
    --conf "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=" \
    --conf "spark.dynamicAllocation.minExecutors=200" \
    --conf "spark.dynamicAllocation.maxExecutors=400" \
    --conf "spark.executor.memoryOverhead=3g" \
    --conf "spark.sql.shuffle.partitions=10" \
    --conf "spark.default.parallelism=10" \
    --conf "spark.executor.cores=3" \
    --conf "spark.yarn.dist.archives=" \
    --conf "spark.jars=some path/tensorflow-hadoop-1.12.0.jar,some path/spark-tensorflow-connector_2.11-1.12.0.jar" \
    --files ./mnist.conf \
    --py-files ./model/model.py,./model/mnist_classifier.py \
    run.py
    