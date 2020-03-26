package com.didi.dm.dmflow.spark.app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}


trait SparkInstance {
  private var optionSparkSession: Option[SparkSession] = None

  def session(sparkSession: SparkSession): this.type = {
    optionSparkSession = Option(sparkSession)
    this
  }

  protected final def sparkSession: SparkSession = {
    if (optionSparkSession.isEmpty) {
      optionSparkSession = Some(initSpark())
    }
    optionSparkSession.get
  }

  protected final def sqlContext: SQLContext = sparkSession.sqlContext

  protected final def sc: SparkContext = sparkSession.sparkContext

  protected def initSpark(): SparkSession = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hive").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .config("hive.exec.orc.default.stripe.size", 268435456L)
      .config("hive.exec.orc.split.strategy", "BI")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    spark
  }
}
