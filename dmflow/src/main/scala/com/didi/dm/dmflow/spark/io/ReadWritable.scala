package com.didi.dm.dmflow.spark.io

import com.didi.dm.dmflow.common.hdfs.HdfsUtility
import com.didi.dm.dmflow.param.{HasEmptyCheck, HasHdfsSource, HasOverwrite}
import com.didi.dm.dmflow.spark.app.SparkInstance
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}


@throws(classOf[RuntimeException])
trait ReadWritable extends SparkInstance with HasHdfsSource with HasOverwrite with HasEmptyCheck with Logging {

  override def copy(extra: ParamMap): Params = defaultCopy(extra)

  def writeDF(df: DataFrame, outPath: String): Unit = writeDF(df, outPath, getHdfsSource, isOverwrite, isCheckEmpty): Unit

  def writeDF(df: DataFrame, outPath: String, overwrite: Boolean): Unit = writeDF(df, outPath, getHdfsSource, overwrite, isCheckEmpty): Unit

  def writeDF(df: DataFrame, outPath: String, sourceType: String, overwrite: Boolean): Unit = writeDF(df, outPath, sourceType, overwrite, isCheckEmpty): Unit

  def writeDF(df: DataFrame, outPath: String, sourceType: String, overwrite: Boolean, checkEmpty: Boolean): Unit = {

    if (HdfsUtility.checkCapacity(outPath, 0)) {
      if (overwrite) {
        logWarning(s"write HDFS and SET overwrite=true，old data will be delete! path: $outPath")
        HdfsUtility.del(outPath)
      } else {
        throw new RuntimeException(s"output Path already exists, SET overwrite=true or delete it, path: $outPath")
      }
    }

    logDebug(s"write HDFS (sourceType=$sourceType), dumpPath=$outPath ...")
    import CompressDataFrameFunctions._
    df.saveCompressedParquet(outPath, sourceType)

    if (checkEmpty) {
      val checkDF = readDF(outPath, sourceType)
      if(checkDF.count() == 0) {
        HdfsUtility.del(outPath)
        throw new RuntimeException(s"saved dataframe is empty, please check upstream data, clear error data on HDFS: $outPath")
      }
    }
  }

  def readDF(inPath: String): DataFrame = readDF(inPath, getHdfsSource, isCheckEmpty)

  def readDF(inPath: String, checkEmpty: Boolean): DataFrame = readDF(inPath, getHdfsSource, checkEmpty)

  def readDF(inPath: String, sourceType: String): DataFrame = readDF(inPath, sourceType, isCheckEmpty)

  def readDF(inPath: String, sourceType: String, checkEmpty: Boolean): DataFrame = {
    logDebug(s"begin to read HDFS (sourceType=$sourceType), path=$inPath ...")
    val df = sparkSession.read.format(sourceType).load(inPath)
    if (checkEmpty) {
      require(df.count() > 0, s"data on HDFS is emtpy！" +
        s"path: $inPath")
    }
    df
  }

}
