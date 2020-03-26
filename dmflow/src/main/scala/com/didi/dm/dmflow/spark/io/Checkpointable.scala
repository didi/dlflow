package com.didi.dm.dmflow.spark.io

import com.didi.dm.dmflow.common.hdfs.HdfsUtility
import com.didi.dm.dmflow.common.io.IColorText
import com.didi.dm.dmflow.param.HasCheckpoint
import com.google.common.base.Strings
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

trait Checkpointable extends HasCheckpoint with Cleanable with ReadWritable with Logging with IColorText {

  protected val DEFAULT_CKP_FMT = "parquet"

  override def setCheckpoint(value: String): this.type = {
    require(!Strings.isNullOrEmpty(value), "set checkpointParam but path is empty")
    set(checkpointParam, Some(value))
  }


  def trySaveCheckpoint(df: DataFrame): DataFrame = {

    getCheckpoint match {
      case Some(pt) =>
        logInfo(Yellow("[Checkpoint] ") + s"OPEN checkpoint，will save data on HDFS: $pt")
        writeDF(df, pt, sourceType = DEFAULT_CKP_FMT, overwrite = true, checkEmpty = true)
        readDF(pt, sourceType = DEFAULT_CKP_FMT, checkEmpty = false)
      case None =>
        logDebug(s"NOT set checkpoint，return original dataframe")
        df
    }

  }

  def loadCheckpoint(): DataFrame = {
    getCheckpoint match {
      case Some(pt) => readDF(pt, checkEmpty = true)
      case None => throw new RuntimeException(s"try to load checkpoint but data not exists, please SET checkpointParam at first")
    }
  }

  override def clear(): Unit = {
    if (getCheckpoint.isDefined) {
      val pt = getCheckpoint.get
      logInfo(Yellow("[Checkpoint] ") + s"clear checkpoint：$pt")
      HdfsUtility.del(pt)
    }
  }

}
