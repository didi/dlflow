package com.didi.dm.dmflow.spark.io

import com.didi.dm.dmflow.common.hdfs.HdfsUtility
import com.didi.dm.dmflow.common.io.IColorText
import com.didi.dm.dmflow.common.time.TimeUtils.Timer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

class CompressDataFrameFunctions(df: DataFrame) extends Logging with IColorText {

  val DEFAULT_HDFS_TEMP_SUFFIX = ".compress.tmp"
  val DEFAULT_BLOCK_SIZE_MB = 512

  val fs: FileSystem = FileSystem.get(df.rdd.sparkContext.hadoopConfiguration)

  def saveCompressedParquet(hdfs: String,
                            sourceType: String = "parquet",
                            mbPerPartition: Int = DEFAULT_BLOCK_SIZE_MB,
                            compressSuffix: String = DEFAULT_HDFS_TEMP_SUFFIX): Unit = {

    val compressPath = hdfs + compressSuffix
    logDebug(s"Doing saveCompressedParquet with temp path: $compressPath")

    if (HdfsUtility.exists(compressPath)) {
      logDebug(s"compress hdfs but it already exists, del firstly: $compressPath")
      HdfsUtility.del(compressPath)
    }

    val timer = Timer().start()

    df.write.mode(SaveMode.Overwrite).parquet(compressPath)
    logDebug(s"compress hdfs tmp save: $compressPath")

    try {

      val fileNum = calPartitionNum(compressPath, mbPerPartition)

      df.sqlContext.read.parquet(compressPath)
        .repartition(fileNum)
        .write
        .mode(SaveMode.Overwrite)
        .format(sourceType)
        .save(hdfs)

      logDebug(s"saveCompressedParquet complete (elapsed: ${timer.end().toMinutes}mins, parts: $fileNum) dump: $hdfs")
    } finally {
      HdfsUtility.del(compressPath)
    }

  }

  private def calPartitionNum(pathName: String, sizePerFile: Int = 512): Int = {
    val path = new Path(pathName)
    val fileNum = fs.getContentSummary(path).getLength / (sizePerFile * 1024 * 1024) // 512M per file
    if (fileNum > 0) fileNum.toInt else 1
  }
}

object CompressDataFrameFunctions {

  implicit def toCompressDataFrameFunctions(df: DataFrame): CompressDataFrameFunctions =
    new CompressDataFrameFunctions(df)

}
