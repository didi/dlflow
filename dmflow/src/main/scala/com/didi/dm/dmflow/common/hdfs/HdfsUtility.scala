package com.didi.dm.dmflow.common.hdfs

import java.io.IOException
import java.nio.file.{Files, Paths}
import java.time.LocalDate

import com.didi.dm.dmflow.common.io.{IColorText, Templates}
import com.google.common.base.Strings
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging


object HdfsUtility extends Logging with IColorText {

  protected def getFileSystem: FileSystem = FileSystem.get(new Configuration())

  def getCapacity(path: String): Long = getFileSystem.getContentSummary(new Path(path)).getLength

  def checkCapacity(path: String, minMB: Double = 10.0): Boolean = {
    try {
      val mb = getCapacity(path).toDouble / (1024 * 1024).toDouble
      logDebug(s"check HDFS capacity=${mb.formatted("%.2f")}MB (by minThreshold:${minMB.formatted("%.2f")}MB), pt=$path")
      mb > minMB
    } catch {
      case _: Throwable => false
    }
  }

  def checkSuccess(path: String): Boolean = {
    val tagPath = if (path.endsWith("/")) {
      path + "_SUCCESS"
    } else {
      path + "/" + "_SUCCESS"
    }
    exists(tagPath)
  }

  def exists(hdfsPath: String): Boolean = {
    require(!Strings.isNullOrEmpty(hdfsPath), "input hdfs path for checking is null or empty")

    var isExists = false
    var fs: FileSystem = null

    try {
      fs = getFileSystem
      isExists = fs.exists(new Path(hdfsPath))
      logDebug(s"checking HDFS (isExists=$isExists) path: $hdfsPath")
    } catch {
      case ex: IOException => throw new RuntimeException("check HDFS file faild! ", ex)
    }
    finally {
      if (fs != null) fs.close()
    }

    isExists
  }

  def del(hdfsPath: String): Unit = {
    require(!Strings.isNullOrEmpty(hdfsPath), "input hdfs path for deleting is null or empty")
    if (exists(hdfsPath)) {
      var fs: FileSystem = null
      try {
        fs = getFileSystem
        fs.delete(new Path(hdfsPath), true)
        logDebug(Red("[HDFS] ") + s"Delete hdfs path:$hdfsPath")
      } catch {
        case ex: IOException => throw new RuntimeException("delete HDFS file faild! ", ex)
      }
      finally {
        if (fs != null) fs.close()
      }
    } else {
      logWarning(s"HDFS path not exists for deleting, path=$hdfsPath")
    }
  }

  def copyFromLocal(srcLocalPath: String, destHdfsPath: String): Unit = {
    require(Files.exists(Paths.get(srcLocalPath)), s"HDFS copyFromLocal but srcLocalPath not exists: $srcLocalPath")

    var fs: FileSystem = null
    try {
      fs = getFileSystem
      fs.copyFromLocalFile(new Path(srcLocalPath), new Path(destHdfsPath))
      logDebug(s"HDFS copyFromLocalFile src: $srcLocalPath dest: $destHdfsPath")
    } catch {
      case ex: IOException => throw new RuntimeException("HDFS copyFromLocal faild! ", ex)
    }
    finally {
      if (fs != null) fs.close()
    }

  }

  def copyToLocalFile(srcHdfsPath: String, destLocalPath: String): Unit = {
    require(exists(srcHdfsPath), s"HDFS copyToLocalFile but srcHdfsPath not exists: $srcHdfsPath")

    var fs: FileSystem = null
    try {
      fs = getFileSystem
      fs.copyToLocalFile(new Path(srcHdfsPath), new Path(destLocalPath))
      logDebug(s"HDFS copyToLocalFile src: $srcHdfsPath dest: $destLocalPath")
    } catch {
      case ex: IOException => throw new RuntimeException("HDFS copyToLocalFile faild! ", ex)
    }
    finally {
      if (fs != null) fs.close()
    }

  }

  case class DegradeRet(hdfs: String, date: LocalDate)

  def degrade(hdfsTemplate: String, baseDate: LocalDate, maxDegradeDays: Int): Option[DegradeRet] = {

    var tryDate = baseDate
    val earliestDate = baseDate.minusDays(maxDegradeDays)
    var degradeRet: Option[DegradeRet] = None

    var tryPath = ""
    var accumTry = 0
    var fs: FileSystem = null

    try {

      fs = getFileSystem

      while (degradeRet.isEmpty && accumTry < maxDegradeDays && tryDate.isAfter(earliestDate)) {

        tryPath = Templates.formatPath(hdfsTemplate, tryDate)
        logDebug(s"try degrade HDFS: $tryPath")

        if (fs.exists(new Path(tryPath))) {
          degradeRet = Some(DegradeRet(tryPath, tryDate))
        } else {
          None
        }

        tryDate = baseDate.minusDays(accumTry)
        accumTry += 1

      }

    } catch {
      case ex: IOException => throw new RuntimeException("try degradeHDFS IO faild! ", ex)
    }
    finally {
      if (fs != null) fs.close()
    }

    logInfo(s"""degrade HDFS complete! isFind=${degradeRet.isDefined} offset: -$accumTry""")

    degradeRet
  }

}
