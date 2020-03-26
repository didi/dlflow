package com.didi.dm.dmflow.feature.dependency

import com.didi.dm.dmflow.base.{JsonReadable, JsonWritable}
import com.didi.dm.dmflow.common.hdfs.HdfsUtility
import com.didi.dm.dmflow.common.io.IColorText
import com.didi.dm.dmflow.spark.app.SparkInstance
import com.didi.dm.dmflow.types.{CheckerMethod, SourceType}
import org.apache.spark.internal.Logging
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render
import org.json4s.{DefaultFormats, JValue}

import scala.util.{Failure, Success, Try}

case class Checker(var method: CheckerMethod,
                   var threshold: Double = 0.0,
                   var sourceType: SourceType = SourceType.Parquet
                  ) extends SparkInstance with JsonWritable with IColorText with Logging {

  def validateComposite(): Boolean = {
    if (method == CheckerMethod.Count && sourceType == null) {
      false
    } else {
      true
    }
  }

  def check(hdfs: String): Boolean = {
    logInfo(s" check HDFS (method=${method.name}, threshold=$threshold sourceType=${sourceType.name}) basePath:$hdfs ...")
    method match {
      case CheckerMethod._SUCCESS => HdfsUtility.checkSuccess(hdfs)
      case CheckerMethod.Capacity => HdfsUtility.checkCapacity(hdfs, threshold)
      case CheckerMethod.Count =>
        Try {
          sparkSession.read.format(sourceType.name).load(hdfs).count()
        } match {
          case Success(cnt) => cnt >= threshold
          case Failure(ex) =>
            logError(s"read dataframe from HDFS failed! sourceType=$sourceType, path=$hdfs, msg=${ex.getMessage}")
            false
        }
      case _ => throw new RuntimeException(s"unknow CheckerType: ${method.toString}")
    }
  }

  override def toJValue: JValue = {
    val metadata = ("method" -> method.name) ~ ("threshold" -> threshold) ~ ("sourceType" -> sourceType.name)
    render(metadata)
  }
}

object Checker extends JsonReadable[Checker] {

  val DEFAULT_CHECK_TYPE: CheckerMethod = CheckerMethod._SUCCESS
  val DEFAULT_CHECK_THRESHOLD = 0.0
  val DEFAULT_CHECK_SOURCE: SourceType = SourceType.Parquet

  def getDefaultChecker: Checker = Checker(
    method = DEFAULT_CHECK_TYPE,
    threshold = DEFAULT_CHECK_THRESHOLD,
    sourceType = DEFAULT_CHECK_SOURCE
  )

  override def fromJValue(json: JValue): Checker = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val method = (json \ "method").extract[String]
    val threshold = (json \ "threshold").extract[Double]
    val sourceType = (json \ "sourceType").extract[String]

    Checker(
      method = CheckerMethod.fromName(method),
      threshold = threshold,
      sourceType = SourceType.fromName(sourceType)
    )
  }
}
