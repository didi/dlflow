package com.didi.dm.dmflow.feature.extractor.udf

import com.didi.dm.dmflow.common.io.Templates
import com.didi.dm.dmflow.feature.extractor.{IFeatureUDF, ParamDesc}
import com.didi.dm.dmflow.spark.io.ReadWritable
import com.google.common.base.Strings
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

class HdfsReader(override val uid: String) extends IFeatureUDF with ReadWritable with Logging {

  def this() = this("HdfsReader")

  val FORMATs: Set[String] = Set[String]("parquet", "orc", "json")
  val DEFAULT_FMT = "parquet"

  override val paramsOfUDF = Seq(
    ParamDesc[String]("path", "input HDFS path", required = true),
    ParamDesc[String]("format", "input source type", required = true)
  )

  override def usage(): String =
    s""" UDFExtractor of HdfsReader usage
       |
       |Required:
       |${paramsOfUDF.filter(_.required == true).map(_.toString).mkString("\n")}
       |
       |Optional:
       |${paramsOfUDF.filter(_.required == false).map(_.toString).mkString("\n")}
       |""".stripMargin

  override def validate(paramMap: Map[String, Any]): Boolean = {
    Try {
      paramsOfUDF.filter(_.required == true).map(_.name).foreach { param =>
        require(paramMap.contains(param), s"UDFExtractor of HdfsReader, missing required argunemnts $param")
      }
      getFormat(paramMap)
      getPath(paramMap)
    } match {
      case Success(_) => true
      case Failure(e) =>
        logError(s"check argument UDFExtractor of HdfsReader failed： ${e.getMessage}")
        false
    }
  }

  @throws(classOf[IllegalArgumentException])
  def getFormat(paramMap: Map[String, Any]): String = {
    paramMap.get("format") match {
      case Some(fmt) =>
        require(FORMATs.contains(fmt.toString), s"un-support SourceType: ${fmt.toString}, only accepts：${FORMATs.mkString("[", ",", "]")}")
        fmt.toString
      case None =>
        logWarning(s"NO SourceType input, using default: $DEFAULT_FMT")
        DEFAULT_FMT
    }
  }

  @throws(classOf[IllegalArgumentException])
  def getPath(paramMap: Map[String, Any]): String = {
    paramMap.get("path") match {
      case Some(pt) =>
        require(!Strings.isNullOrEmpty(pt.toString), s"input HDFS path for loading data is null or empty")
        pt.toString
      case None => throw new IllegalArgumentException("input HDFS path is missing")
    }
  }


  override def invoke(paramMap: Map[String, Any], substitutions: Map[String, String]): DataFrame = {
    val fmt = getFormat(paramMap)
    val rawPath = getPath(paramMap)
    val fixPath = Templates.format(rawPath, substitutions)
    logDebug(s"format Path from: $rawPath to $fixPath")
    readDF(inPath = fixPath, sourceType = fmt, checkEmpty = true)
  }
}
