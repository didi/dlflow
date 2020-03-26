package com.didi.dm.dmflow.spark.io

import java.time.LocalDate

import com.didi.dm.dmflow.common.io.Templates
import com.didi.dm.dmflow.common.time.TimeUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer


trait ISparkIO extends Logging {

  @throws(classOf[RuntimeException])
  def loadRangeParquets(spark: SparkSession, startDate: LocalDate, endDate: LocalDate, pathTemplate: String, withCheck: Boolean = false): DataFrame = {
    loadRangeParquetsByCols(spark, startDate, endDate, pathTemplate, None, withCheck)
  }


  @throws(classOf[RuntimeException])
  def loadRangeParquetsByCols(spark: SparkSession,
                              startDate: LocalDate,
                              endDate: LocalDate,
                              pathTemplate: String,
                              useCols: Option[List[String]],
                              withCheck: Boolean = false): DataFrame = {

    require(startDate.isBefore(endDate), s"startDate=$startDate must early than endDate=$endDate")

    var execDt = startDate
    val buff = ArrayBuffer[String]()
    while (TimeUtils.isBeforeOrEqual(execDt, endDate)) {
      val path = Templates.formatPath(pathTemplate, execDt)
      buff += path
      execDt = execDt.plusDays(1)
    }

    logInfo(s"prepare to load range data, start=$startDate end=$endDate prefix=$pathTemplate in parallel ...")
    buff.par.map { path =>

      val df = spark.read.parquet(path)

      useCols match {
        case Some(cols) =>
          val matchCols = cols.intersect(df.columns.toList)
          require(matchCols.size == cols.size, s"some columns is missing in sub-part, required columns=<${cols.mkString(",")}> path=$path")
          df.selectExpr(cols.toArray: _*)
        case None => return df
      }

      val size = df.count()
      if (size > 0) {
        logInfo(s"loadRangeData sub-part size=$size path=$path")
        Some(df)
      } else {
        logInfo(s"loadRangeData sub-part not exists: $path")
        if (withCheck) {
          throw new RuntimeException(s"loadRangeData with check, but some sub-part not exists: $path")
        } else {
          None
        }
      }
    }.toList
      .filter(_.isDefined).map(_.get)
      .reduce((x, y) => x.union(y))
  }
}