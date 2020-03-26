package com.didi.dm.dmflow.spark.app

import java.time.{Duration, LocalDateTime}

import com.didi.dm.dmflow.common.io.IColorText
import com.didi.dm.dmflow.spark.io.ISparkIO
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import scopt.OptionParser

import scala.reflect.ClassTag


abstract class SparkAppBase[T <: Product : ClassTag] extends scala.AnyRef
  with SparkInstance
  with Logging
  with ISparkIO
  with IColorText
  with scala.Serializable {

  protected val DEFAULT_IN_REPARTITION: Int = 1000
  protected val DEFAULT_OUT_REPARTITION: Int = 10
  protected val DEFAULT_TEMP_REPARTITION: Int = 100

  def getOptParser: OptionParser[T]

  def run(param: T)

  def main(args: Array[String]): Unit = {

    val defaultConfig = implicitly[ClassTag[T]].runtimeClass.newInstance.asInstanceOf[T]

    getOptParser.parse(args, defaultConfig) match {
      case Some(cliParams) =>

        printParams(cliParams)

        val startTime = LocalDateTime.now()

        run(cliParams)

        val duration = Duration.between(startTime, LocalDateTime.now()).toMinutes
        logInfo(s"all is done, elapsed:$duration mins")

        sc.stop()

      case _ => logError(s"parse args failed! args=${args.mkString(" ")}")
    }
  }


  def printParams(param: T): Unit = {

    val values = param.productIterator
    param.getClass.getDeclaredFields.foreach { x =>
      if (values.hasNext) {
        val key = x.getName
        val field = values.next()
        val value = if (field != null) field.toString else ""
        logInfo(Yellow("SPARK APP INPUT: ") + s"$key -> $value")
      }
    }
  }

}