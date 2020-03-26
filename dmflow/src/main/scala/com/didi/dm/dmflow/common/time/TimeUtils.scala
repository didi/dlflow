package com.didi.dm.dmflow.common.time

import java.time.{Duration, LocalDate, LocalDateTime}

import org.slf4j.LoggerFactory

import scala.util.matching.Regex


object TimeUtils {

  val PATTERN_DASH_DATE: Regex = "^[1-9]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])$".r
  val PATTERN_COMPACT_DATE: Regex = "^[1-9]\\d{3}(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])$".r
  val PATTERN_SLASH_DATE: Regex = "^[1-9]\\d{3}/(0[1-9]|1[0-2])/(0[1-9]|[1-2][0-9]|3[0-1])$".r

  final val LOGGER = LoggerFactory.getLogger(TimeUtils.getClass)

  def isAfterOrEqual(curDate: LocalDate, targetDate: LocalDate): Boolean = {
    curDate.isAfter(targetDate) || curDate.isEqual(targetDate)
  }

  def isBeforeOrEqual(curDate: LocalDate, targetDate: LocalDate): Boolean = {
    curDate.isBefore(targetDate) || curDate.isEqual(targetDate)
  }

  @throws(classOf[IllegalArgumentException])
  def parseLocalDate(date: String): LocalDate = {
    if ((PATTERN_DASH_DATE findFirstIn date).isDefined) {
      LocalDate.parse(date, Formatters.ymdDashFmt)
    } else if ((PATTERN_COMPACT_DATE findFirstIn date).isDefined) {
      LocalDate.parse(date, Formatters.ymdCompactFmt)
    } else if ((PATTERN_SLASH_DATE findFirstIn date).isDefined) {
      LocalDate.parse(date, Formatters.ymdSlashFmt)
    } else {
      throw new IllegalArgumentException(s"unknow date format: $date, only accept: YYYYMMDD, YYYY-MM-DD, YYYY/MM/DD")
    }
  }

  def timeit[T](func: => T, stepName: String = ""): T = {

    val startTime = LocalDateTime.now()

    val result = func

    val duration = Duration.between(startTime, LocalDateTime.now())

    val funcName = if (stepName.isEmpty) {
      "[unNamed Function]"
    } else {
      s"[$stepName]"
    }

    LOGGER.info(s"$funcName is DONE, elapsed: ${duration.toMinutes} mins (${duration.toMinutes} ms)")

    result
  }


  case class Timer() {

    val DEFAULT_TIMER = "default"

    case class DurationUnit(var startTs: LocalDateTime, var endTs: Option[LocalDateTime])

    private val buff = scala.collection.mutable.HashMap[String, DurationUnit]()

    def start(recordName: String = DEFAULT_TIMER): this.type = {
      this.buff += (recordName -> DurationUnit(LocalDateTime.now(), None))
      this
    }

    def end(recordName: String = DEFAULT_TIMER): Duration = {
      this.buff.get(recordName) match {
        case Some(durationUnit) =>
          durationUnit.endTs = Some(LocalDateTime.now())
          this.buff += (recordName -> durationUnit)
          Duration.between(durationUnit.startTs, durationUnit.endTs.get)
        case None => throw new RuntimeException(s"unknow Timer end before start: $recordName")
      }
    }
  }

}
