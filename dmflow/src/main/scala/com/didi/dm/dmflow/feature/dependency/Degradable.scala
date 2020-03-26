package com.didi.dm.dmflow.feature.dependency

import java.time.LocalDate

import com.didi.dm.dmflow.common.io.{IColorText, Templates}
import com.didi.dm.dmflow.common.time.Formatters
import org.apache.spark.internal.Logging

trait Degradable extends Logging with IColorText {

  def maxDegradeDays: Int

  def tryDegrade(baseDate: LocalDate, pathTemplate: String, checker: Checker): Option[LocalDate] = {
    require(maxDegradeDays >= 0, s"maxDegradeDays must greater equal Zero, now=$maxDegradeDays")

    logInfo(Green("[Degrade] ") + s"try to degrade date=${baseDate.format(Formatters.ymdCompactFmt)} HDFS=$pathTemplate")

    var tryCount: Int = 0
    var tryDate: LocalDate = baseDate
    var tryPath = ""

    while (tryCount <= maxDegradeDays) {

      tryPath = Templates.formatPath(pathTemplate, tryDate)

      if (checker.check(tryPath)) {
        logInfo(s" find degrade path (baseDate=${baseDate.toString}, try=$tryCount), Ret:$tryPath")
        return Some(tryDate)
      }

      tryDate = tryDate.minusDays(1)
      tryCount += 1
    }

    logError(s" degrade failed! startDate=${baseDate.toString} maxTry=$maxDegradeDays lastPt=$tryPath")
    None
  }
}
