package com.didi.dm.dmflow.feature.dependency

import java.time.LocalDate

trait TimeShiftable {

  def offsetDays: Int

  def getRedirectDate(baseDate: LocalDate): LocalDate = baseDate.plusDays(offsetDays)
}
