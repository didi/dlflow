package com.didi.dm.dmflow.feature.dependency

import java.time.LocalDate

trait Dependent {

  def key: String

  def getDependency(bizDate: LocalDate): String

  def getKvPair(bizDate: LocalDate): (String, String) = Tuple2(key, getDependency(bizDate))
}
