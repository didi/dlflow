package com.didi.dm.dmflow.common.time

import java.time.format.DateTimeFormatter

object Formatters {

  final val ymdCompactFmt = DateTimeFormatter.ofPattern("yyyyMMdd")
  final val ymdDashFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  final val ymdSlashFmt = DateTimeFormatter.ofPattern("yyyy/MM/dd")
  final val yearFmt = DateTimeFormatter.ofPattern("yyyy")
  final val monthFmt = DateTimeFormatter.ofPattern("MM")
  final val dayFmt = DateTimeFormatter.ofPattern("dd")

  final val timeZoneFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssz")

}
