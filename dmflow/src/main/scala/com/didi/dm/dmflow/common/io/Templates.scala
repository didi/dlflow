package com.didi.dm.dmflow.common.io

import java.time.LocalDate

import com.didi.dm.dmflow.common.time.Formatters
import org.slf4j.LoggerFactory


object Templates {

  final val LOGGER = LoggerFactory.getLogger(Templates.getClass)

  val LEFT_TAG: String = "${"
  val RIGHT_TAG = "}"

  @throws(classOf[IllegalArgumentException])
  def formatPath(pathTemplate: String, date: LocalDate): String = {
    require(!pathTemplate.isEmpty, "pathTemplate is null or empty")

    val placeholderMap = Map[String, String](
      "yyyy" -> date.format(Formatters.yearFmt),
      "mm" -> date.format(Formatters.monthFmt),
      "dd" -> date.format(Formatters.dayFmt),
      "YYYY" -> date.format(Formatters.yearFmt),
      "MM" -> date.format(Formatters.monthFmt),
      "DD" -> date.format(Formatters.dayFmt)
    )
    val path = replace(pathTemplate, placeholderMap)
    checkPlaceholders(path, List[String]())

    path
  }

  @throws(classOf[IllegalArgumentException])
  def format(template: String,
             placeholderMap: Map[String, String],
             isCheck: Boolean = true,
             whitelist: List[String] = List[String]()
            ): String = {

    val whitelistKeys = whitelist
      .map(_.replace(LEFT_TAG, ""))
      .map(_.replace(RIGHT_TAG, ""))
      .toSet

    val cleanMaps = placeholderMap.filter(x => !whitelistKeys.contains(x._1))

    val result = replace(template, cleanMaps)

    if (isCheck) checkPlaceholders(result, whitelist)

    result
  }

  @throws(classOf[IllegalArgumentException])
  protected def checkPlaceholders(needCheckStr: String, whitelist: List[String]): Unit = {

    val preCheckStr = if (whitelist.nonEmpty) {
      whitelist.foldLeft(needCheckStr)((str: String, whiteTerm) => str.replace(whiteTerm, "WHITE_TAGs"))
    } else {
      needCheckStr
    }

    require(preCheckStr.indexOf(LEFT_TAG) == -1, s"some placeholders are not replaced!, e.g. ${getFirstPlaceholder(preCheckStr)}, template=$preCheckStr")
  }

  protected def replace(template: String,
                        substitutions: Map[String, String],
                        leftTag: String = LEFT_TAG,
                        rightTag: String = RIGHT_TAG
                       ): String = {
    substitutions.foldLeft(template)((tmpl: String, kv: (String, String)) => tmpl.replace(leftTag + kv._1 + rightTag, kv._2))
  }


  protected def getFirstPlaceholder(templateStr: String): String = {

    val startOffset = templateStr.indexOf(LEFT_TAG)
    val endOffset = templateStr.indexOf(RIGHT_TAG, startOffset)

    if (startOffset >= 0 && endOffset >= startOffset) {
      templateStr.substring(startOffset, endOffset + 1)
    } else {
      s"<notFind: ${LEFT_TAG}XXX$RIGHT_TAG>"
    }
  }

}
