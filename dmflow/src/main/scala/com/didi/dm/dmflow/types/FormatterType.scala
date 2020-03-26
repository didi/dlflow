package com.didi.dm.dmflow.types

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.didi.dm.dmflow.base.{JsonReadable, JsonWritable}
import org.json4s.JValue
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._

sealed abstract class FormatterType(val pattern: String) extends JsonWritable {

  def formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)

  def format(date: LocalDate): String = date.format(formatter)

  def toJValue: JValue = "formatter" -> pattern

  def toJObject: JObject = toJValue.asInstanceOf[JObject]

  override def toString: String = s"FormatterType($pattern)"
}

object FormatterType extends JsonReadable[FormatterType] {

  val CompactDate: FormatterType = {
    val pattern = "yyyyMMdd"
    case object CompactDate extends FormatterType(pattern)
    CompactDate
  }

  val DashDate: FormatterType = {
    val pattern = "yyyy-MM-dd"
    case object DashDate extends FormatterType(pattern)
    DashDate
  }

  val SlashDate: FormatterType = {
    val pattern = "yyyy/MM/dd"
    case object SlashDate extends FormatterType(pattern)
    SlashDate
  }

  val Year: FormatterType = {
    val pattern = "yyyy"
    case object Year extends FormatterType(pattern)
    Year
  }

  val Month: FormatterType = {
    val pattern = "MM"
    case object Month extends FormatterType(pattern)
    Month
  }

  val Day: FormatterType = {
    val pattern = "dd"
    case object Day extends FormatterType(pattern)
    Day
  }

  def fromName(pattern: String): FormatterType = {
    pattern match {
      case CompactDate.pattern => CompactDate
      case DashDate.pattern => DashDate
      case SlashDate.pattern => SlashDate
      case Year.pattern => Year
      case Month.pattern => Month
      case Day.pattern => Day
      case _ => throw new IllegalArgumentException(s"Cannot recognize FormatterType $pattern")
    }
  }

  override def fromJValue(jval: JValue): FormatterType = fromName(jval.extract[String])
}