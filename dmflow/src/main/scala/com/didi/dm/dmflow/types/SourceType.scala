package com.didi.dm.dmflow.types

import com.didi.dm.dmflow.base.{JsonReadable, JsonWritable}
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

sealed abstract class SourceType(val name: String, val desc: String) extends JsonWritable {

  def toJValue: JValue = render("name" -> name)

  override def toString: String = s"SourceType($name -> $desc)"
}

object SourceType extends JsonReadable[SourceType] {

  val Parquet: SourceType = {
    case object Parquet extends SourceType("parquet", "parquet format")
    Parquet
  }

  val ORC: SourceType = {
    case object ORC extends SourceType("orc", "orc format")
    ORC
  }

  val JSON: SourceType = {
    case object JSON extends SourceType("json", "json format")
    JSON
  }

  def fromName(name: String): SourceType = {
    name match {
      case Parquet.name => Parquet
      case ORC.name => ORC
      case JSON.name => JSON
      case _ => throw new IllegalArgumentException(s"Cannot recognize SourceType $name")
    }
  }

  override def fromJValue(jval: JValue): SourceType = fromName(parseJValueStrictly[String](jval, "name"))

}