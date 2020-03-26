package com.didi.dm.dmflow.types

import com.didi.dm.dmflow.base.{JsonReadable, JsonWritable}
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render


sealed abstract class CheckerMethod(val name: String, val desc: String) extends JsonWritable {

  def toJValue: JValue = render("method" -> name)

  override def toString: String = s"CheckerMethod($name -> $desc)"
}

object CheckerMethod extends JsonReadable[CheckerMethod] {

  val _SUCCESS: CheckerMethod = {
    case object _SUCCESS extends CheckerMethod("_SUCCESS", "by hdfs _SUCCESS file")
    _SUCCESS
  }

  val Capacity: CheckerMethod = {
    case object Capacity extends CheckerMethod("capacity", "by hdfs capacity greater equal threshold")
    Capacity
  }

  val Count: CheckerMethod = {
    case object Count extends CheckerMethod("count", "by dataframe size greater equal threshold（MB）")
    Count
  }

  def fromName(name: String): CheckerMethod = {
    name match {
      case _SUCCESS.name => _SUCCESS
      case Capacity.name => Capacity
      case Count.name => Count
      case _ => throw new IllegalArgumentException(s"Cannot recognize CheckerType $name")
    }
  }

  override def fromJValue(jval: JValue): CheckerMethod = fromName(parseJValueStrictly[String](jval, "method"))

}

