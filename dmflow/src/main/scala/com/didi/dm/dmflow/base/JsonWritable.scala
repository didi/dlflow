package com.didi.dm.dmflow.base

import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}


trait JsonWritable extends Reflective with Json4sPlus with Serializable {

  def toJValue: JValue

  def toJson: String = {

    val metadata = ("class" -> clazzName) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("paramMap" -> toJValue)

    compact(render(metadata))
  }

  override def toString: String = toJson

}
