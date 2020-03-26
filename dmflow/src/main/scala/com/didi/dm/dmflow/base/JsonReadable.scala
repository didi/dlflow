package com.didi.dm.dmflow.base

import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JValue, _}

import scala.reflect.ClassTag


trait JsonReadable[T <: Object] extends Reflective with Json4sPlus {

  def fromJValue(jval: JValue): T

  def fromJson(json: String)(implicit tag: ClassTag[T]): T = {

    val metadata = parse(json)

    implicit val format: DefaultFormats.type = DefaultFormats

    val className = (metadata \ "class").extract[String]
    val params = metadata \ "paramMap"

    val expectClassName = getClassName(tag.runtimeClass)
    require(className.contains(expectClassName), s"Error loading Json: Expected class name" +
      s" $expectClassName but found class name $className")

    fromJValue(params)

  }
}
