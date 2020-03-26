package com.didi.dm.dmflow.feature.flow.param

import com.didi.dm.dmflow.base.JsonReadable
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render
import org.json4s.{DefaultFormats, JValue}

case class UDFAssemblerParams(paramMap: Map[String, Any]) extends BaseMapParams(
  paramMap = paramMap,
  requiredKeys = List[String]("joinKeys", "clazz", "extraParams", "substitutions")
) {


  override def toJValue: JValue = {

    var metadata = ("joinKeys" -> joinKeys) ~
      ("clazz" -> clazzOfUDF) ~
      ("extraParams" -> extraParams) ~
      ("substitutions" -> substitutions)

    if (hasKey("emptyCheck")) metadata = metadata ~ ("emptyCheck" -> emptyCheck)
    if (hasKey("primaryKeys")) metadata = metadata ~ ("primaryKeys" -> primaryKeys.get)
    if (hasKey("uniqueCheck")) metadata = metadata ~ ("uniqueCheck" -> uniqueCheck.get)

    render(metadata)
  }

  def joinKeys: List[String] = getRequiredParam[List[String]]("joinKeys")

  def clazzOfUDF: String = getRequiredParam[String]("clazz")

  def extraParams: Map[String, String] = getRequiredParam[Map[String, String]]("extraParams")

  def substitutions: List[String] = getRequiredParam[List[String]]("substitutions")

  def emptyCheck: Boolean = getParamWithDefault[Boolean]("emptyCheck", true)

  def primaryKeys: Option[List[String]] = getOptionalParam[List[String]]("primaryKeys")

  def uniqueCheck: Option[Boolean] = getOptionalParam[Boolean]("uniqueCheck")
}

object UDFAssemblerParams extends JsonReadable[UDFAssemblerParams] {
  override def fromJValue(jval: JValue): UDFAssemblerParams = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val map = scala.collection.mutable.HashMap[String, Any]()

    map += ("joinKeys" -> (jval \ "joinKeys").extract[List[String]])
    map += ("clazz" -> (jval \ "clazz").extract[String])
    map += ("extraParams" -> (jval \ "extraParams").extract[Map[String, Any]])
    map += ("substitutions" -> (jval \ "substitutions").extract[List[String]])

    if (jval.has("emptyCheck")) map += ("emptyCheck" -> (jval \ "emptyCheck").extract[Boolean])
    if (jval.has("primaryKeys")) map += ("primaryKeys" -> (jval \ "primaryKeys").extract[List[String]])
    if (jval.has("uniqueCheck")) map += ("uniqueCheck" -> (jval \ "uniqueCheck").extract[Boolean])

    UDFAssemblerParams(map.toMap)
  }
}