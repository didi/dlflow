package com.didi.dm.dmflow.feature.flow.param

import com.didi.dm.dmflow.base.{Json4sPlus, JsonReadable}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render
import org.json4s.{DefaultFormats, JValue}

case class SQLAssemblerParams(paramMap: Map[String, Any]) extends BaseMapParams(
  paramMap = paramMap,
  requiredKeys = List[String]("joinKeys", "sqlFile", "substitutions")) with Json4sPlus {

  def apply(): SQLAssemblerParams = new SQLAssemblerParams(paramMap)

  override def toJValue: JValue = {

    var metadata = ("joinKeys" -> joinKeys) ~
      ("sqlFile" -> sqlFile) ~
      ("substitutions" -> substitutions)

    if (hasKey("enableSubdir")) metadata = metadata ~ ("enableSubdir" -> enableSubdir)
    if (hasKey("emptyCheck")) metadata = metadata ~ ("emptyCheck" -> emptyCheck)
    if (hasKey("primaryKeys")) metadata = metadata ~ ("primaryKeys" -> primaryKeys.get)
    if (hasKey("uniqueCheck")) metadata = metadata ~ ("uniqueCheck" -> uniqueCheck.get)


    render(metadata)
  }

  def joinKeys: List[String] = getRequiredParam[List[String]]("joinKeys")

  def sqlFile: String = getRequiredParam[String]("sqlFile")

  def substitutions: List[String] = getRequiredParam[List[String]]("substitutions")

  def enableSubdir: Boolean = getParamWithDefault[Boolean]("enableSubdir", false)

  def emptyCheck: Boolean = getParamWithDefault[Boolean]("emptyCheck", true)

  def primaryKeys: Option[List[String]] = getOptionalParam[List[String]]("primaryKeys")

  def uniqueCheck: Option[Boolean] = getOptionalParam[Boolean]("uniqueCheck")


}

object SQLAssemblerParams extends JsonReadable[SQLAssemblerParams] {
  override def fromJValue(jval: JValue): SQLAssemblerParams = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val map = scala.collection.mutable.HashMap[String, Any]()

    map += ("joinKeys" -> (jval \ "joinKeys").extract[List[String]])
    map += ("sqlFile" -> (jval \ "sqlFile").extract[String])
    map += ("substitutions" -> (jval \ "substitutions").extract[List[String]])

    if (jval.has("enableSubdir")) map += ("enableSubdir" -> (jval \ "enableSubdir").extract[Boolean])
    if (jval.has("emptyCheck")) map += ("emptyCheck" -> (jval \ "emptyCheck").extract[Boolean])
    if (jval.has("primaryKeys")) map += ("primaryKeys" -> (jval \ "primaryKeys").extract[List[String]])
    if (jval.has("uniqueCheck")) map += ("uniqueCheck" -> (jval \ "uniqueCheck").extract[Boolean])

    SQLAssemblerParams(map.toMap)
  }
}