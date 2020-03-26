package com.didi.dm.dmflow.feature.flow.param

import com.didi.dm.dmflow.base.{Json4sPlus, JsonReadable}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render
import org.json4s.{DefaultFormats, JValue}

import scala.collection.mutable

case class MissingValueCounterParams(paramMap: Map[String, Any]) extends BaseMapParams(
  paramMap = paramMap,
  requiredKeys = List[String]("excludeCols")) with Json4sPlus {

  def apply(): MissingValueCounterParams = new MissingValueCounterParams(paramMap)

  override def toJValue: JValue = {
    render("excludeCols" -> excludeCols)
  }

  def excludeCols: List[String] = getRequiredParam[List[String]]("excludeCols")

}

object MissingValueCounterParams extends JsonReadable[MissingValueCounterParams] {
  override def fromJValue(jval: JValue): MissingValueCounterParams = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val map = mutable.HashMap[String, Any]()

    map += ("excludeCols" -> (jval \ "excludeCols").extract[List[String]])

    MissingValueCounterParams(map.toMap)
  }
}