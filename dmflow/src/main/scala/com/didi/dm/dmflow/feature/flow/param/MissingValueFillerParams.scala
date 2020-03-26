package com.didi.dm.dmflow.feature.flow.param

import com.didi.dm.dmflow.base.{Json4sPlus, JsonReadable}
import org.json4s.JsonAST.JString
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render
import org.json4s.{DefaultFormats, JValue, JsonAST}

import scala.collection.mutable

case class FillnaConfig[T](default: Option[T], mapping: Map[List[String], T])

case class MissingValueFillerParams(paramMap: Map[String, Any]) extends BaseMapParams(
  paramMap = paramMap,
  requiredKeys = List[String]("excludeCols", "int", "long", "float", "double", "decimal", "str")) with Json4sPlus {

  protected val DEFAULT_VALUE = "default"

  def apply(): MissingValueFillerParams = {
    MissingValueFillerParams(paramMap)
  }

  protected implicit def castJValue(mappings: Map[String, Any]): Map[String, JValue] = {
    mappings.map { case (k, v) =>
      k match {
        case DEFAULT_VALUE => k -> JString(v.toString)
        case _ => k -> JsonAST.JArray(v.asInstanceOf[List[String]].map(f = JString))
      }
    }
  }

  override def toJValue: JValue = {

    val metadata = ("excludeCols" -> excludeCols) ~
      ("int" -> castJValue(getInt)) ~
      ("long" -> castJValue(getLong)) ~
      ("float" -> castJValue(getFloat)) ~
      ("double" -> castJValue(getDouble)) ~
      ("decimal" -> castJValue(getDecimal)) ~
      ("str" -> castJValue(getStr))

    render(metadata)
  }

  def excludeCols: List[String] = getRequiredParam[List[String]]("excludeCols")

  protected def reverseMappings[T](rawMappings: Map[String, Any], cast: String => T): FillnaConfig[T] = {

    val default: Option[T] = rawMappings.get(DEFAULT_VALUE) match {
      case Some(x) => Some(cast(x.toString))
      case None => None
    }

    val mapping = rawMappings.toSeq.filterNot(_._1 == DEFAULT_VALUE).map { case (k, v) =>
      val value = cast(k.toString)
      val cols = v.asInstanceOf[List[String]]
      (cols, value)
    }.toMap

    FillnaConfig[T](default, mapping)
  }


  def getIntMappings: FillnaConfig[Int] = reverseMappings[Int](getInt, (x: String) => x.toInt)

  def getLongMappings: FillnaConfig[Long] = reverseMappings[Long](getLong, (x: String) => x.toLong)

  def getFloatMappings: FillnaConfig[Float] = reverseMappings[Float](getFloat, (x: String) => x.toFloat)

  def getDoubleMappings: FillnaConfig[Double] = reverseMappings[Double](getDouble, (x: String) => x.toDouble)

  def getDecimalMappings: FillnaConfig[Number] = reverseMappings[Number](getDecimal, (x: String) => BigDecimal.valueOf(x.toDouble))

  def getStrMappings: FillnaConfig[String] = reverseMappings[String](getStr, (x: String) => x)

  /* 解析 json 配置 */

  protected def getInt: Map[String, Any] = getRequiredParam[Map[String, Any]]("int")

  protected def getLong: Map[String, Any] = getRequiredParam[Map[String, Any]]("long")

  protected def getFloat: Map[String, Any] = getRequiredParam[Map[String, Any]]("float")

  protected def getDouble: Map[String, Any] = getRequiredParam[Map[String, Any]]("double")

  protected def getDecimal: Map[String, Any] = getRequiredParam[Map[String, Any]]("decimal")

  protected def getStr: Map[String, Any] = getRequiredParam[Map[String, Any]]("str")

}

object MissingValueFillerParams extends JsonReadable[MissingValueFillerParams] {
  override def fromJValue(jval: JValue): MissingValueFillerParams = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val map = mutable.HashMap[String, Any]()

    //必须字段\
    map += ("excludeCols" -> (jval \ "excludeCols").extract[List[String]])
    map += ("int" -> (jval \ "int").extract[Map[String, Any]])
    map += ("long" -> (jval \ "long").extract[Map[String, Any]])
    map += ("float" -> (jval \ "float").extract[Map[String, Any]])
    map += ("double" -> (jval \ "double").extract[Map[String, Any]])
    map += ("decimal" -> (jval \ "decimal").extract[Map[String, Any]])
    map += ("str" -> (jval \ "str").extract[Map[String, Any]])

    MissingValueFillerParams(map.toMap)
  }
}

