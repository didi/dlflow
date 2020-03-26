package com.didi.dm.dmflow.types

import com.didi.dm.dmflow.base.{JsonReadable, JsonWritable}
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

sealed abstract class FeatureFlowType(val name: String, val desc: String) extends JsonWritable {

  def toJValue: JValue = render("name" -> name)

  override def toString: String = s"FeatureFlowType($name -> $desc)"

}

object FeatureFlowType extends JsonReadable[FeatureFlowType] {

  val SQL_Assembler: FeatureFlowType = {
    case object SQL_Assembler extends FeatureFlowType("SQL_Assembler", "基于SQL的特征合并的step ")
    SQL_Assembler
  }

  val UDF_Assembler: FeatureFlowType = {
    case object UDF_Assembler extends FeatureFlowType("UDF_Assembler", "基于UDF的特征合并的step ")
    UDF_Assembler
  }

  val MissingValueCounter: FeatureFlowType = {
    case object MissingValueCounter extends FeatureFlowType("MissingValueCounter", "统计特征中缺失字段数作为扩展特征 ")
    MissingValueCounter
  }

  val CrossExtender: FeatureFlowType = {
    case object CrossExtender extends FeatureFlowType("CrossExtender", "一阶交叉扩展特征")
    CrossExtender
  }

  val MissingValueFiller: FeatureFlowType = {
    case object MissingValueFiller extends FeatureFlowType("MissingValueFiller", "约定俗称的缺失值填充")
    MissingValueFiller
  }

  def fromName(name: String): FeatureFlowType = {
    name match {
      case SQL_Assembler.name => SQL_Assembler
      case UDF_Assembler.name => UDF_Assembler
      case MissingValueCounter.name => MissingValueCounter
      case CrossExtender.name => CrossExtender
      case MissingValueFiller.name => MissingValueFiller
      case _ => throw new IllegalArgumentException(s"Cannot recognize FeatureFlowType $name")
    }
  }

  override def fromJValue(jval: JValue): FeatureFlowType = fromName(parseJValueStrictly[String](jval, "name"))
}
