package com.didi.dm.dmflow.types

import com.didi.dm.dmflow.base.{JsonReadable, JsonWritable}
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render


sealed abstract class DependencyType(val name: String, val desc: String) extends JsonWritable {

  def toJValue: JValue = render("name" -> name)

  override def toString: String = s"DependencyType($name -> $desc)"
}

object DependencyType extends JsonReadable[DependencyType] {

  val Hive: DependencyType = {
    case object Hive extends DependencyType("hive", "Placeholder 返回日期")
    Hive
  }

  val HDFS: DependencyType = {
    case object Parquet extends DependencyType("hdfs", "Placeholder 返回有效的hdfs地址")
    Parquet
  }

  val Date: DependencyType = {
    case object Date extends DependencyType("date", "Placeholder 日期字符串")
    Date
  }

  val Static: DependencyType = {
    case object Static extends DependencyType("static", "Placeholder 返回固定的值")
    Static
  }


  def fromName(name: String): DependencyType = {
    name match {
      case Hive.name => Hive
      case HDFS.name => HDFS
      case Date.name => Date
      case Static.name => Static
      case _ => throw new IllegalArgumentException(s"Cannot recognize type $name")
    }
  }

  override def fromJValue(jval: JValue): DependencyType = fromName(parseJValueStrictly[String](jval, "name"))
}