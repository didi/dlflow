package com.didi.dm.dmflow.feature.extractor

import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

case class ParamDesc[T: ClassTag](name: String, doc: String, required: Boolean) {

  require(name.matches("[a-z][a-zA-Z0-9]*"), s"Param name $name is invalid.")
  require(doc.nonEmpty)

  def paramTypeName: String = {
    val c = implicitly[ClassTag[T]].runtimeClass
    c match {
      case _ if c == classOf[Int] => "IntParam"
      case _ if c == classOf[Long] => "LongParam"
      case _ if c == classOf[Float] => "FloatParam"
      case _ if c == classOf[Double] => "DoubleParam"
      case _ if c == classOf[Boolean] => "BooleanParam"
      case _ if c.isArray && c.getComponentType == classOf[String] => s"StringArrayParam"
      case _ if c.isArray && c.getComponentType == classOf[Double] => s"DoubleArrayParam"
      case _ => s"Param[${getTypeString(c)}]"
    }
  }

  def valueTypeName: String = {
    val c = implicitly[ClassTag[T]].runtimeClass
    getTypeString(c)
  }

  private def getTypeString(c: Class[_]): String = {
    c match {
      case _ if c == classOf[Int] => "Int"
      case _ if c == classOf[Long] => "Long"
      case _ if c == classOf[Float] => "Float"
      case _ if c == classOf[Double] => "Double"
      case _ if c == classOf[Boolean] => "Boolean"
      case _ if c == classOf[String] => "String"
      case _ if c.isArray => s"Array[${getTypeString(c.getComponentType)}]"
    }
  }

  override def toString: String = s"$name: $valueTypeName, required=$required, desc: $doc"
}


trait IFeatureUDF {

  val paramsOfUDF: Seq[ParamDesc[_ >: String with Int with Double]]

  def usage(): String

  def validate(paramMap: Map[String, Any]): Boolean

  def invoke(paramMap: Map[String, Any], substitutions: Map[String, String]): DataFrame
}
