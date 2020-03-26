package com.didi.dm.dmflow.param


import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.didi.dm.dmflow.common.cast.TypeConverters
import com.didi.dm.dmflow.common.time.Formatters
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import org.json4s.{DefaultFormats, JArray, NoTypeHints, _}

class DateParam(parent: String, name: String, doc: String, isValid: LocalDate => Boolean)
  extends Param[LocalDate](parent, name, doc, isValid) {

  val DEFAULT_FMT: DateTimeFormatter = Formatters.ymdDashFmt

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, DMParamValidators.alwaysTrue[LocalDate])

  def this(parent: Identifiable, name: String, doc: String, isValid: LocalDate => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: LocalDate): ParamPair[LocalDate] = super.w(value)

  override def jsonEncode(value: LocalDate): String = {
    compact(render(JString(value.format(DEFAULT_FMT))))
  }

  override def jsonDecode(json: String): LocalDate = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    LocalDate.parse(parse(json).extract[String], DEFAULT_FMT)
  }
}


class OptStringParam(parent: String, name: String, doc: String, isValid: Option[String] => Boolean)
  extends Param[Option[String]](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, DMParamValidators.alwaysTrue[Option[String]])

  def this(parent: Identifiable, name: String, doc: String, isValid: Option[String] => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Option[String]): ParamPair[Option[String]] = super.w(value)

  override def jsonEncode(value: Option[String]): String = {
    compact(render(JString(value.getOrElse("None"))))
  }

  override def jsonDecode(json: String): Option[String] = {

    parse(json).asInstanceOf[String] match {
      case "None" => None
      case x => Some(x)
    }
  }
}

class StringParam(parent: String, name: String, doc: String, isValid: String => Boolean)
  extends Param[String](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, DMParamValidators.nonEmpty)

  def this(parent: Identifiable, name: String, doc: String, isValid: String => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: String): ParamPair[String] = super.w(value)

  override def jsonEncode(value: String): String = {
    compact(render(JString(value)))
  }

  override def jsonDecode(json: String): String = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    parse(json).extract[String]
  }
}

class StrMapParam(parent: Params, name: String, doc: String, isValid: Map[String, String] => Boolean)
  extends Param[Map[String, String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, DMParamValidators.alwaysTrue)

  override def jsonEncode(value: Map[String, String]): String = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    write(value)
  }


  override def jsonDecode(json: String): Map[String, String] = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    parse(json) match {
      case JObject(fields) => fields.map(x => (x._1, x._2.extract[String])).toMap
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode StrMapParam from jsonStr: $json")
    }
  }
}

class StructTypeParam(parent: Params, name: String, doc: String, isValid: StructType => Boolean)
  extends Param[StructType](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, DMParamValidators.alwaysTrue)

  override def jsonEncode(value: StructType): String = {
    import org.json4s.JsonDSL._

    compact(render(value.map(x => ("name" -> x.name) ~ ("type" -> x.dataType.toString))))
  }

  override def jsonDecode(jsonStr: String): StructType = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val meta = parse(jsonStr) match {
      case JArray(values) =>
        values.map(jval => ((jval \ "name").extract[String], (jval \ "type").extract[String]))
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $jsonStr to Array[Double].")
    }

    StructType(meta.map { case (name, typeInfo) => TypeConverters.toStructField(name, typeInfo) })

  }
}

