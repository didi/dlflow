package com.didi.dm.dmflow.config

import com.didi.dm.dmflow.base.{JsonReadable, JsonWritable}
import com.didi.dm.dmflow.types.SourceType
import com.google.common.base.Strings
import com.typesafe.config.Config
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render
import org.json4s.{DefaultFormats, JValue}

case class Checkpoint(
                       var enable: Boolean,
                       var overwrite: Boolean,
                       var clear: Boolean,
                       var emptyCheck: Boolean,
                       @transient var sourceType: SourceType,
                       var path: Option[String]
                     ) extends JsonWritable {
  override def toJValue: JValue = {
    var metadata = ("enable" -> enable) ~
      ("overwrite" -> overwrite) ~
      ("clear" -> clear) ~
      ("emptyCheck" -> emptyCheck) ~
      ("sourceType" -> sourceType.name)

    if (path.isDefined && !Strings.isNullOrEmpty(path.get.trim)) metadata = metadata ~ ("path" -> path.get)

    render(metadata)
  }
}

object Checkpoint extends JsonReadable[Checkpoint] {
  val DEFAULT_CKP_ENABLE = false
  val DEFAULT_CKP_OVERWRITE = false
  val DEFAULT_CKP_CLEAR = true
  val DEFAULT_CKP_EMPTY_CHECK = true
  val DEFAULT_CKP_FORMAT: SourceType = SourceType.Parquet
  val DEFAULT_CKP_PATH: Option[Nothing] = None

  def fromConfig(config: Config, path: String): Checkpoint = fromJValue(ConfigHelper.getJValue(config, path))

  override def fromJValue(jval: JValue): Checkpoint = {

    implicit val format: DefaultFormats.type = DefaultFormats

    Checkpoint(
      enable = parseJValueOrElse[Boolean](jval, "enable", DEFAULT_CKP_ENABLE),
      overwrite = parseJValueOrElse[Boolean](jval, "overwrite", DEFAULT_CKP_OVERWRITE),
      clear = parseJValueOrElse[Boolean](jval, "clear", DEFAULT_CKP_CLEAR),
      emptyCheck = parseJValueOrElse[Boolean](jval, "emptyCheck", DEFAULT_CKP_EMPTY_CHECK),
      sourceType = parseWithMappingOrElse[String, SourceType](jval, "sourceType", DEFAULT_CKP_FORMAT, (x: String) => SourceType.fromName(x)),
      path = parseJValue[String](jval, "path")

    )
  }

  def getDefaultCheckpoint: Checkpoint = {
    Checkpoint(
      enable = DEFAULT_CKP_ENABLE,
      overwrite = DEFAULT_CKP_OVERWRITE,
      clear = DEFAULT_CKP_CLEAR,
      emptyCheck = DEFAULT_CKP_EMPTY_CHECK,
      sourceType = DEFAULT_CKP_FORMAT,
      path = DEFAULT_CKP_PATH
    )
  }

}
