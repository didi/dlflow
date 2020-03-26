package com.didi.dm.dmflow.base

import com.google.common.base.Strings
import org.apache.spark.internal.Logging
import org.json4s.JsonAST.JNothing
import org.json4s.{DefaultFormats, JValue}

trait Json4sPlus extends Logging {

  implicit val format: DefaultFormats.type = DefaultFormats

  implicit class JValueExtended(value: JValue) {
    def has(childString: String): Boolean = if ((value \ childString) != JNothing) true else false
  }

  def parseOptWithMapping[U: Manifest, T](jval: JValue, path: String, mappingFunc: U => T): Option[T] = {
    if (jval.has(path)) Some(mappingFunc((jval \ path).extract[U])) else None
  }

  def parseWithMappingOrElse[U: Manifest, T](jval: JValue, path: String, default: T, mappingFunc: U => T): T = {
    if (!jval.has(path)) return default
    parseWithMapping(jval, path, mappingFunc)
  }

  def parseWithMapping[U: Manifest, T](jval: JValue, path: String, mappingFunc: U => T): T = {
    require(!Strings.isNullOrEmpty(path), "parse jvalue but input path is empty！")
    require(jval.has(path), s"parse jvalue but input path not exists：$path")
    mappingFunc((jval \ path).extract[U])
  }

  def parseJValue[T: Manifest](jval: JValue, path: String): Option[T] = {
    if (jval.has(path)) Some((jval \ path).extract[T]) else None
  }

  def parseJValue[T: Manifest](jval: JValue, path: String, extract: JValue => T): Option[T] = {
    if (jval.has(path)) Some(extract(jval \ path)) else None
  }

  def parseJValueOrElse[T: Manifest](jval: JValue, path: String, default: T): T = {
    if (jval.has(path)) {
      (jval \ path).extract[T]
    } else {
      logDebug(s"parse jvalue but path=$path not exists, return default value: $default")
      default
    }
  }

  def parseJValueStrictly[T: Manifest](jval: JValue, path: String): T = {
    require(!Strings.isNullOrEmpty(path), "parse jvalue but input path is empty！")
    require(jval.has(path), s"parse jvalue but input path not exists：$path")
    (jval \ path).extract[T]
  }

}
