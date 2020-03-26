package com.didi.dm.dmflow.config

import com.google.common.base.Strings
import com.typesafe.config.{Config, ConfigObject, ConfigRenderOptions}
import org.apache.spark.internal.Logging
import org.json4s.jackson.JsonMethods._
import org.json4s.{JValue, _}


object ConfigHelper extends Logging {

  def get[T](config: Config, path: String): T = {
    require(!Strings.isNullOrEmpty(path), "try to get config but input path is null or empty!")
    require(config.hasPath(path), s"try to get config but input path not exists: $path")
    config.getAnyRef(path).asInstanceOf[T]
  }

  def getOrElse[T](config: Config, path: String, default: T): T = {
    require(!Strings.isNullOrEmpty(path), "try to get config but input path is null or empty!")
    if (config.hasPath(path)) config.getAnyRef(path).asInstanceOf[T] else default
  }

  def getOpt[T](config: Config, path: String): Option[T] = {
    require(!Strings.isNullOrEmpty(path), "try to get config but input path is null or empty!")
    if (config.hasPath(path)) Some(config.getAnyRef(path).asInstanceOf[T]) else None
  }

  def getSealedType[T](config: Config, path: String, defaultType: T, func: String => T): T = {
    if (config.hasPath(path)) func(config.getString(path)) else defaultType
  }

  def getSubConf(config: Config, path: String): Option[Config] = {
    if (config.hasPath(path)) Some(config.getConfig(path)) else None
  }

  def getJsonMap(config: Config, path: String): String = {
    getConfigObject(config, path).render(ConfigRenderOptions.concise())
  }

  def getJValue(config: Config, path: String): JValue = {
    parse(getJsonMap(config, path))
  }

  def getConfigObject(config: Config, path: String): ConfigObject = {
    require(!Strings.isNullOrEmpty(path), "try to get config but input path is null or empty!")
    require(config.hasPath(path), s"try to get config but input path not exists: $path")
    config.getObject(path)
  }

  def getOptConf[T](config: Config, path: String, func: String => T): Option[T] = {
    if (config.hasPath(path)) Some(func(path)) else None
  }

  def getList(config: Config, path: String): List[Config] = {
    require(!Strings.isNullOrEmpty(path), "try to get config but input path is null or empty!")
    import scala.collection.JavaConversions._
    if (config.hasPath(path)) config.getConfigList(path).toList else List.empty
  }

  def getBooleanOrElse(config: Config, path: String, default: Boolean): Boolean = getOrElse[Boolean](config, path, default)

  def getIntOrElse(config: Config, path: String, default: Int): Int = getOrElse[Int](config, path, default)

  def getDoubleOrElse(config: Config, path: String, default: Double): Double = getOrElse[Number](config, path, default).doubleValue()

  def getStringOrElse(config: Config, path: String, default: String): String = getOrElse[String](config, path, default)

  def getString(config: Config, path: String): String = get[String](config, path)


}
