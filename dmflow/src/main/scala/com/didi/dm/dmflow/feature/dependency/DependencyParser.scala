package com.didi.dm.dmflow.feature.dependency

import com.didi.dm.dmflow.base.JsonReadable
import com.didi.dm.dmflow.feature.dependency.DependencyParams.{Dependencies, _}
import com.didi.dm.dmflow.types.FormatterType
import org.json4s.{DefaultFormats, JValue}

object DependencyParser extends JsonReadable[Dependencies] {

  def parseHiveDeps(jdeps: List[JValue]): List[HiveDependency] = {

    if (jdeps.isEmpty) {
      return List.empty
    }

    implicit val format: DefaultFormats.type = DefaultFormats

    jdeps.map { jval =>
      HiveDependency(
        key = (jval \ "key").extract[String],
        formatter = FormatterType.fromName((jval \ "formatter").extract[String]),
        offsetDays = (jval \ "offsetDays").extract[Int],
        maxDegradeDays = (jval \ "maxDegradeDays").extract[Int],
        pathTemplate = (jval \ "pathTemplate").extract[String],
        checker = Checker.fromJValue((jval \ "checker").extract[JValue])
      )
    }


  }

  def parseHdfsDeps(jdeps: List[JValue]): List[HdfsDependency] = {

    if (jdeps.isEmpty) {
      return List.empty
    }

    implicit val format: DefaultFormats.type = DefaultFormats

    jdeps.map { jval =>
      HdfsDependency(
        key = (jval \ "key").extract[String],
        offsetDays = (jval \ "offsetDays").extract[Int],
        maxDegradeDays = (jval \ "maxDegradeDays").extract[Int],
        pathTemplate = (jval \ "pathTemplate").extract[String],
        checker = Checker.fromJValue((jval \ "checker").extract[JValue])
      )
    }
  }

  def parseDateDeps(jdeps: List[JValue]): List[DateDependency] = {

    if (jdeps.isEmpty) {
      return List.empty
    }

    implicit val format: DefaultFormats.type = DefaultFormats

    jdeps.map { jval =>
      DateDependency(
        key = (jval \ "key").extract[String],
        formatter = FormatterType.fromName((jval \ "formatter").extract[String]),
        offsetDays = (jval \ "offsetDays").extract[Int]
      )
    }
  }

  def parseStaticDeps(jdeps: List[JValue]): List[StaticDependency] = {

    if (jdeps.isEmpty) {
      return List.empty
    }

    implicit val format: DefaultFormats.type = DefaultFormats

    jdeps.map { jval => StaticDependency(key = (jval \ "key").extract[String], value = (jval \ "value").extract[String])
    }
  }


  override def fromJValue(json: JValue): Dependencies = {
    implicit val format: DefaultFormats.type = DefaultFormats

    Dependencies(
      hive = parseHiveDeps((json \ "hive").extract[List[JValue]]),
      hdfs = parseHdfsDeps((json \ "hdfs").extract[List[JValue]]),
      date = parseDateDeps((json \ "date").extract[List[JValue]]),
      static = parseStaticDeps((json \ "static").extract[List[JValue]])
    )
  }

}
