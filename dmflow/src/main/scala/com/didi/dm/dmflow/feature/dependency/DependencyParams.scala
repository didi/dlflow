package com.didi.dm.dmflow.feature.dependency

import java.time.LocalDate

import com.didi.dm.dmflow.base.{JsonReadable, JsonWritable}
import com.didi.dm.dmflow.common.io.Templates
import com.didi.dm.dmflow.common.util.Validator
import com.didi.dm.dmflow.pojo.DTO.BoolRet
import com.didi.dm.dmflow.types.FormatterType
import com.google.common.base.Strings
import org.json4s.JValue
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.render

import scala.collection.mutable


object DependencyParams {

  val DEFAULT_OFFSET_DAYs = 0
  val DEFAULT_DEGRADE_DAYs = 0

  case class Dependencies(var hive: List[HiveDependency],
                          var hdfs: List[HdfsDependency],
                          var date: List[DateDependency],
                          var static: List[StaticDependency]) extends JsonWritable {

    def this() = this(hive = List.empty, hdfs = List.empty, date = List.empty, static = List.empty)

    protected val depsCache: mutable.Map[LocalDate, Map[String, String]] = mutable.HashMap[LocalDate, Map[String, String]]()

    protected final def depValues(bizDate: LocalDate): Map[String, String] = {
      Validator.assert(validateKeys())
      
      if (depsCache.get(bizDate).isEmpty) {
        logInfo("begin to extract runtime Dependency ... ")
        val hiveDeps = getKeyValuePairs(bizDate, hive)
        val hdfsDeps = getKeyValuePairs(bizDate, hdfs)
        val dateDeps = getKeyValuePairs(bizDate, date)
        val staticDeps = getKeyValuePairs(bizDate, static)
        logInfo(s"extract runtime Dependency complete! hive: ${hiveDeps.size} hdfs: ${hdfsDeps.size} date: ${dateDeps.size} static: ${staticDeps.size}")
        depsCache += (bizDate -> (hiveDeps ++ hdfsDeps ++ dateDeps ++ staticDeps).toMap)
      }

      depsCache(bizDate)
    }

    protected def getKeys(deps: List[Dependent]): List[String] = deps.map(_.key)

    protected def getKeyValuePairs(bizDate: LocalDate, deps: List[Dependent]): List[(String, String)] = {
      deps.map(x => (x.key, x.getDependency(bizDate)))
    }

    def validateKeys(): BoolRet = {

      val keys = getKeys(hive) ++ getKeys(hdfs) ++ getKeys(date) ++ getKeys(static)
      val counter = keys.groupBy(x => x).mapValues(_.length)

      if (keys.size == counter.size) {
        BoolRet(ret = true)
      } else {
        val duplicates = counter.filter(_._2 > 1)
        BoolRet(ret = false, s"Dependencies some key duplicates in config：${duplicates.mkString("[", ",", "]")}")
      }

    }

    def getKeyValuePairs(bizDate: LocalDate): Map[String, String] = depValues(bizDate)

    override def toJValue: JValue = {
      import org.json4s.JsonDSL._

      def getJValueDeps[T <: JsonWritable](list: List[T]) = list.map(_.toJValue)

      val metadata = ("hive" -> getJValueDeps[HiveDependency](hive)) ~
        ("hdfs" -> getJValueDeps[HdfsDependency](hdfs)) ~
        ("date" -> getJValueDeps[DateDependency](date)) ~
        ("static" -> getJValueDeps[StaticDependency](static))
      render(metadata)
    }

  }

  object Dependencies extends JsonReadable[Dependencies] {
    override def fromJValue(jval: JValue): Dependencies = {


      Dependencies(
        hive = (jval \ "hive").extract[JArray].arr.map(x => HiveDependency.fromJValue(x)),
        hdfs = (jval \ "hdfs").extract[JArray].arr.map(x => HdfsDependency.fromJValue(x)),
        date = (jval \ "date").extract[JArray].arr.map(x => DateDependency.fromJValue(x)),
        static = (jval \ "static").extract[JArray].arr.map(x => StaticDependency.fromJValue(x))
      )
    }
  }

  case class HiveDependency(key: String,
                            formatter: FormatterType,
                            offsetDays: Int,
                            maxDegradeDays: Int,
                            pathTemplate: String,
                            checker: Checker
                           ) extends Dependent with TimeShiftable with Degradable with JsonWritable {
    require(!Strings.isNullOrEmpty(key), "HiveDependency input key is null or empty!")

    override def getDependency(bizDate: LocalDate): String = {
      val optDate = tryDegrade(getRedirectDate(bizDate), pathTemplate, checker)
      require(optDate.isDefined, s"hive data source degrade failed！${this.toString}")
      formatter.format(optDate.get)
    }

    override def toJValue: JValue = {
      import org.json4s.JsonDSL._
      val metadata = ("key" -> key) ~
        formatter.toJObject ~
        ("offsetDays" -> offsetDays) ~
        ("maxDegradeDays" -> maxDegradeDays) ~
        ("pathTemplate" -> pathTemplate) ~
        ("checker" -> checker.toJValue)
      render(metadata)
    }

  }

  object HiveDependency extends JsonReadable[HiveDependency] {
    override def fromJValue(jval: JValue): HiveDependency = {
      HiveDependency(
        key = parseJValueStrictly[String](jval, "key"),
        formatter = FormatterType.fromJValue(jval \ "formatter"),
        offsetDays = parseJValueStrictly[Int](jval, "offsetDays"),
        maxDegradeDays = parseJValueStrictly[Int](jval, "maxDegradeDays"),
        pathTemplate = parseJValueStrictly[String](jval, "pathTemplate"),
        checker = Checker.fromJValue(jval \ "checker")
      )
    }
  }


  case class HdfsDependency(key: String,
                            offsetDays: Int,
                            maxDegradeDays: Int,
                            pathTemplate: String,
                            checker: Checker
                           ) extends Dependent with TimeShiftable with Degradable with JsonWritable {
    require(!Strings.isNullOrEmpty(key), "HdfsDependency input key is null or empty!")

    override def getDependency(bizDate: LocalDate): String = {
      val optDate = tryDegrade(getRedirectDate(bizDate), pathTemplate, checker)
      require(optDate.isDefined, s"hive 数据源降级失败！${this.toString}")
      Templates.formatPath(pathTemplate, optDate.get)
    }

    override def toJValue: JValue = {
      import org.json4s.JsonDSL._
      val metadata = ("key" -> key) ~
        ("offsetDays" -> offsetDays) ~
        ("maxDegradeDays" -> maxDegradeDays) ~
        ("pathTemplate" -> pathTemplate) ~
        ("checker" -> checker.toJValue)
      render(metadata)
    }

  }

  object HdfsDependency extends JsonReadable[HdfsDependency] {
    override def fromJValue(jval: JValue): HdfsDependency = {
      HdfsDependency(
        key = parseJValueStrictly[String](jval, "key"),
        offsetDays = parseJValueStrictly[Int](jval, "offsetDays"),
        maxDegradeDays = parseJValueStrictly[Int](jval, "maxDegradeDays"),
        pathTemplate = parseJValueStrictly[String](jval, "pathTemplate"),
        checker = Checker.fromJValue(jval \ "checker")
      )
    }
  }

  case class DateDependency(key: String, formatter: FormatterType, offsetDays: Int)
    extends Dependent with TimeShiftable with JsonWritable {
    require(!Strings.isNullOrEmpty(key), "DateDependency input key is null or empty!")

    override def getDependency(bizDate: LocalDate): String = formatter.format(getRedirectDate(bizDate))

    override def toJValue: JValue = {
      import org.json4s.JsonDSL._
      val metadata = ("key" -> key) ~
        formatter.toJObject ~
        ("offsetDays" -> offsetDays)
      render(metadata)
    }

  }

  object DateDependency extends JsonReadable[DateDependency] {
    override def fromJValue(jval: JValue): DateDependency = {
      DateDependency(
        key = parseJValueStrictly[String](jval, "key"),
        formatter = FormatterType.fromJValue(jval \ "formatter"),
        offsetDays = parseJValueStrictly[Int](jval, "offsetDays")
      )
    }
  }

  case class StaticDependency(key: String, value: String) extends Dependent with JsonWritable {
    require(!Strings.isNullOrEmpty(key), "StaticDependency input key is null or empty!")

    override def getDependency(bizDate: LocalDate): String = value

    override def toJValue: JValue = {
      import org.json4s.JsonDSL._
      val metadata = ("key" -> key) ~ ("value" -> value)
      render(metadata)
    }
  }

  object StaticDependency extends JsonReadable[StaticDependency] {
    override def fromJValue(jval: JValue): StaticDependency = {
      StaticDependency(
        key = parseJValueStrictly[String](jval, "key"),
        value = parseJValueStrictly[String](jval, "value")
      )
    }
  }

}
