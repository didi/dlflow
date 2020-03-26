package com.didi.dm.dmflow.config

import com.didi.dm.dmflow.base.{JsonReadable, JsonWritable}
import com.didi.dm.dmflow.common.io.FileUtility
import com.didi.dm.dmflow.config.FeatureFlowConfig.GlobalSettings
import com.didi.dm.dmflow.feature.dependency.DependencyParams._
import com.didi.dm.dmflow.feature.dependency.{Checker, DependencyParams}
import com.didi.dm.dmflow.feature.flow.Step
import com.didi.dm.dmflow.feature.flow.param.{BaseMapParams, StepParams}
import com.didi.dm.dmflow.types.{CheckerMethod, FeatureFlowType, FormatterType, SourceType}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.internal.Logging
import org.json4s.JValue
import org.json4s.JsonAST.{JArray, JBool, JInt}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render


object FeatureFlowConfig extends JsonReadable[FeatureFlowConfig] {

  val DEFAULT_OVERRIDE = false
  val DEFAULT_PARALLELISM = 1000


  def create(configFilePath: String): FeatureFlowConfig = (new FeatureFlowConfig).load(configFilePath)

  override def fromJValue(jval: JValue): FeatureFlowConfig = {
    new FeatureFlowConfig(
      settings = if (jval.has("settings")) Some(GlobalSettings.fromJValue(jval \ "settings")) else None,
      dependencies = if (jval.has("dependencies")) Some(Dependencies.fromJValue(jval \ "dependencies")) else None,
      steps = Some((jval \ "steps").extract[JArray].arr.map(x => Step.fromJValue(x)))
    )
  }

  case class GlobalSettings(
                             var enableOverride: Boolean = DEFAULT_OVERRIDE,
                             @transient var checkpointConfig: Checkpoint,
                             @transient var checker: Checker,
                             var parallelism: Int = DEFAULT_PARALLELISM
                           ) extends JsonWritable {

    override def toJValue: JValue = {
      val metadata = ("enableOverride" -> JBool(enableOverride)) ~
        ("checkpointConfig" -> checkpointConfig.toJValue) ~
        ("checker" -> checker.toJValue) ~
        ("parallelism" -> JInt(parallelism))

      render(metadata)
    }

  }

  object GlobalSettings extends JsonReadable[GlobalSettings] {
    override def fromJValue(jval: JValue): GlobalSettings = {
      GlobalSettings(
        enableOverride = parseJValueOrElse[Boolean](jval, "enableOverride", DEFAULT_OVERRIDE),
        checkpointConfig = Checkpoint.fromJValue(jval \ "checkpointConfig"),
        checker = Checker.fromJValue(jval \ "checker"),
        parallelism = parseJValueOrElse[Int](jval, "parallelism", DEFAULT_PARALLELISM)
      )
    }

    def getDefault: GlobalSettings = {
      GlobalSettings(
        enableOverride = FeatureFlowConfig.DEFAULT_OVERRIDE,
        checkpointConfig = Checkpoint.getDefaultCheckpoint,
        checker = Checker.getDefaultChecker,
        parallelism = FeatureFlowConfig.DEFAULT_PARALLELISM
      )
    }
  }

}

class FeatureFlowConfig private (var settings: Option[GlobalSettings] = None,
                        var dependencies: Option[Dependencies] = None,
                        var steps: Option[List[Step]] = None
                       ) extends JsonWritable with Logging {

  def load(configFilePath: String): this.type = {
    require(FileUtility.exists(configFilePath), s"input config file for FeatureFlowConfig not exists: $configFilePath")

    logInfo(s"Load feature flow config from: $configFilePath")
    val configContent: String = FileUtility.readLines(configFilePath)
    logInfo(s"config text: $configContent")

    logInfo("begin to parse FeatureFlow Config ...")
    val conf: Config = ConfigFactory.parseString(configContent)

    this.settings = Some(parserSettings(conf))

    this.dependencies = Some(parseDependencies(conf))

    this.steps = Some(parseSteps(conf))

    logInfo("FeatureFlow Config parse complete!")
    this
  }

  def validateSelf(): this.type = {
    require(settings.isDefined && dependencies.isDefined && steps.isDefined, s"some params in FeatureFlowConfig is invalid: ${this.toString}")
    this
  }

  protected def parserSettings(conf: Config): GlobalSettings = {

    val globalSettings = FeatureFlowConfig.GlobalSettings.getDefault

    val optSettings = ConfigHelper.getSubConf(conf, "settings")
    if (optSettings.isDefined) {
      val settings = optSettings.get
      globalSettings.enableOverride = ConfigHelper.getBooleanOrElse(settings, "enableOverride", FeatureFlowConfig.DEFAULT_OVERRIDE)

      val optCheckpoint = ConfigHelper.getSubConf(settings, "checkpoint")
      if (optCheckpoint.isDefined) {
        val checkpoint = optCheckpoint.get
        globalSettings.checkpointConfig.enable = ConfigHelper.getBooleanOrElse(checkpoint, "enable", Checkpoint.DEFAULT_CKP_ENABLE)
        globalSettings.checkpointConfig.overwrite = ConfigHelper.getBooleanOrElse(checkpoint, "overwrite", Checkpoint.DEFAULT_CKP_OVERWRITE)
        globalSettings.checkpointConfig.clear = ConfigHelper.getBooleanOrElse(checkpoint, "clear", Checkpoint.DEFAULT_CKP_CLEAR)
        globalSettings.checkpointConfig.emptyCheck = ConfigHelper.getBooleanOrElse(checkpoint, "emptyCheck", Checkpoint.DEFAULT_CKP_EMPTY_CHECK)
        globalSettings.checkpointConfig.sourceType = ConfigHelper.getSealedType[SourceType](checkpoint, "format", Checkpoint.DEFAULT_CKP_FORMAT, (x: String) => SourceType.fromName(x))
        globalSettings.checkpointConfig.path = ConfigHelper.getOpt[String](checkpoint, "path")
      }

      val optChecker = ConfigHelper.getSubConf(settings, "checker")
      if (optChecker.isDefined) {
        val checker = optChecker.get
        globalSettings.checker.method = ConfigHelper.getSealedType[CheckerMethod](checker, "method", Checker.DEFAULT_CHECK_TYPE, (x: String) => CheckerMethod.fromName(x))
        globalSettings.checker.threshold = ConfigHelper.getDoubleOrElse(checker, "threshold", Checker.DEFAULT_CHECK_THRESHOLD)
        globalSettings.checker.sourceType = ConfigHelper.getSealedType[SourceType](checker, "sourceType", Checker.DEFAULT_CHECK_SOURCE, (x: String) => SourceType.fromName(x))
      }

      globalSettings.parallelism = ConfigHelper.getIntOrElse(settings, "parallelism", FeatureFlowConfig.DEFAULT_PARALLELISM)
    }

    globalSettings
  }

  protected def parseDependencies(conf: Config): Dependencies = {

    val dependencies = new Dependencies()

    val optConf = ConfigHelper.getSubConf(conf, "dependencies")
    if (optConf.isDefined) {
      val depsConf = optConf.get

      dependencies.hive = ConfigHelper.getList(depsConf, "hive").map { hive =>
        HiveDependency(
          key = ConfigHelper.getString(hive, "key"),
          formatter = FormatterType.fromName(ConfigHelper.getString(hive, "formatter")),
          offsetDays = ConfigHelper.getIntOrElse(hive, "offsetDays", DependencyParams.DEFAULT_OFFSET_DAYs),
          maxDegradeDays = ConfigHelper.getIntOrElse(hive, "degradeDays", DependencyParams.DEFAULT_DEGRADE_DAYs),
          pathTemplate = ConfigHelper.getString(hive, "pathTemplate"),
          checker = parseChecker(hive)
        )
      }

      dependencies.hdfs = ConfigHelper.getList(depsConf, "hdfs").map { hdfs =>
        HdfsDependency(
          key = ConfigHelper.getString(hdfs, "key"),
          offsetDays = ConfigHelper.getIntOrElse(hdfs, "offsetDays", DependencyParams.DEFAULT_OFFSET_DAYs),
          maxDegradeDays = ConfigHelper.getIntOrElse(hdfs, "degradeDays", DependencyParams.DEFAULT_DEGRADE_DAYs),
          pathTemplate = ConfigHelper.getString(hdfs, "pathTemplate"),
          checker = parseChecker(hdfs)
        )
      }

      dependencies.date = ConfigHelper.getList(depsConf, "date").map { date =>
        DateDependency(
          key = ConfigHelper.getString(date, "key"),
          formatter = FormatterType.fromName(ConfigHelper.getString(date, "formatter")),
          offsetDays = ConfigHelper.getIntOrElse(date, "offsetDays", DependencyParams.DEFAULT_OFFSET_DAYs)
        )
      }

      dependencies.static = ConfigHelper.getList(depsConf, "static").map { static =>
        StaticDependency(
          key = ConfigHelper.getString(static, "key"),
          value = ConfigHelper.getString(static, "value")
        )
      }

    }

    dependencies
  }

  protected def parseChecker(conf: Config, path: String = "checker"): Checker = {

    val optCheckConf = ConfigHelper.getSubConf(conf, path)
    if(optCheckConf.isEmpty) {
      logDebug("Checker config is missing, using _SUCCESS checker as Default")
      return Checker(method = CheckerMethod._SUCCESS)
    }

    val checkConf = optCheckConf.get

    val methodName = ConfigHelper.getStringOrElse(checkConf, "method", Checker.DEFAULT_CHECK_TYPE.name)
    val sourceName = ConfigHelper.getStringOrElse(checkConf, "sourceType", Checker.DEFAULT_CHECK_SOURCE.name)
    Checker(
      method = CheckerMethod.fromName(methodName),
      threshold = ConfigHelper.getDoubleOrElse(checkConf, "threshold", Checker.DEFAULT_CHECK_THRESHOLD),
      sourceType = SourceType.fromName(sourceName)
    )
  }

  protected def parseSteps(conf: Config): List[Step] = {
    ConfigHelper.getList(conf, "steps").zipWithIndex.map { case (stepConf, idx) =>
      logInfo(s"parse config of OP Step-$idx: ${stepConf.toString}")

      val name = ConfigHelper.getString(stepConf, "name")
      val desc = ConfigHelper.getStringOrElse(stepConf, "desc", "")
      val opType = FeatureFlowType.fromName(ConfigHelper.getString(stepConf, "opType"))

      val getCkpFunc = (path: String) => Checkpoint.fromConfig(stepConf, path)
      val checkpoint = ConfigHelper.getOptConf[Checkpoint](stepConf, "checkpoint", getCkpFunc)

      val getParamsFunc = (path: String) => StepParams.fromConfig(stepConf, path, opType)
      val params = ConfigHelper.getOptConf[BaseMapParams](stepConf, "params", getParamsFunc)

      Step(name = name, desc = desc, opType = opType, checkpoint = checkpoint, params = params)
    }
  }

  override def toJValue: JValue = {
    require(settings.isDefined && dependencies.isDefined && steps.isDefined,
      s"required value of FeatureFlowConfig is empty, " +
        s"settings=${settings.isDefined} dependencies=${dependencies.isDefined} steps=${steps.isDefined}")

    ("settings" -> settings.get.toJValue) ~ ("dependencies" -> dependencies.get.toJValue) ~ ("steps" -> steps.get.map(_.toJValue))
  }

}
