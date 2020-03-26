package com.didi.dm.dmflow.feature.flow

import com.didi.dm.dmflow.common.hdfs.{Dataframes, HdfsUtility}
import com.didi.dm.dmflow.common.io.{FileUtility, IColorText}
import com.didi.dm.dmflow.common.time.Formatters
import com.didi.dm.dmflow.common.time.TimeUtils.Timer
import com.didi.dm.dmflow.config.FeatureFlowConfig
import com.didi.dm.dmflow.feature.dependency.DependencyParams.Dependencies
import com.didi.dm.dmflow.feature.flow.FeatureFlowModel.FeatureFlowModellWriter
import com.didi.dm.dmflow.feature.flow.operator.{MissingValueCounter, MissingValueFiller, SQLExtender, UDFExtender}
import com.didi.dm.dmflow.feature.flow.param.StepParams
import com.didi.dm.dmflow.param._
import com.didi.dm.dmflow.spark.io.Checkpointable
import com.didi.dm.dmflow.types.FeatureFlowType
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{parse, _}
import org.json4s.{DefaultFormats, _}

private[flow] trait FeatureFlowParams extends Params
  with HasConfigPath
  with HasExecuteDate
  with HasCheckpoint {

  class FlowConfigParam(parent: Params, name: String, doc: String, isValid: FeatureFlowConfig => Boolean)
    extends Param[FeatureFlowConfig](parent, name, doc, isValid) {

    def this(parent: Params, name: String, doc: String) =
      this(parent, name, doc, DMParamValidators.alwaysTrue)

    override def jsonEncode(value: FeatureFlowConfig): String = value.toJson

    override def jsonDecode(json: String): FeatureFlowConfig = FeatureFlowConfig.fromJson(json)
  }

  final val cfgParam: FlowConfigParam = new FlowConfigParam(this, "cfgText", "origin text of feature flow config")

  final def getConfig: FeatureFlowConfig = $(cfgParam)

  final def setConfig(value: FeatureFlowConfig): FeatureFlowParams.this.type = set(cfgParam, value)


  final val sqlDictParam: StrMapParam = new StrMapParam(this, "sqlTextDict", "k-v pairs to stores sqlPath -> sqlText")

  final def getSqlDict: Map[String, String] = $(sqlDictParam)

  final def setSqlDict(value: Map[String, String]): FeatureFlowParams.this.type = set(sqlDictParam, value)

  /* extend feature schema without input dataset's columns */
  final val extSchemaParam: StructTypeParam = new StructTypeParam(this, "extSchemaParam", "extended feature schema after feature flow")

  final def getExtSchema: StructType = $(extSchemaParam)

  final def setExtSchema(value: StructType): FeatureFlowParams.this.type = set(extSchemaParam, value)

}


class FeatureFlow(override val uid: String) extends Estimator[FeatureFlowModel]
  with FeatureFlowParams
  with DefaultParamsWritable
  with Logging
  with IColorText {

  def this() = this("FeatureFlow")

  var outSchema: Option[StructType] = None

  override def fit(dataset: Dataset[_]): FeatureFlowModel = {
    require(isDefined(configPathParam), "FeatureFlow must set configPathParam to load feature config")
    require(isDefined(executeDateParam), "FeatureFlow must set executeDateParam to specify feature date")

    logDebug(s"configPathParam -> $getConfigPath")
    logDebug(s"executeDateParam -> ${getExecuteDate.format(Formatters.ymdCompactFmt)}")

    val cfg = FeatureFlowConfig.create(getConfigPath)
    setConfig(cfg)
    logDebug(Yellow("[Config] ") + s"FeatureFlow parsed Config: ${getConfig.toJson}")

    val sqlMap = extractSQLs(cfg)
    setSqlDict(sqlMap)

    val model = copyValues(new FeatureFlowModel(s"FeatureFlowModel-$uid").setParent(this))
    logInfo("create FeatureFlowModel and try to transform features ...")
    val outSchema = model.transform(dataset).schema
    val inCols = dataset.schema.map(_.name).toSet
    val extSchema = StructType(outSchema.filterNot(field => inCols.contains(field.name)))
    setExtSchema(extSchema)

    model.setExtSchema(extSchema)

    model

  }

  protected def extractSQLs(cfg: FeatureFlowConfig): Map[String, String] = {
    val sqlMap = cfg.steps.getOrElse(List[Step]())
      .filter(_.opType == FeatureFlowType.SQL_Assembler)
      .filter(_.params.isDefined)
      .map { step =>
        val pt = StepParams.toSQLAssemblerParams(step.params).sqlFile
        val sql = FileUtility.readLines(pt)
        (pt, sql)
      }.toMap

    logInfo(s"Extract sql files to kv-dict, totalFiles=${sqlMap.size}")
    sqlMap.toSeq.zipWithIndex.foreach { case ((pt, content), idx) =>
      logInfo(Yellow("[DICT] ") + s"SQL-$idx\tlength=${content.length}\tsqlFile: $pt")
    }

    sqlMap
  }

  override def copy(extra: ParamMap): Estimator[FeatureFlowModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    if (isDefined(extSchemaParam)) {
      StructType(schema.toList ++ getExtSchema.toList)
    } else {
      logWarning("output schema only can be inferred after call fit(), now return original schema as default")
      schema
    }
  }
}

class FeatureFlowModel(override val uid: String)
  extends Model[FeatureFlowModel]
    with FeatureFlowParams
    with MLWritable
    with Checkpointable
    with Logging
    with IColorText {

  def this() = this("FeatureFlowModel")

  override def copy(extra: ParamMap): FeatureFlowModel = {
    val copied = new FeatureFlowModel(uid)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new FeatureFlowModellWriter(this).overwrite()

  override def transform(dataset: Dataset[_]): DataFrame = {
    require(getCheckpoint.isDefined, "please SET checkpoint path before call FeatureFlowModel.transform()")
    transform(dataset, getCheckpoint.get)
  }

  def transform(dataset: Dataset[_], featureCkp: String): DataFrame = {

    this.setCheckpoint(featureCkp)

    if (!isOverwrite && HdfsUtility.checkSuccess(featureCkp)) {
      logWarning(Green("[SKIP] ") + s"find FeatureFlow output checkpoint without overwrite, Skip process and load: $featureCkp")
      return this.loadCheckpoint()
    }

    val timer = Timer()
    timer.start("FeatureFlow")

    val cfg = getConfig.validateSelf()
    logInfo(Yellow("[Config] ") + s"FeatureFlow Runtime Config: ${cfg.toJson}")
    logInfo(Yellow("[SqlDict] ") + s"FeatureFlow SQL dict: ${compact(render(getSqlDict))}")

    val settings = cfg.settings.get
    val dependencies = cfg.dependencies.get
    val steps = cfg.steps.get

    // load dependencies value on runtime
    val runtimeDeps = getRuntimeDeps(dependencies)

    val stepsCkpBase = featureCkp + ".steps"
    logInfo(s"SET Steps' checkpoint base dir: $stepsCkpBase")

    val runners = steps.zipWithIndex.map { case (stepMeta, idx) =>
      val stepName = s"STEP.$idx"
      stepMeta.opType match {
        case FeatureFlowType.SQL_Assembler => new SQLExtender(
          uid = stepName, meta = stepMeta, settings = settings, ckpBaseHDFS = stepsCkpBase,
          runtimeDeps = runtimeDeps, sqlDict = getSqlDict
        )
        case FeatureFlowType.UDF_Assembler => new UDFExtender(
          uid = stepName, meta = stepMeta, settings = settings, ckpBaseHDFS = stepsCkpBase,
          runtimeDeps = runtimeDeps)
        case FeatureFlowType.MissingValueCounter => new MissingValueCounter(
          uid = stepName, meta = stepMeta, settings = settings, ckpBaseHDFS = stepsCkpBase
        )
        case FeatureFlowType.MissingValueFiller => new MissingValueFiller(
          uid = stepName, meta = stepMeta, settings = settings, ckpBaseHDFS = stepsCkpBase
        )
        case _ => throw new IllegalArgumentException(s"unknow FeatureFlowType: ${stepMeta.opType.name}, operator is NOT implemented! ")
      }
    }

    val lastValideIndexOfCkp = getLastIndexOfCheckpoint(runners)
    logInfo(s"find last index of STEP checkpoint in FeatureFlow, idx=$lastValideIndexOfCkp")

    var curDataset = dataset.toDF()
    runners.slice(lastValideIndexOfCkp, runners.size).foreach { op => curDataset = op.runStep(curDataset) }

    trySaveCheckpoint(curDataset)
    logInfo(Yellow("[Dump] ") + s"Extended Feature dump: $featureCkp")

    runners.filter(_.getRuntimeCkp.clear).foreach(_.clear())


    val elasped = timer.end("FeatureFlow").toMinutes
    logInfo(s"Run FeatureFlow pipeline complete, totalSteps: ${steps.size} elasped: ${elasped}mins")

    loadCheckpoint()
  }

  protected def getLastIndexOfCheckpoint(steps: List[BaseOperator]): Int = {
    var lastValideIndexOfCkp: Int = 0
    steps.zipWithIndex.foreach { case (op, idx) =>
      if (op.getRuntimeCkp.enable
        && !op.getRuntimeCkp.overwrite
        && op.getRuntimeCkp.path.isDefined
        && HdfsUtility.checkSuccess(op.getRuntimeCkp.path.get)) {
        logInfo(s"Find checkpoint of ${op.uid}: ${op.getCheckpoint.get}")
        lastValideIndexOfCkp = idx
      }
    }
    if (lastValideIndexOfCkp > 0) {
      logInfo(Green("[Checkpoint] ") + s"find latest checkpoint of Steps index=$lastValideIndexOfCkp")
    }
    lastValideIndexOfCkp
  }

  override def transformSchema(schema: StructType): StructType = {
    // stages.foldLeft(schema)((cur, transformer) => transformer.transformSchema(cur))
    val outSchema = StructType(schema.fields ++ getExtSchema.fields)
    val schemaInfo = Dataframes.checkSchema(outSchema).toString
    logInfo(s"FeatureSchema inputSize: ${schema.fields.length}, extendSize: ${getExtSchema.fields.length}, info: $schemaInfo")
    outSchema
  }

  def getRuntimeDeps(dependencies: Dependencies): Map[String, String] = {
    logInfo("begin to parse runtime dependencies ...")
    val timer = Timer().start()
    val runtimeDeps = dependencies.getKeyValuePairs(getExecuteDate)
    logInfo(s"parse runtime dependencies complete, elasped: ${timer.end().toMillis}ms")
    runtimeDeps.toSeq.zipWithIndex.foreach(x => logInfo(Yellow("[Dependency] ") + s"DEP-${x._2}\t${x._1._1}\t${x._1._2}"))
    runtimeDeps
  }

}

object FeatureFlowModel extends MLReadable[FeatureFlowModel] with IColorText {

  private val expectedClassName = classOf[FeatureFlowModel].getName

  def serialize(instance: FeatureFlowModel): String = {

    val jsonParams = (instance.cfgParam.name -> instance.cfgParam.jsonEncode(instance.getConfig)) ~
      (instance.sqlDictParam.name -> instance.sqlDictParam.jsonEncode(instance.getSqlDict)) ~
      (instance.extSchemaParam.name -> instance.extSchemaParam.jsonEncode(instance.getExtSchema))

    val metadata = ("class" -> instance.getClass.getName) ~ ("uid" -> instance.uid) ~ ("paramMap" -> jsonParams)

    compact(render(metadata))
  }

  def deserialize(metadataStr: String): FeatureFlowModel = {
    val metadata = parse(metadataStr)

    implicit val format: DefaultFormats.type = DefaultFormats
    val className = (metadata \ "class").extract[String]
    val uid = (metadata \ "uid").extract[String]
    val params = metadata \ "paramMap"

    require(className == expectedClassName, s"Error loading metadata: Expected class name" +
      s" $expectedClassName but found class name $className")


    val model = new FeatureFlowModel(uid)

    model.setConfig(model.cfgParam.jsonDecode((params \ model.cfgParam.name).extract[String]))
    model.setSqlDict(model.sqlDictParam.jsonDecode((params \ model.sqlDictParam.name).extract[String]))
    model.setExtSchema(model.extSchemaParam.jsonDecode((params \ model.extSchemaParam.name).extract[String]))
    model
  }

  private[FeatureFlowModel]
  class FeatureFlowModellWriter(instance: FeatureFlowModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val metadataPath = new Path(path, "metadata").toString
      val metadataJson: String = serialize(instance)
      sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
      logInfo(Yellow("[Meta] ") + s"FeatureFlowModel save complete! dump: $metadataPath")
    }


  }

  private[FeatureFlowModel]
  class FeatureFlowModelReader extends MLReader[FeatureFlowModel] {


    override def load(path: String): FeatureFlowModel = {
      val metadataPath = new Path(path, "metadata").toString
      val metadataStr = sc.textFile(metadataPath, 1).first()
      deserialize(metadataStr)
    }
  }

  override def read: MLReader[FeatureFlowModel] = new FeatureFlowModelReader

  override def load(path: String): FeatureFlowModel = super.load(path)

}
