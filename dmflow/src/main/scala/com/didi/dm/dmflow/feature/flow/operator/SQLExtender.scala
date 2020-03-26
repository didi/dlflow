package com.didi.dm.dmflow.feature.flow.operator

import com.didi.dm.dmflow.config.FeatureFlowConfig.GlobalSettings
import com.didi.dm.dmflow.feature.extractor.SQLExtractor
import com.didi.dm.dmflow.feature.flow.param.StepParams
import com.didi.dm.dmflow.feature.flow.{BaseOperator, Step}
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

class SQLExtender(override val uid: String,
                  @transient override val meta: Step,
                  @transient override val settings: GlobalSettings,
                  override val ckpBaseHDFS: String,
                  val runtimeDeps: Map[String, String],
                  val sqlDict: Map[String, String]
                 ) extends BaseOperator(uid, meta, settings, ckpBaseHDFS) {

  override def process(inDF: DataFrame): DataFrame = {

    val sqlExtractor: SQLExtractor = new SQLExtractor()
    val param = StepParams.toSQLAssemblerParams(meta.params)

    logInfo(s"RUN SQLExtender=$uid (${meta.name}) sqlFile=${param.sqlFile} joinKeys=${param.joinKeys.mkString("<", ",", ">")} parallelism=${settings.parallelism}")

    val sqlTemplate = sqlDict(param.sqlFile)
    val args = param.substitutions.map(x => (x, runtimeDeps(x))).toMap

    sqlExtractor.setSqlTemplate(sqlTemplate).setEnableSubdir(param.enableSubdir).setCheckEmpty(param.emptyCheck)
    if (param.primaryKeys.isDefined) sqlExtractor.setPrimaryKeys(param.primaryKeys.get)
    if (param.uniqueCheck.isDefined) sqlExtractor.setUniqueCheck(param.uniqueCheck.get)

    Try {
      val featureDF = sqlExtractor.getFeature(args)
      inDF.repartition(settings.parallelism).join(featureDF.repartition(settings.parallelism), Seq(param.joinKeys: _*), "left_outer")
    } match {
      case Success(x) => x
      case Failure(ex) =>
        logError(s"some error happend when run SQLExtender: $uid, msg=${ex.getMessage}")
        throw new RuntimeException(ex)
    }
  }
}
