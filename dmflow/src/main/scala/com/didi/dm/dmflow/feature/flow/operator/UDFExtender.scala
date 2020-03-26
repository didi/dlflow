package com.didi.dm.dmflow.feature.flow.operator

import com.didi.dm.dmflow.base.Reflective
import com.didi.dm.dmflow.config.FeatureFlowConfig.GlobalSettings
import com.didi.dm.dmflow.feature.extractor.UDFExtractor
import com.didi.dm.dmflow.feature.flow.param.StepParams
import com.didi.dm.dmflow.feature.flow.{BaseOperator, Step}
import org.apache.spark.sql.DataFrame
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

import scala.util.{Failure, Success, Try}


class UDFExtender(override val uid: String,
                  @transient override val meta: Step,
                  @transient override val settings: GlobalSettings,
                  override val ckpBaseHDFS: String,
                  val runtimeDeps: Map[String, String]
                 ) extends BaseOperator(uid, meta, settings, ckpBaseHDFS) with Reflective {

  override def process(inDF: DataFrame): DataFrame = {

    val udfExtractor: UDFExtractor = new UDFExtractor()
    val param = StepParams.toUDFAssemblerParams(meta.params)
    val clazz = param.clazzOfUDF
    val joinKeys = param.joinKeys
    val extraParams = param.extraParams

    logInfo(s"RUN UDFExtender=$uid (${meta.name}) clazz=$clazz joinKeys=${joinKeys.mkString("<", ",", ">")} parallelism=${settings.parallelism}")

    logDebug(s"UDFExtender extraParams: ${render(extraParams).toString}")

    val subs = param.substitutions.map(x => (x, runtimeDeps(x))).toMap

    udfExtractor.setClassName(clazz).setSubstitutions(subs).setCheckEmpty(param.emptyCheck)
    if (param.primaryKeys.isDefined) udfExtractor.setPrimaryKeys(param.primaryKeys.get)
    if (param.uniqueCheck.isDefined) udfExtractor.setUniqueCheck(param.uniqueCheck.get)

    Try {
      val featureDF = udfExtractor.getFeature(extraParams)
      inDF.repartition(settings.parallelism).join(featureDF.repartition(settings.parallelism), Seq(param.joinKeys: _*), "left_outer")
    } match {
      case Success(x) => x
      case Failure(ex) =>
        logError(s"some error happend when run UDFExtender: $uid, msg=${ex.getMessage}")
        throw new RuntimeException(ex)
    }
  }
}
