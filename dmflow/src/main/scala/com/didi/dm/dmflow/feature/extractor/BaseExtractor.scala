package com.didi.dm.dmflow.feature.extractor

import com.didi.dm.dmflow.common.hdfs.HdfsUtility
import com.didi.dm.dmflow.param.{HasEmptyCheck, HasSubstitutionsDict, HasUniqueCheck, RequiredArguments}
import com.didi.dm.dmflow.spark.app.SparkInstance
import com.didi.dm.dmflow.spark.io.Checkpointable
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame

abstract class BaseExtractor(override val uid: String) extends SparkInstance
  with HasSubstitutionsDict
  with HasEmptyCheck
  with HasUniqueCheck
  with Checkpointable
  with RequiredArguments {

  @throws(classOf[IllegalArgumentException])
  protected def validate(df: DataFrame): DataFrame = {

    if (isCheckEmpty) {
      require(df.count() > 0, s"check Dataframe failed: count == 0")
    }

    if (getPrimaryKeys.nonEmpty) {
      val pkcheckRet = checkPrimaryKeys(df)
      require(pkcheckRet.ret, s"check Primary Keys failed: ${pkcheckRet.msg}")
    }

    if (isUniqueCheck) {
      val uniqueRet = checkUnique(df)
      require(uniqueRet.ret, s"check Unique failed:  ${uniqueRet.msg}")
    }

    df
  }

  protected def extract(paramMap: Map[String, Any]): DataFrame

  @throws(classOf[IllegalArgumentException])
  def getFeature(paramMap: Map[String, Any]): DataFrame = {
    val argsCheckRet = checkRequiredArguments()
    require(argsCheckRet.ret, s"some required arguments is missing, SET thems before call getFeature() ${argsCheckRet.msg}")

    val outDF = if (!isOverwrite && hasCheckpoint && HdfsUtility.checkCapacity(getCheckpoint.get, 0)) {
      loadCheckpoint()
    } else {
      trySaveCheckpoint(extract(paramMap))
    }

    validate(outDF)

  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

}
