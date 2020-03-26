package com.didi.dm.dmflow.feature.extractor

import com.didi.dm.dmflow.common.io.{FileUtility, Templates}
import com.didi.dm.dmflow.feature.flow.param.SQLAssemblerParams
import com.didi.dm.dmflow.param.StringParam
import com.didi.dm.dmflow.pojo.DTO.BoolRet
import com.google.common.base.Strings
import org.apache.spark.ml.param.{BooleanParam, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}


trait SQLExtractorParams extends Params {

  final val sqlFileParam: StringParam = new StringParam(this, "sqlFileParam", "sql模板文件路径")

  final def getSqlFile: String = $(sqlFileParam)

  def setSqlFile(path: String): this.type = set(sqlFileParam, path)



  final val sqlTemplateParam: StringParam = new StringParam(this, "sqlTemplateParam", "sql模板字符串")

  final def getSqlTemplate: String = $(sqlTemplateParam)

  def setSqlTemplate(path: String): this.type = set(sqlTemplateParam, path)



  final val enableSubdirParam: BooleanParam = new BooleanParam(this, "enableSubdirParam", "执行 sql 是否递归读取子目录")

  setDefault(enableSubdirParam, false)

  final def isEnableSubdir: Boolean = $(enableSubdirParam)

  def setEnableSubdir(bool: Boolean): this.type = set(enableSubdirParam, bool)

}


class SQLExtractor(override val uid: String) extends BaseExtractor(uid = uid) with SQLExtractorParams {

  def this() = this(Identifiable.randomUID("SQLExtractor"))

  override protected def checkRequiredArguments(): BoolRet = {
    Try {
      require(isDefined(sqlTemplateParam) || !Strings.isNullOrEmpty(getSqlFile),
        s"required arguments sqlTemplateParam and sqlFileParam both are missing, to SET one at least")
    } match {
      case Success(_) => BoolRet(ret = true)
      case Failure(e) => BoolRet(ret = false, msg = s"Check SQLExtractor=$uid ' s required arugments failed： ${e.getMessage}")
    }
  }

  override protected def extract(paramMap: Map[String, Any]): DataFrame = {
    val substitutions = paramMap.map(x => (x._1, x._2.toString))

    val sqlTemplate = if (isDefined(sqlTemplateParam)) {
      getSqlTemplate
    } else {
      logInfo(s"SQLExtractor load sql text from file: $getSqlFile")
      FileUtility.readLines(getSqlFile)
    }

    val sql = Templates.format(sqlTemplate, substitutions)
    logInfo(s"$uid RUN SQL:\n$sql")


    if (isEnableSubdir) {
      logInfo("SQLExtractor activate recursive and subdirectories")
      sqlContext.sql("set mapred.input.dir.recursive=true")
      sqlContext.sql("set spark.hadoop.hive.mapred.supports.subdirectories=true")
    } else {
      sqlContext.sql("set mapred.input.dir.recursive=false")
      sqlContext.sql("set spark.hadoop.hive.mapred.supports.subdirectories=false")
    }

    sparkSession.sqlContext.sql(sql)
  }

}

object SQLExtractor {

  def load(params: SQLAssemblerParams): SQLExtractor = {
    val extractor = (new SQLExtractor)
      .setSqlFile(params.sqlFile)
      .setEnableSubdir(params.enableSubdir)
    if (params.primaryKeys.isDefined) extractor.setPrimaryKeys(params.primaryKeys.get)
    if (params.uniqueCheck.isDefined) extractor.setUniqueCheck(params.uniqueCheck.get)
    extractor
  }

}
