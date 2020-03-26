package com.didi.dm.dmflow.param

import java.time.LocalDate

import com.didi.dm.dmflow.pojo.DTO
import com.didi.dm.dmflow.pojo.DTO.BoolRet
import com.google.common.base.Strings
import org.apache.spark.ml.param.{BooleanParam, Params, StringArrayParam}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

trait HasPrimaryKeys extends Params {

  final val primaryKeysParam: StringArrayParam = new StringArrayParam(this, "primaryKeysParam", "主键集合")

  final def getPrimaryKeys: List[String] = $(primaryKeysParam).toList

  setDefault(primaryKeysParam, List[String]().toArray)

  final def hasPrimaryKeys: Boolean = getPrimaryKeys.nonEmpty

  final def setPrimaryKeys(value: List[String]): this.type = {
    val PKs = value.filter(_.nonEmpty).distinct.toArray
    require(PKs.nonEmpty, s"设置 primaryKeysParam 过滤后为空：${value.mkString("[", ",", "]")}")
    set(primaryKeysParam, PKs)
  }

  final def checkPrimaryKeys(df: DataFrame): DTO.BoolRet = {
    require(getPrimaryKeys.nonEmpty, "准备检查 dataframe 唯一性，但未设置：primaryKeysParam")
    val outliers = getPrimaryKeys.toSet -- df.columns.toSet

    if (outliers.isEmpty) {
      BoolRet(ret = true)
    } else {
      BoolRet(ret = false, s"设置的 primaryKeysParam 在数据集中未全部找到，缺失的主键：${outliers.mkString("[", ",", "]")}")
    }
  }

}

trait HasUniqueCheck extends HasPrimaryKeys {

  final val uniqueCheckParam: BooleanParam = new BooleanParam(this, "uniqueCheckParam", "是否去重检查")

  final def isUniqueCheck: Boolean = $(uniqueCheckParam)

  setDefault(uniqueCheckParam, false)

  final def setUniqueCheck(value: Boolean): this.type = set(uniqueCheckParam, value)

  final def checkUnique(df: DataFrame): BoolRet = {
    val pkcheckRet = checkPrimaryKeys(df)
    require(pkcheckRet.ret, s"准备做唯一性校验，但是主键有问题 ${pkcheckRet.msg}")

    if (df.storageLevel == StorageLevel.NONE) df.persist(StorageLevel.DISK_ONLY)
    val orginSize = df.count()
    val uniqSize = df.dropDuplicates(getPrimaryKeys).count()

    if (orginSize == uniqSize) {
      BoolRet(ret = true)
    } else {
      BoolRet(ret = false, s"存在重复的值（origin: $orginSize -> unique: $uniqSize）")
    }

  }

}

trait HasHdfsSource extends Params {

  val HDFS_FORMATs: Set[String] = Set[String]("parquet", "orc", "json")
  val DEFAULT_HDFS_FORMAT = "parquet"

  def isValidSource: String => Boolean = { value: String => HDFS_FORMATs.contains(value) }

  final val hdfsSourceParam: StringParam = new StringParam(this, "hdfsSourceParam", "HDFS 数据格式", isValidSource)

  final def getHdfsSource: String = $(hdfsSourceParam)

  setDefault(hdfsSourceParam, DEFAULT_HDFS_FORMAT)

  def setHdfsSource(value: String): this.type = {
    require(isValidSource(value), s"输入的 hdfsSourceParam 不支持！now=$value accept=${HDFS_FORMATs.mkString("[", ",", "]")}")
    set(hdfsSourceParam, value)
  }

}

trait HasInputPath extends Params {

  final val inputPathParam: StringParam = new StringParam(this, "output", "产出地址", DMParamValidators.nonEmpty)

  final def geInputPath: String = $(inputPathParam)

  final def setInputPath(value: String): this.type = {
    require(DMParamValidators.nonEmpty(value), "设置 inputPathParam 字符串不能为空")
    set(inputPathParam, value.trim)
  }

}

trait HasOutputPath extends Params {

  final val outputPathParam: StringParam = new StringParam(this, "output", "产出地址", DMParamValidators.nonEmpty)

  final def getOutputPath: String = $(outputPathParam)

  final def setOutputPath(value: String): this.type = {
    require(DMParamValidators.nonEmpty(value), "设置 outputParam 字符串不能为空")
    set(outputPathParam, value.trim)
  }

}

trait HasCheckpoint extends Params {

  final val checkpointParam: OptStringParam = new OptStringParam(this, "checkpoint",
    "中间落盘的checkpoint地址 (optional，传入None表示不设置产出地址，但不允许输入空字符串)",
    DMParamValidators.nonEmpty4opt)

  setDefault(checkpointParam, None)

  final def getCheckpoint: Option[String] = $(checkpointParam)

  final def hasCheckpoint: Boolean = getCheckpoint.isDefined

  def setCheckpoint(value: String): this.type = {
    require(!Strings.isNullOrEmpty(value), "设置 checkpointParam 字符串不能为空")
    set(checkpointParam, Some(value))
  }

}

trait HasOverwrite extends Params {

  final val overwriteParam: BooleanParam = new BooleanParam(this, "overwrite", "是否覆盖已有地址")

  setDefault(overwriteParam, false)

  final def isOverwrite: Boolean = $(overwriteParam)

  def setOverwrite(value: Boolean): this.type = set(overwriteParam, value)

}

trait HasEmptyCheck extends Params {

  final val checkEmptyParam: BooleanParam = new BooleanParam(this, "checkEmpty", "是否对数据进行非空检查")

  setDefault(checkEmptyParam, true)

  final def isCheckEmpty: Boolean = $(checkEmptyParam)

  def setCheckEmpty(value: Boolean): this.type = set(checkEmptyParam, value)

}

trait HasSubstitutions extends Params {

  final val substitutionsParam: StringArrayParam = new StringArrayParam(this, "substitutions", "占位符集合")

  final def getSubstitutions: List[String] = $(substitutionsParam).toList

  setDefault(substitutionsParam, List[String]().toArray)

  final def hasSubstitutions: Boolean = getSubstitutions.nonEmpty

  final def setSubstitutions(value: List[String]): this.type = {
    val keys = value.filter(_.nonEmpty).distinct.toArray
    require(keys.nonEmpty, s"设置 substitutionsParam 过滤后为空：${value.mkString("[", ",", "]")}")
    set(substitutionsParam, keys)
  }

}

trait HasSubstitutionsDict extends Params {

  final val substitutionsDictParam: StrMapParam = new StrMapParam(this, "substitutionsDict", "占位符字典")

  final def getSubstitutionsDict: Map[String, String] = $(substitutionsDictParam)

  setDefault(substitutionsDictParam, Map.empty[String, String])

  final def setSubstitutions(value: Map[String, String]): this.type = set(substitutionsDictParam, value)

}

trait HasConfigPath extends Params {

  final val configPathParam: StringParam = new StringParam(this, "configPath", "file path of config", DMParamValidators.localFileExists)

  final def getConfigPath: String = $(configPathParam)

  final def setConfigPath(value: String): this.type = {
    require(DMParamValidators.nonEmpty(value), "设置 configPathParam 字符串不能为空")
    set(configPathParam, value.trim)
  }

}

trait HasExecuteDate extends Params {

  final val executeDateParam: DateParam = new DateParam(this, "executeDate", "execute date for identify feature date")

  final def getExecuteDate: LocalDate = $(executeDateParam)

  final def setExecuteDate(value: LocalDate): this.type = set(executeDateParam, value)

}

