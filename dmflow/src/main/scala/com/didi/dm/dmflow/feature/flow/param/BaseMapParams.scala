package com.didi.dm.dmflow.feature.flow.param

import com.didi.dm.dmflow.base.JsonWritable
import com.didi.dm.dmflow.config.ConfigHelper
import com.didi.dm.dmflow.types.FeatureFlowType
import com.typesafe.config.Config
import org.json4s.JValue

abstract class BaseMapParams(paramMap: Map[String, Any], requiredKeys: List[String]) extends JsonWritable {
  require(validateRequireKeys, s"""$simpleClazzName 必选参数缺失：<${getMissingRequireKeys.mkString(",")}>, inParams: <$showParams>""")

  def isEmpty: Boolean = this.paramMap.isEmpty

  def showParams: String = this.paramMap.keys.mkString(",")

  protected def validateRequireKeys: Boolean = requiredKeys.forall(paramMap.get(_).isDefined)

  protected def getMissingRequireKeys: Set[String] = requiredKeys.toSet -- paramMap.keys.toSet

  protected def hasKey(key: String): Boolean = paramMap.get(key).isDefined

  protected def getRequiredParam[T](key: String): T = {
    require(hasKey(key), s"$simpleClazzName 获取参数失败, key=$key 不存在!")
    paramMap(key).asInstanceOf[T]
  }

  protected def getParamWithDefault[T](key: String, default: T): T = {
    paramMap.getOrElse(key, default).asInstanceOf[T]
  }

  protected def getOptionalParam[T](key: String): Option[T] = if (hasKey(key)) Some(getRequiredParam[T](key)) else None


}

object StepParams {


  def fromConfig(config: Config, path: String, opType: FeatureFlowType): BaseMapParams = parse(ConfigHelper.getJValue(config, path), opType)

  def parse(jval: JValue, flowType: FeatureFlowType): BaseMapParams = flowType match {
    case FeatureFlowType.SQL_Assembler => SQLAssemblerParams.fromJValue(jval)
    case FeatureFlowType.UDF_Assembler => UDFAssemblerParams.fromJValue(jval)
    case FeatureFlowType.MissingValueCounter => MissingValueCounterParams.fromJValue(jval)
    case FeatureFlowType.MissingValueFiller => MissingValueFillerParams.fromJValue(jval)
    case _ => throw new IllegalArgumentException(s"Cannot create FeatureFlowParams by FeatureFlowType: ${flowType.name}")
  }

  protected def castBaseMapParams[T](params: Option[BaseMapParams])(implicit m: Manifest[T]): T = {
    val runtimeTypeName = m.runtimeClass.getTypeName
    require(params.isDefined, s"parse BaseMapParams to $runtimeTypeName but input is None")
    params.get.asInstanceOf[T]
  }

  implicit def toSQLAssemblerParams(params: Option[BaseMapParams]): SQLAssemblerParams = castBaseMapParams[SQLAssemblerParams](params)

  implicit def toUDFAssemblerParams(params: Option[BaseMapParams]): UDFAssemblerParams = castBaseMapParams[UDFAssemblerParams](params)

  implicit def toMissingValueCounterParams(params: Option[BaseMapParams]): MissingValueCounterParams = castBaseMapParams[MissingValueCounterParams](params)

  implicit def toMissingValueFillerParams(params: Option[BaseMapParams]): MissingValueFillerParams = castBaseMapParams[MissingValueFillerParams](params)

}
