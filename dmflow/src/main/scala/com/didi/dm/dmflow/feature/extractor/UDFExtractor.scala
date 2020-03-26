package com.didi.dm.dmflow.feature.extractor

import com.didi.dm.dmflow.base.Reflective
import com.didi.dm.dmflow.param.StringParam
import com.didi.dm.dmflow.pojo.DTO.BoolRet
import com.google.common.base.Strings
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.json4s.{DefaultFormats, JValue}

import scala.util.{Failure, Success, Try}

trait UDFExtractorParams extends Params {

  final val classNameParam: StringParam = new StringParam(this, "classNameParam", "UDF类的className（需实现 com.didi.dm.dmflow.feature.extractor.IFeatureUDF）")

  final def getClassName: String = $(classNameParam)

  final def setClassName(value: String): this.type = {
    require(!Strings.isNullOrEmpty(value), "设置 classNameParam 不能为空")
    set(classNameParam, value)
  }

}

class UDFExtractor(override val uid: String) extends BaseExtractor(uid = uid)
  with UDFExtractorParams
  with Reflective {

  def this() = this(Identifiable.randomUID("UDFExtractor"))

  override protected def checkRequiredArguments(): BoolRet = {
    Try {
      require(!Strings.isNullOrEmpty(getClassName), s" classNameParam is null or empty")
    } match {
      case Success(_) => BoolRet(ret = true)
      case Failure(e) => BoolRet(ret = false, msg = s"check UDFExtractor=$uid required arguments failed： ${e.getMessage}")
    }
  }

  override def extract(paramMap: Map[String, Any]): DataFrame = {
    require(isDefined(classNameParam), "SET classNameParam before using UDFExtractor!")

    val className = getClassName
    logDebug(s"try to create Object of clazz：$className")

    val caller = this.create[IFeatureUDF](className)

    require(caller.validate(paramMap), s"UDFExtractor (clazz: $className) runtime arguments check failed! ")

    caller.invoke(paramMap, getSubstitutionsDict)
  }


}

object UDFExtractor {

  def load(jval: JValue): UDFExtractor = {
    implicit val format: DefaultFormats.type = DefaultFormats
    (new UDFExtractor)
      .setUniqueCheck((jval \ "uniqueCheck").extract[Boolean])
      .setPrimaryKeys((jval \ "primaryKeys").extract[List[String]])
      .setClassName((jval \ "clazz").extract[String])
  }

}
