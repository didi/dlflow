package com.didi.dm.dmflow.feature.flow

import com.didi.dm.dmflow.base.{JsonReadable, JsonWritable}
import com.didi.dm.dmflow.config.Checkpoint
import com.didi.dm.dmflow.feature.flow.param.{BaseMapParams, StepParams}
import com.didi.dm.dmflow.types.FeatureFlowType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render
import org.json4s.{DefaultFormats, JValue}


trait OP {
  def process(inDF: DataFrame): DataFrame
}

case class Step(var name: String,
                var desc: String,
                @transient var opType: FeatureFlowType,
                @transient var checkpoint: Option[Checkpoint],
                var params: Option[BaseMapParams]
               ) extends JsonWritable with Logging {

  def this() = this(
    name = "",
    desc = "",
    opType = FeatureFlowType.SQL_Assembler,
    checkpoint = Some(Checkpoint.getDefaultCheckpoint),
    params = None
  )

  override def toJValue: JValue = {
    var metadata = ("name" -> name) ~ ("desc" -> desc) ~ ("opType" -> opType.name)
    if (checkpoint.isDefined) metadata = metadata ~ ("checkpoint" -> checkpoint.get.toJValue)
    if (params.isDefined) metadata = metadata ~ ("params" -> params.get.toJValue)
    render(metadata)
  }
}

object Step extends JsonReadable[Step] {
  override def fromJValue(jval: JValue): Step = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val opType = FeatureFlowType.fromName((jval \ "opType").extract[String])

    val getCkpFunc = (value: JValue) => Checkpoint.fromJValue(value)
    val checkpoint = parseOptWithMapping[JObject, Checkpoint](jval, "checkpoint", getCkpFunc)

    val getParamsFunc = (value: JValue) => StepParams.parse(value, opType)
    val params = parseOptWithMapping[JObject, BaseMapParams](jval, "params", getParamsFunc)

    Step(
      name = (jval \ "name").extract[String],
      desc = (jval \ "desc").extract[String],
      opType = opType,
      checkpoint = checkpoint,
      params = params
    )
  }
}
