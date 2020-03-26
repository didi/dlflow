package com.didi.dm.dmflow.feature.flow.operator

import com.didi.dm.dmflow.config.FeatureFlowConfig.GlobalSettings
import com.didi.dm.dmflow.feature.flow.param.{FillnaConfig, MissingValueFillerParams, StepParams}
import com.didi.dm.dmflow.feature.flow.{BaseOperator, Step}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._


class MissingValueFiller(override val uid: String,
                         @transient override val meta: Step,
                         @transient override val settings: GlobalSettings,
                         override val ckpBaseHDFS: String
                        ) extends BaseOperator(uid, meta, settings, ckpBaseHDFS) {

  @transient
  val extraParams: MissingValueFillerParams = StepParams.toMissingValueFillerParams(meta.params)

  protected def getNames[T <: DataType](schema: StructType, exludeNames: Set[String])(implicit m: Manifest[T]): List[String] = {
    val runtimeTypeName = m.runtimeClass.getTypeName
    val names = schema.fields
      .zipWithIndex
      .filter(_._1.dataType.getClass.getTypeName.contains(m.runtimeClass.getTypeName))
      .filterNot(x => exludeNames.contains(x._1.name))
      .map(_._1.name)
      .toList
    logDebug(s"Feature of $runtimeTypeName size=${names.size} names=${names.mkString("[", ",", "]")}")
    names
  }

  def fillna[T <: DataType, U](df: DataFrame,
                               cfg: FillnaConfig[U],
                               fillFunc: (DataFrame, U, List[String]) => DataFrame
                              )(implicit c: Manifest[T], m: Manifest[U]): DataFrame = {

    val runtimeTypeName = m.runtimeClass.getTypeName
    val totalCols = getNames[T](df.schema, extraParams.excludeCols.toSet).toSet
    if (cfg.default.isDefined || cfg.mapping.nonEmpty) {
      var outDF = df

      val outliers = scala.collection.mutable.HashSet[String]()
      if (cfg.mapping.nonEmpty) {
        cfg.mapping.foreach { case (cols, value) =>
          logInfo(s"featureType: $runtimeTypeName fillna with specified: $value, cols: ${cols.mkString(",")}")
          outDF = fillFunc(outDF, value, cols)
          cols.foreach(outliers.add)
        }
      }

      val remainders = totalCols.diff(outliers)
      if (remainders.nonEmpty && cfg.default.isDefined) {
        val default = cfg.default.get
        logInfo(s"featureType: $runtimeTypeName fillna with default: $default, cols: ${remainders.mkString(",")}")
        outDF = fillFunc(outDF, default, remainders.toList)
      }

      outDF

    } else {
      logDebug(s"No valid config for fillna of featureType: $runtimeTypeName")
      df
    }
  }

  override def process(inDF: DataFrame): DataFrame = {
    var outDF = inDF
    outDF = fillna[IntegerType, Int](outDF, extraParams.getIntMappings, (df: DataFrame, v: Int, cols: List[String]) => df.na.fill(v, cols))
    outDF = fillna[LongType, Long](outDF, extraParams.getLongMappings, (df: DataFrame, v: Long, cols: List[String]) => df.na.fill(v, cols))
    outDF = fillna[FloatType, Float](outDF, extraParams.getFloatMappings, (df: DataFrame, v: Float, cols: List[String]) => df.na.fill(v, cols))
    outDF = fillna[DoubleType, Double](outDF, extraParams.getDoubleMappings, (df: DataFrame, v: Double, cols: List[String]) => df.na.fill(v, cols))
    outDF = fillna[NumericType, Double](outDF, extraParams.getDoubleMappings, (df: DataFrame, v: Double, cols: List[String]) => df.na.fill(v, cols))
    outDF = fillna[StringType, String](outDF, extraParams.getStrMappings, (df: DataFrame, v: String, cols: List[String]) => df.na.fill(v, cols))
    outDF
  }
}