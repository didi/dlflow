package com.didi.dm.dmflow.feature.flow.operator

import com.didi.dm.dmflow.config.FeatureFlowConfig.GlobalSettings
import com.didi.dm.dmflow.feature.flow.param.{MissingValueCounterParams, StepParams}
import com.didi.dm.dmflow.feature.flow.{BaseOperator, Step}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}


class MissingValueCounter(override val uid: String,
                          @transient override val meta: Step,
                          @transient override val settings: GlobalSettings,
                          override val ckpBaseHDFS: String
                       ) extends BaseOperator(uid, meta, settings, ckpBaseHDFS) {

  @transient
  val extraParams: MissingValueCounterParams = StepParams.toMissingValueCounterParams(meta.params)

  protected def getIdx[T <: DataType](schema: StructType, exludeIdxs: Set[Int])(implicit m: Manifest[T]): List[Int] = {
    val runtimeTypeName = m.runtimeClass.getTypeName
    val idxs = schema.fields
      .zipWithIndex
      .filter(_._1.dataType.getClass.getTypeName.contains(m.runtimeClass.getTypeName))
      .map(_._2)
      .toList
    logDebug(s"Feature of $runtimeTypeName size=${idxs.size} offsets=${idxs.mkString("[", ",", "]")}")

    idxs.filterNot(exludeIdxs.contains)
  }

  override def process(inDF: DataFrame): DataFrame = {


    val inSchema = inDF.schema

    logInfo(s"RUN FeatureValueCounter=$uid (${meta.name}) rawSchemaSize=${inSchema.fields.length} parallelism=${settings.parallelism}")


    val newSchema = inSchema
      .add("feature_stat_total_null_cnt", IntegerType)
      .add("feature_stat_total_null_pct", DoubleType)

      .add("feature_stat_str_null_cnt", IntegerType)
      .add("feature_stat_str_null_pct", DoubleType)
      .add("feature_stat_str_empty_cnt", IntegerType)
      .add("feature_stat_str_empty_pct", DoubleType)

      .add("feature_stat_numeric_null_cnt", IntegerType)
      .add("feature_stat_numeric_null_pct", DoubleType)
      .add("feature_stat_numeric_empty_cnt", IntegerType)
      .add("feature_stat_numeric_empty_pct", DoubleType)

      .add("feature_stat_double_null_cnt", IntegerType)
      .add("feature_stat_double_null_pct", DoubleType)
      .add("feature_stat_double_empty_cnt", IntegerType)
      .add("feature_stat_double_empty_pct", DoubleType)

      .add("feature_stat_float_null_cnt", IntegerType)
      .add("feature_stat_float_null_pct", DoubleType)
      .add("feature_stat_float_empty_cnt", IntegerType)
      .add("feature_stat_float_empty_pct", DoubleType)

      .add("feature_stat_int_null_cnt", IntegerType)
      .add("feature_stat_int_null_pct", DoubleType)
      .add("feature_stat_int_empty_cnt", IntegerType)
      .add("feature_stat_int_empty_pct", DoubleType)

      .add("feature_stat_long_null_cnt", IntegerType)
      .add("feature_stat_long_null_pct", DoubleType)
      .add("feature_stat_long_empty_cnt", IntegerType)
      .add("feature_stat_long_empty_pct", DoubleType)

    val size = sparkSession.sparkContext.broadcast(inDF.schema.fields.length)
    val exludeIdxs = inDF.schema.fields.zipWithIndex.filter(x => extraParams.excludeCols.contains(x._1.name)).map(_._2).toSet
    val idxs = sparkSession.sparkContext.broadcast((0 until size.value).filterNot(exludeIdxs.contains).toList)

    val strTypeIdxs = sparkSession.sparkContext.broadcast(getIdx[StringType](inSchema, exludeIdxs))
    val numericTypeIdxs = sparkSession.sparkContext.broadcast(getIdx[NumericType](inSchema, exludeIdxs))
    val doubleTypeIdxs = sparkSession.sparkContext.broadcast(getIdx[DoubleType](inSchema, exludeIdxs))
    val floatTypeIdxs = sparkSession.sparkContext.broadcast(getIdx[FloatType](inSchema, exludeIdxs))
    val intTypeIdxs = sparkSession.sparkContext.broadcast(getIdx[IntegerType](inSchema, exludeIdxs))
    val longTypeIdxs = sparkSession.sparkContext.broadcast(getIdx[LongType](inSchema, exludeIdxs))


    val rowRDD = inDF.rdd.map { row =>

      def countNull(inputRow: Row, listOfIndex: List[Int], total: Int): (Int, Double) = {
        val hits = listOfIndex.count(idx => inputRow.isNullAt(idx))
        val pct = if (total > 0) hits.toDouble / total.toDouble else 0.0
        (hits.toInt, pct.toDouble)
      }

      def countValue[T <: DataType, U](inputRow: Row, listOfIndex: List[Int], hit: U, total: Int): (Int, Double) = {
        val hits = listOfIndex.count(idx => inputRow.getAs[U](idx) == hit)
        val pct = if (total > 0) hits.toDouble / total.toDouble else 0.0
        (hits.toInt, pct.toDouble)
      }

      val (nullCnt, nullPct) = countNull(row, idxs.value, size.value)

      val (strNullCnt, strNullPct) = countNull(row, strTypeIdxs.value, size.value)
      val (strEmptyCnt, strEmptyPct) = countValue[StringType, String](row, strTypeIdxs.value, hit = "", size.value)

      val (numericNullCnt, numericNullPct) = countNull(row, numericTypeIdxs.value, size.value)
      val (numericEmptyCnt, numericEmptyPct) = countValue[NumericType, Number](row, numericTypeIdxs.value, hit = 0.0, size.value)

      val (doubleNullCnt, doubleNullPct) = countNull(row, doubleTypeIdxs.value, size.value)
      val (doubleEmptyCnt, doubleEmptyPct) = countValue[DoubleType, Double](row, doubleTypeIdxs.value, hit = 0.0, size.value)

      val (floatNullCnt, floatNullPct) = countNull(row, floatTypeIdxs.value, size.value)
      val (floatEmptyCnt, floatEmptyPct) = countValue[FloatType, Float](row, floatTypeIdxs.value, hit = 0.0.toFloat, size.value)

      val (intNullCnt, intNullPct) = countNull(row, intTypeIdxs.value, size.value)
      val (intEmptyCnt, intEmptyPct) = countValue[IntegerType, Int](row, intTypeIdxs.value, hit = 0, size.value)

      val (longNullCnt, longNullPct) = countNull(row, longTypeIdxs.value, size.value)
      val (longEmptyCnt, longEmptyPct) = countValue[LongType, Long](row, longTypeIdxs.value, hit = 0.toLong, size.value)

      val newRow = row.toSeq ++ Seq[Any](
        nullCnt, nullPct,
        strNullCnt, strNullPct, strEmptyCnt, strEmptyPct,
        numericNullCnt, numericNullPct, numericEmptyCnt, numericEmptyPct,
        doubleNullCnt, doubleNullPct, doubleEmptyCnt, doubleEmptyPct,
        floatNullCnt, floatNullPct, floatEmptyCnt, floatEmptyPct,
        intNullCnt, intNullPct, intEmptyCnt, intEmptyPct,
        longNullCnt, longNullPct, longEmptyCnt, longEmptyPct

      )
      Row.fromSeq(newRow)
    }

    inDF.sparkSession.createDataFrame(rowRDD, newSchema)

  }
}