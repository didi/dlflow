package com.didi.dm.dmflow.common.hdfs

import org.apache.spark.sql.types._


object Dataframes {

  case class SchemaStat(numericCnt: Int, categoryCnt: Int, mapCnt: Int, arrayCnt: Int, otherCnt: Int) {
    override def toString: String = s"""SchemaStat(numeric=$numericCnt, category=$categoryCnt map=$mapCnt array=$arrayCnt) other=$otherCnt"""
  }

  def checkSchema(schema: StructType): SchemaStat = {

    var categoryCnt = 0
    var numericCnt = 0
    var mapCnt = 0
    var arrayCnt = 0
    var other = 0

    for (field <- schema) {
      field.dataType match {
        case _: StringType => categoryCnt += 1
        case _: DoubleType => numericCnt += 1
        case _: FloatType => numericCnt += 1
        case _: IntegerType => numericCnt += 1
        case _: LongType => numericCnt += 1
        case _: MapType => mapCnt += 1
        case _: ArrayType => arrayCnt += 1
        case _ => other += 1
      }
    }

    SchemaStat(
      numericCnt = numericCnt,
      categoryCnt = categoryCnt,
      mapCnt = mapCnt,
      arrayCnt = arrayCnt,
      otherCnt = other
    )
  }
}