package com.didi.dm.dmflow.common.cast

import org.apache.spark.sql.types._


object TypeConverters {
  def toStructField(colName: String, typeName: String): StructField = {

    if (typeName.startsWith("DecimalType")) {
      return StructField(colName, DecimalType(10, 2), nullable = true)
    }

    typeName match {
      case "StringType" => StructField(colName, StringType, nullable = true)
      case "IntegerType" => StructField(colName, IntegerType, nullable = true)
      case "LongType" => StructField(colName, LongType, nullable = true)
      case "DoubleType" => StructField(colName, DoubleType, nullable = true)
      case "FloatType" => StructField(colName, FloatType, nullable = true)

      case "MapType(StringType,StringType,true)" => StructField(colName, MapType(StringType, StringType, valueContainsNull = true), nullable = true)
      case "MapType(StringType,IntegerType,true)" => StructField(colName, MapType(StringType, IntegerType, valueContainsNull = true), nullable = true)
      case "MapType(StringType,LongType,true)" => StructField(colName, MapType(StringType, LongType, valueContainsNull = true), nullable = true)
      case "MapType(StringType,DoubleType,true)" => StructField(colName, MapType(StringType, DoubleType, valueContainsNull = true), nullable = true)
      case "MapType(StringType,FloatType,true)" => StructField(colName, MapType(StringType, FloatType, valueContainsNull = true), nullable = true)
      case "MapType(StringType,DecimalType,true)" => StructField(colName, MapType(StringType, DecimalType(10, 0), valueContainsNull = true), nullable = true)

      case "MapType(IntegerType,StringType,true)" => StructField(colName, MapType(IntegerType, StringType, valueContainsNull = true), nullable = true)
      case "MapType(IntegerType,IntegerType,true)" => StructField(colName, MapType(IntegerType, IntegerType, valueContainsNull = true), nullable = true)
      case "MapType(IntegerType,LongType,true)" => StructField(colName, MapType(IntegerType, LongType, valueContainsNull = true), nullable = true)
      case "MapType(IntegerType,DoubleType,true)" => StructField(colName, MapType(IntegerType, DoubleType, valueContainsNull = true), nullable = true)
      case "MapType(IntegerType,FloatType,true)" => StructField(colName, MapType(IntegerType, FloatType, valueContainsNull = true), nullable = true)
      case "MapType(IntegerType,DecimalType,true)" => StructField(colName, MapType(IntegerType, DecimalType(10, 0), valueContainsNull = true), nullable = true)

      case "MapType(LongType,StringType,true)" => StructField(colName, MapType(LongType, StringType, valueContainsNull = true), nullable = true)
      case "MapType(LongType,IntegerType,true)" => StructField(colName, MapType(LongType, IntegerType, valueContainsNull = true), nullable = true)
      case "MapType(LongType,LongType,true)" => StructField(colName, MapType(LongType, LongType, valueContainsNull = true), nullable = true)
      case "MapType(LongType,DoubleType,true)" => StructField(colName, MapType(LongType, DoubleType, valueContainsNull = true), nullable = true)
      case "MapType(LongType,FloatType,true)" => StructField(colName, MapType(LongType, FloatType, valueContainsNull = true), nullable = true)
      case "MapType(LongType,DecimalType,true)" => StructField(colName, MapType(LongType, DecimalType(10, 0), valueContainsNull = true), nullable = true)

      case "ArrayType(DoubleType,true)" | "ArrayType(DoubleType)" => StructField(colName, ArrayType(DoubleType), nullable = true)
      case "ArrayType(FloatType,true)" | "ArrayType(FloatType)" => StructField(colName, ArrayType(FloatType), nullable = true)

      case _ =>
        throw new IllegalArgumentException(s"unknow type of StructField($colName, $typeName).")
    }
  }
}
