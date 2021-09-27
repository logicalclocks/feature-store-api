/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  *     http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.logicalclocks.hsfs.utils

import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Random


trait FixtureSupport {

  def getDfWithDifferentDatatypes(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val schema = StructType(
      StructField("bool", BooleanType, true) ::
        StructField("byte", ByteType, true) ::
        StructField("short", ShortType, true) ::
        StructField("int", IntegerType, true) ::
        StructField("long", LongType, true) ::
        StructField("float", FloatType, true) ::
        StructField("dbl", DoubleType, true) ::
        StructField("dec", DecimalType(38, 28), true) ::
        StructField("ts", TimestampType, true) ::
        StructField("dt", DateType, true) ::
        StructField("str", StringType, true) ::
        StructField("bn", BinaryType, true) ::
        StructField("arr", ArrayType(IntegerType, true), true) ::
        StructField("map", MapType(StringType, StringType, true), true) ::
        StructField("struct", StructType(
          List(
            StructField("favorite_color", StringType, true),
            StructField("age", IntegerType, true)
          )
        ), true) :: Nil
    )

    import java.io.{ByteArrayOutputStream, ObjectOutputStream}
    val serialise = (value: Any) => {
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(value)
      oos.close()
      stream.toByteArray
    }

    val dataList = Seq(
      Row(
        true,
        7.toByte,
        15.toShort,
        3743,
        327828732L,
        5F,
        123.5,
        Decimal("1208484888.8474763788847476378884747637"),
        java.sql.Timestamp.valueOf("2020-06-29 22:41:30"),
        java.sql.Date.valueOf("2020-06-29"),
        "str",
        serialise("strBinary"),
        Array(17, 2, 3),
        Map("aguila" -> "Colombia", "modelo" -> "Mexico"),
        Row("blue", 45)),
      Row(
        false,
        8.toByte,
        9.toShort,
        3742,
        32728732L,
        10F,
        12.5,
        Decimal("1208484889.8474763788847476378884747637"),
        java.sql.Timestamp.valueOf("2020-06-30 22:41:30"),
        java.sql.Date.valueOf("2020-06-30"),
        "str2",
        serialise("strBinary2"),
        Array(17, 2, 3),
        Map("aguila" -> "Colombia2", "modelo" -> "Mexico2"),
        Row("brown", 46)),
      Row(
        true,
        7.toByte,
        15.toShort,
        3743,
        327828732L,
        5F,
        123.5,
        Decimal("1208484888.8474763788847476378884747637"),
        java.sql.Timestamp.valueOf("2020-06-29 22:41:30"),
        java.sql.Date.valueOf("2020-06-29"),
        "str",
        serialise("strBinary"),
        Array(1, 2, 7),
        Map("aguila" -> "Colombia", "modelo" -> "Mexico"),
        Row("blue", 45)),
      Row(
        false,
        8.toByte,
        9.toShort,
        3742,
        32728732L,
        0F,
        12.5,
        Decimal("1208484889.8474763788847476378884747637"),
        java.sql.Timestamp.valueOf("2020-06-30 22:41:30"),
        java.sql.Date.valueOf("2020-06-30"),
        "str2",
        serialise("strBinary2"),
        Array(1, 2, 7),
        Map("aguila" -> "Colombia2", "modelo" -> "Mexico2"),
        Row("brown", 46)),
      Row(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null),
      Row(
        false,
        2.toByte,
        22.toShort,
        37342,
        327228732L,
        Float.NaN,
        Double.NegativeInfinity,
        Decimal("1228384889.8474763788847476378884747637"),
        java.sql.Timestamp.valueOf("2020-07-30 22:41:30"),
        java.sql.Date.valueOf("2020-07-30"),
        "str3",
        serialise("strBinary3"),
        Array(1, 2),
        Map("aguila" -> "Colombia2", "modelo" -> "Mexico3"),
        Row("brown", 47)),
      Row(
        false,
        2.toByte,
        22.toShort,
        37342,
        327228732L,
        Float.PositiveInfinity,
        Double.NegativeInfinity,
        Decimal("1228384889.8474763788847476378884747637"),
        java.sql.Timestamp.valueOf("2020-07-30 22:41:30"),
        java.sql.Date.valueOf("2020-07-30"),
        "str3",
        serialise("strBinary3"),
        Array(1, 2),
        Map("aguila" -> "Colombia2", "modelo" -> "Mexico3"),
        Row("brown", 47))
    )

    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(dataList),
      schema
    )
  }

}
