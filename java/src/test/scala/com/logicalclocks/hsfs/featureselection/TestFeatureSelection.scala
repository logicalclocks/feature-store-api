/**
 * Copyright 2021 Logical Clocks AB. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.featureselection

import com.logicalclocks.hsfs.utils.FixtureSupport
import com.logicalclocks.hsfs.SparkContextSpec
import org.apache.spark.sql.functions.{count, mean, min, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, functions}
import org.apache.spark.storage.StorageLevel
import org.scalatest.{Matchers, WordSpec}

class TestFeatureSelection extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "Feature Selection" should {

    "FeatureSelectionHelper on generated data" in
      withSparkSession { sparkSession =>

        val tc = System.nanoTime

        val nVal = 10
        val nTargetBins = 100
        var df = sparkSession.read.format("parquet").load(
          f"test-data/features_int_10k_$nVal.parquet")
        df = df.withColumn("target",
          functions.round(functions.rand(10) * lit(nTargetBins)).cast(IntegerType))

        df.persist(StorageLevel.MEMORY_AND_DISK_SER)
        df.count()

        val durationc = (System.nanoTime - tc) / 1e9d
        println(f"read data x $durationc")

        val selectedFeatures = FeatureSelectionEngine.runFeatureSelection(df,
          config = FeatureSelectionConfig(nSelectFeatures = 1,
            numPartitions = df.rdd.getNumPartitions,
            verbose = true))
      }

    "FeatureSelectionHelper on titanic" in
      withSparkSession { sparkSession =>

        val tc = System.nanoTime

        val df = sparkSession.read.format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load("test-data/titanic.csv")

        df.persist(StorageLevel.MEMORY_AND_DISK_SER)
        df.count()

        val durationc = (System.nanoTime - tc) / 1e9d
        println(f"read data x $durationc")


        val selectedFeatures = FeatureSelectionEngine.runFeatureSelection(df,
          config = FeatureSelectionConfig(nSelectFeatures = -1,
            target = "Survived",
            numPartitions = df.rdd.getNumPartitions,
            verbose = true))
      }

    "FeatureSelectionHelper on different data types should not crash" in
      withSparkSession { sparkSession =>

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

        var df = sparkSession.createDataFrame(
          sparkSession.sparkContext.parallelize(dataList),
          schema
        )

        val statsrow = FeatureSelectionStatisticsEngine.computeStatistics(df,
          RDDStatisticsConfig(frequentItems = false))

        println(statsrow.min.mkString(" "))
        println(statsrow.max.mkString(" "))

        val selectedFeatures = FeatureSelectionEngine.runFeatureSelection(df,
          FeatureSelectionConfig(nSelectFeatures = -1,
            numPartitions = df.rdd.getNumPartitions,
            target = "bool",
            verbose = true,
            nBuckets = 200))
      }

    "DataFrame-based stats equal ExtendedMultivariateOnlineSummarize stats" in
      withSparkSession { sparkSession =>

        val nVal = 10
        val df = sparkSession.read.format("parquet").load(f"test-data/features_int_10k_$nVal" +
          f".parquet")
        df.persist(StorageLevel.MEMORY_AND_DISK_SER)
        df.count()

        val numericColumns = df.schema.filter(kv => {
          kv.dataType match {
            case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
                 DoubleType | TimestampType | DateType | DecimalType() => true
            case _ =>
              false
          }
        })

        val otherColumns = df.schema.filterNot(kv => {
          kv.dataType match {
            case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
                 DoubleType | TimestampType | DateType | DecimalType() => true
            case _ =>
              false
          }
        })

        val numStatsAggs = numericColumns.flatMap(c => {
          val withoutNullAndNan = when(!col(c.name).isNull && !col(c.name).isNaN, col(c.name))
          Seq(
            min(withoutNullAndNan).cast(DoubleType).alias(c.name + "_min"),
            max(withoutNullAndNan).cast(DoubleType).alias(c.name + "_max"),
            approx_count_distinct(col(c.name)).alias(c.name + "_dist"),
            count(when(col(c.name).isNull || col(c.name).isNaN, lit(1)))
              .alias(c.name + "_count_null"),
            stddev(withoutNullAndNan).alias(c.name + "_stddev"),
            mean(withoutNullAndNan).alias(c.name + "_mean"))
        })
        val otherStatsAggs = otherColumns
          .flatMap(c => Seq(
            approx_count_distinct(col(c.name)).alias(c.name + "_dist"),
            count(when(col(c.name).isNull, lit(1))).alias(c.name + "_count_null")))
        val generalCount = Seq(count(lit(1)).alias("_count"))

        val stats = df.select(numStatsAggs ++ otherStatsAggs ++
          generalCount: _*)
        stats.show()

        val statsrow = FeatureSelectionStatisticsEngine.computeStatistics(df,
          RDDStatisticsConfig(frequentItems = false))

        println(statsrow.min.mkString(" "))
        println(statsrow.max.mkString(" "))
        println(statsrow.approxDistinct.get.mkString(" "))
      }
  }
}
