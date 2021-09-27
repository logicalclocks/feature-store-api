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

        val df = getDfWithDifferentDatatypes(sparkSession)

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
