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
import org.apache.spark.storage.StorageLevel
import org.scalatest.{Matchers, WordSpec}

class TestFeatureSelectionStats extends WordSpec with Matchers with SparkContextSpec
  with FixtureSupport {

  "Feature Selection Benchmark" should {

    "DataFrame-based stats equal ExtendedMultivariateOnlineSummarize stats" in
      withSparkSession { sparkSession =>

        val nVal = 10
        val df = sparkSession.read.format("parquet").load(f"test-data/features_int_10k_$nVal" +
          f".parquet")
        df.persist(StorageLevel.MEMORY_AND_DISK_SER)
        df.count()

        val tStatsRdd = System.nanoTime

        val statsrow = FeatureSelectionStatisticsEngine.computeStatistics(df,
          RDDStatisticsConfig(frequentItems = false, approxQuantiles = true))

        println(statsrow.min.mkString(" "))
        println(statsrow.max.mkString(" "))
        println(statsrow.approxDistinct.get.mkString(" "))
        println(statsrow.histograms.get.map({ kv => kv.mkString(" ") }).mkString(" "))
        println(statsrow.percentiles.get.map({ kv => kv.mkString(" ") }).mkString(" "))

        val durationStatsRdd = (System.nanoTime - tStatsRdd) / 1e9d
        println(f"summary stats rdd x $durationStatsRdd")

        val tStats = System.nanoTime

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


        val durationStats = (System.nanoTime - tStats) / 1e9d
        println(f"summary stats x $durationStats")

      }
  }
}
