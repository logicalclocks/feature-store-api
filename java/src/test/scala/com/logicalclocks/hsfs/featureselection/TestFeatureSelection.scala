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

    "FeatureSelectionHelper on synthetic data" in
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
          config = FeatureSelectionConfig(nSelectFeatures = 3,
            numPartitions = df.rdd.getNumPartitions,
            verbose = true))

        val groundTruth = Array("att10", "att8", "att7")
        val actual = selectedFeatures.map(kv => kv._1).toArray

        groundTruth should contain theSameElementsInOrderAs actual
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

        val groundTruth = Array("Sex", "Fare", "Age", "Pclass", "SibSp", "Embarked", "Parch")
        val actual = selectedFeatures.map(kv => kv._1).toArray

        groundTruth should contain theSameElementsInOrderAs actual
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
  }
}
