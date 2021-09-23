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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer


case class FeatureSelectionConfig(target: String = "target",
                                  nSelectFeatures: Int = -1,
                                  rowLimit: Int = 1000000,
                                  numPartitions: Int = 0,
                                  nBuckets: Int = 255,
                                  normalizedVarianceThreshold: Double = 0.01,
                                  distinctnessThresholdIntegral: Double = 0.9,
                                  distinctnessThresholdOther: Double = 0.5,
                                  completenessThreshold: Double = 0.5,
                                  discretizationTreshold: Int = 100,
                                  frequentItemSketchSize: Int = 1024,
                                  verbose: Boolean = false
                                 ) {
  require(nBuckets <= 255, "nBuckets must be smaller or equal to 255")
  require(discretizationTreshold <= nBuckets, "discretizationTreshold must be smaller or " +
    "equal to nBuckets")
}


class FeatureSelectionRunner(schema: StructType, config: FeatureSelectionConfig =
  FeatureSelectionConfig()) extends Serializable {

  private val indexToName = ListMap(schema.zipWithIndex.map(kv => kv._2 -> kv._1.name): _*)
  private val nameToIndex = schema.zipWithIndex.map(kv => kv._1.name -> kv._2).toMap
  private val nameToType = schema.map(kv => kv.name -> kv.dataType).toMap
  private val fractionalColumns = schema.filter( kv => {
    kv.dataType match {
      case FloatType | DoubleType | DecimalType() => true
      case _ =>
        false
    }
  }).map(kv => kv.name).toSet
  private val integralColumns = schema.filter( kv => {
    kv.dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType |
           TimestampType | DateType => true
      case _ =>
        false
    }
  }).map(kv => kv.name).toSet

  def runFeatureSelection(df: DataFrame): Map[String, Double] ={
    val dfLimited = df.limit(config.rowLimit)

    // compute summary stats...
    val tStats = System.nanoTime

    val stats = StatisticsEngineRDD.computeStatistics(dfLimited,
      RDDStatisticsConfig(maxFreqItems = config.nBuckets-2))

    if (config.verbose){
      val durationStats = (System.nanoTime - tStats) / 1e9d
      println(f"summary stats x $durationStats")
    }

    //println(stats.min.toArray.mkString(" "))
    //println(stats.max.toArray.mkString(" "))
    //println(stats.freqItems.get.map(seq => seq.mkString(", ")).mkString("\n"))

    // pre-filter columns...
    val tFilter = System.nanoTime

    val selectedColumns = preFilterColumns(stats)
    val subSelectedDf = dfLimited.select(selectedColumns.map(name => col(name)): _*)

    if (config.verbose) {
      val durationFilter = (System.nanoTime - tFilter) / 1e9d
      println(f"pre-filter columns x $durationFilter")
    }

    // TODO: broadcasting seems to be suboptimal here, but check on larger cluster/data
    //val sc = subSelectedDf.rdd.context
    //val bStats = sc.broadcast(stats)

    // transform values...
    val tTransform = System.nanoTime

    val transformedColumns = transformColumns(subSelectedDf.rdd, selectedColumns, stats)
    transformedColumns.persist(StorageLevel.MEMORY_AND_DISK_SER)
    transformedColumns.count()

    if (config.verbose) {
      val durationTransform = (System.nanoTime - tTransform) / 1e9d
      println(f"transform values x $durationTransform")
    }

    // select features...
    val tSelect = System.nanoTime

    val indexToFeatures = ListMap(selectedColumns.zipWithIndex.map(kv => kv._2 -> kv._1): _*)
    val selectedFeatures = FeatureSelectorMRMR.selectFeatures(transformedColumns,
                               config.nSelectFeatures,
                               selectedColumns.length,
                               indexToFeatures,
                               config.verbose)

    if (config.verbose) {
      val durationSelect = (System.nanoTime - tSelect) / 1e9d
      println(f"select features x $durationSelect")
    }

    selectedFeatures
  }

  // feature pre-filter (except target)
  private def preFilterColumns(stats: RDDStatistics)
  : Array[String] = {
    val selectedColumns = indexToName.filter(kv => kv._2 != config.target).filterNot(kv => {
      val index = kv._1
      val name = kv._2
      val distinct = stats.approxDistinct.get(index)
      val count = stats.count
      val countNull = stats.count - stats.numNonzeros(index)
      val countSafe = if (count == 0) 1 else count
      val noVariance = distinct == 1
      val completeness = (count - countNull) / countSafe
      val lowCompleteness = completeness < config.completenessThreshold
      var lowVariance = false
      val distinctness = (distinct / countSafe)
      var highDistinctnessIntegral = false
      var highDistinctness = false
      var noFrequentItems = false
      var normVariance = 0.0
      if (fractionalColumns.contains(name) || integralColumns.contains(name)) {
        val stddev = stats.variance(index) * stats.variance(index)
        val mean = stats.mean(index)
        val meanSafe = if (mean == 0) 1e-100 else mean
        normVariance = (stddev / meanSafe) * (stddev / meanSafe)
        lowVariance = normVariance < config.normalizedVarianceThreshold
        if (integralColumns.contains(name)) {
          highDistinctnessIntegral = distinctness > config.distinctnessThresholdIntegral
        }
      } else {
        highDistinctness = distinctness > config.distinctnessThresholdOther
        noFrequentItems = stats.freqItems.get(index).size < 2
      }
      val filter = noVariance || lowCompleteness || lowVariance || highDistinctness |
        highDistinctnessIntegral || noFrequentItems

      if (filter && config.verbose) {
        val reasons = ArrayBuffer[String]()
        if (noVariance) reasons += f"variance is 0"
        if (lowCompleteness) reasons += f"completeness $completeness < ${
          config.completenessThreshold  }"
        if (lowVariance) reasons += f"variance $normVariance < ${
          config.normalizedVarianceThreshold }"
        if (highDistinctnessIntegral) reasons += f"integral distinctness $distinctness > ${
          config.distinctnessThresholdIntegral }"
        if (highDistinctness) reasons += f"distinctness $distinctness > ${
          config.distinctnessThresholdOther }"
        if (noFrequentItems) reasons += f"less than two most frequent items"
        println(f"Column $name was pre-filtered, because ${reasons.mkString(", ")}")
      }

      filter
    }).values.toArray :+ config.target

    selectedColumns
  }


  private def transformColumns(rdd: RDD[Row], selectedColumns: Array[String],
                               stats: RDDStatistics): RDD[(Long, Byte)]
  = {
    val nAllFeatures = selectedColumns.length

    val columnarData: RDD[(Long, Byte)] = rdd.zipWithIndex().flatMap ({ kv =>
      val values = kv._1
      val r = kv._2
      val rindex = r * nAllFeatures
      val inputs = for(i <- 0 until nAllFeatures) yield {
        val name = selectedColumns(i)
        val statsIndex = nameToIndex(name)
        val index = rindex + i
        var byte = 0.toByte
        if (!values.isNullAt(i)) {
          val value = NumerizationHelper.numerize(values, i, nameToType(name))
          if (Seq(Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN).contains(value)) {
            byte = 0.toByte
          } else if(fractionalColumns.contains(name) || (integralColumns.contains(name) &&
            stats.approxDistinct.get(statsIndex) > config.discretizationTreshold )) {
            val mn = stats.min(statsIndex)
            val range = stats.max(statsIndex) - stats.min(statsIndex)
            val scaled = ((value - mn) / range) * (config.nBuckets - 2) + 1
            byte = scaled.toByte
          } else {
            val lookup = stats.freqItems.get(statsIndex)
            // hash-lookups-> 0-253; miss: -1 => range (-1-253)+2 = 1-255 (leaves 0-index for nulls)
            byte = (lookup.getOrElse(value.toLong, -1) + 2).toByte
          }
        }
        (index, byte)
      }
      inputs
    })

    val nPart = if(config.numPartitions == 0) rdd.context.getConf.getInt(
      "spark.default.parallelism", 500) else config.numPartitions

    columnarData.sortByKey(numPartitions = nPart)
  }


}

object FeatureSelectionRunner {

  def runFeatureSelection(df: DataFrame, config: FeatureSelectionConfig =
      FeatureSelectionConfig()): Map[String, Double] ={
    val runner = new FeatureSelectionRunner(df.schema, config)
    runner.runFeatureSelection(df)
  }

  def runFeatureSelectionJava(df: DataFrame, config: FeatureSelectionConfig =
      FeatureSelectionConfig()): java.util.Map[java.lang.String, java.lang.Double] = {
    val selectedFeatures = runFeatureSelection(df, config)
    JavaConverters.mapAsJavaMapConverter(selectedFeatures.map(kv => kv._1 -> double2Double(kv._2))).asJava
  }

}
