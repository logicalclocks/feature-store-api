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
 * based on org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
 */

package com.logicalclocks.hsfs.featureselection

import org.apache.datasketches.frequencies.{ErrorType, LongsSketch}
import org.apache.datasketches.hll.{HllSketch => jHllSketch, Union => HllUnion}
import org.apache.datasketches.kll.{KllFloatsSketch => jKllFloatsSketch}
import org.apache.datasketches.memory.Memory
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
 * ExtendedMultivariateOnlineSummarizer implements [[ExtendedMultivariateOnlineSummarizer]] to
 * compute the mean, variance, minimum, maximum, counts, approx distinctness, frequent items,
 * and nonzero counts for instances in RDD[Row] format in an online fashion.
 *
 * Two ExtendedMultivariateOnlineSummarizer can be merged together to have a statistical summary of
 * the corresponding joint dataset.
 *
 * A numerically stable algorithm is implemented to compute the mean and variance of instances:
 * Reference: <a href="http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance">
 * variance-wiki</a>
 * Zero elements (including explicit zero values) are skipped when calling add(),
 * to have time complexity O(nnz) instead of O(n) for each column.
 *
 * For weighted instances, the unbiased estimation of variance is defined by the reliability
 * weights:
 * see <a href="https://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Reliability_weights">
 * Reliability weights (Wikipedia)</a>.
 */
class ExtendedMultivariateOnlineSummarizer extends Serializable{

  var dataTypes: Array[DataType] = _
  var configuration: RDDStatisticsConfig = _
  @transient
  var longsSketches: IndexedSeq[LongsSketch] = _
  var longsSketchesBin: IndexedSeq[Array[Byte]] = _
  @transient
  var hllSketches: IndexedSeq[jHllSketch] = _
  var hllSketchesBin: IndexedSeq[Array[Byte]] = _
  //var kllSketches: IndexedSeq[QuantileNonSample[Double]] = _
  @transient
  var kllSketches: IndexedSeq[jKllFloatsSketch] = _
  var kllSketchesBin: IndexedSeq[Array[Byte]] = _
  var n = 0
  var currMean: Array[Double] = _
  var currM2n: Array[Double] = _
  var currM2: Array[Double] = _
  var currL1: Array[Double] = _
  var totalCnt: Long = 0
  var totalWeightSum: Double = 0.0
  var weightSquareSum: Double = 0.0
  var currWeightSum: Array[Double] = _
  var nnz: Array[Long] = _
  var currMax: Array[Double] = _
  var currMin: Array[Double] = _

  def this(vectorSize: Int, dataTypes: Array[DataType], statsConfig: RDDStatisticsConfig) {
    this()

    this.dataTypes = dataTypes
    configuration = statsConfig
    if (configuration.frequentItems) {
      longsSketches = (0 to vectorSize).map( i => new LongsSketch(statsConfig.freqItemSketchSize) )
    }
    if (configuration.approxDistinctness) {
      hllSketches = (0 to vectorSize).map( i => new jHllSketch() )
    }
    if (configuration.approxQuantiles) {
      //kllSketches = (0 to vectorSize).map( i => new QuantileNonSample[Double](statsConfig
      //  .kllSketchSize, statsConfig.kllSketchShrinkingFactor))
      kllSketches = (0 to vectorSize).map( i => new jKllFloatsSketch(statsConfig.kllSketchSize))
    }
  }

  /**
   * Add a new sample to this summarizer, and update the statistical summary.
   *
   * @param sample The sample in dense/sparse vector format to be added into this summarizer.
   * @return This MultivariateOnlineSummarizer object.
   */
  def add(sample: Row): this.type = add(sample, 1.0)

  private def add(instance: Row, weight: Double): this.type = {
    require(weight >= 0.0, s"sample weight, ${weight} has to be >= 0.0")
    if (weight == 0.0) return this

    if (n == 0) {
      require(instance.size > 0, s"Vector should have dimension larger than zero.")
      n = instance.size

      currMean = Array.ofDim[Double](n)
      currM2n = Array.ofDim[Double](n)
      currM2 = Array.ofDim[Double](n)
      currL1 = Array.ofDim[Double](n)
      currWeightSum = Array.ofDim[Double](n)
      nnz = Array.ofDim[Long](n)
      currMax = Array.fill[Double](n)(Double.MinValue)
      currMin = Array.fill[Double](n)(Double.MaxValue)
    }

    require(n == instance.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting ${n} but got ${instance.size}.")

    val localCurrMean = currMean
    val localCurrM2n = currM2n
    val localCurrM2 = currM2
    val localCurrL1 = currL1
    val localWeightSum = currWeightSum
    val localNumNonzeros = nnz
    val localCurrMax = currMax
    val localCurrMin = currMin
    val nans = if (configuration.treatInfinityAsNaN) Seq(Double.NegativeInfinity,
      Double.PositiveInfinity, Double.NaN) else Seq(Double.NaN)
    (0 until n).foreach { index =>
      if (!instance.isNullAt(index)) {
        val value = NumerizationHelper.numerize(instance, index, dataTypes(index))
        if (!nans.contains(value)) {
          if (configuration.frequentItems) {
            longsSketches(index).update(value.toLong)
          }
          if (configuration.approxDistinctness) {
            hllSketches(index).update(value)
          }
          if (configuration.approxQuantiles) {
            //kllSketches(index).update(value)
            kllSketches(index).update(value.toFloat)
          }
          if (localCurrMax(index) < value) {
            localCurrMax(index) = value
          }
          if (localCurrMin(index) > value) {
            localCurrMin(index) = value
          }

          val prevMean = localCurrMean(index)
          val diff = value - prevMean
          localCurrMean(index) = prevMean + weight * diff / (localWeightSum(index) + weight)
          localCurrM2n(index) += weight * (value - localCurrMean(index)) * diff
          localCurrM2(index) += weight * value * value
          localCurrL1(index) += weight * math.abs(value)

          localWeightSum(index) += weight
          localNumNonzeros(index) += 1
        }
      }
    }

    totalWeightSum += weight
    weightSquareSum += weight * weight
    totalCnt += 1
    this
  }

  /**
   * Merge another MultivariateOnlineSummarizer, and update the statistical summary.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other MultivariateOnlineSummarizer to be merged.
   * @return This MultivariateOnlineSummarizer object.
   */
  def merge(other: ExtendedMultivariateOnlineSummarizer): ExtendedMultivariateOnlineSummarizer
  = {
    if (totalWeightSum != 0.0 && other.totalWeightSum != 0.0) {
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting ${n} but got ${other.n}.")
      totalCnt += other.totalCnt
      totalWeightSum += other.totalWeightSum
      weightSquareSum += other.weightSquareSum
      var i = 0
      while (i < n) {
        val thisNnz = currWeightSum(i)
        val otherNnz = other.currWeightSum(i)
        val totalNnz = thisNnz + otherNnz
        val totalCnnz = nnz(i) + other.nnz(i)
        if (totalNnz != 0.0) {
          val deltaMean = other.currMean(i) - currMean(i)
          // merge mean together
          currMean(i) += deltaMean * otherNnz / totalNnz
          // merge m2n together
          currM2n(i) += other.currM2n(i) + deltaMean * deltaMean * thisNnz * otherNnz / totalNnz
          // merge m2 together
          currM2(i) += other.currM2(i)
          // merge l1 together
          currL1(i) += other.currL1(i)
          // merge max and min
          currMax(i) = math.max(currMax(i), other.currMax(i))
          currMin(i) = math.min(currMin(i), other.currMin(i))
        }
        currWeightSum(i) = totalNnz
        nnz(i) = totalCnnz
        i += 1
      }
    } else if (totalWeightSum == 0.0 && other.totalWeightSum != 0.0) {
      n = other.n
      currMean = other.currMean.clone()
      currM2n = other.currM2n.clone()
      currM2 = other.currM2.clone()
      currL1 = other.currL1.clone()
      totalCnt = other.totalCnt
      totalWeightSum = other.totalWeightSum
      weightSquareSum = other.weightSquareSum
      currWeightSum = other.currWeightSum.clone()
      nnz = other.nnz.clone()
      currMax = other.currMax.clone()
      currMin = other.currMin.clone()
    }
    if (configuration.frequentItems) {
      longsSketches.zipWithIndex.foreach( kv =>
        kv._1.merge(other.longsSketches(kv._2)) )
    }
    if (configuration.approxDistinctness) {
      hllSketches = hllSketches.zipWithIndex.map( kv => {
        val union = new HllUnion()
        union.update(kv._1)
        union.update(other.hllSketches(kv._2))
        union.getResult
      })
    }
    if (configuration.approxQuantiles) {
      kllSketches.zipWithIndex.foreach( kv =>
        kv._1.merge(other.kllSketches(kv._2)) )
    }
    this
  }

  /**
   * Sample mean of each dimension.
   *
   */
  def mean: Array[Double] = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realMean = Array.ofDim[Double](n)
    var i = 0
    while (i < n) {
      realMean(i) = currMean(i) * (currWeightSum(i) / totalWeightSum)
      i += 1
    }
    realMean
  }

  /**
   * Unbiased estimate of sample variance of each dimension.
   *
   */
  def variance: Array[Double] = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realVariance = Array.ofDim[Double](n)

    val denominator = totalWeightSum - (weightSquareSum / totalWeightSum)

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      val deltaMean = currMean
      var i = 0
      val len = currM2n.length
      while (i < len) {
        // We prevent variance from negative value caused by numerical error.
        realVariance(i) = math.max((currM2n(i) + deltaMean(i) * deltaMean(i) * currWeightSum(i) *
          (totalWeightSum - currWeightSum(i)) / totalWeightSum) / denominator, 0.0)
        i += 1
      }
    }
    realVariance
  }

  /**
   * Sample size.
   *
   */
  def count: Long = totalCnt

  /**
   * Sum of weights.
   */
  def weightSum: Double = totalWeightSum

  /**
   * Number of nonzero elements in each dimension.
   *
   */
  def numNonzeros: Array[Double] = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    nnz.map(_.toDouble)
  }

  /**
   * Maximum value of each dimension.
   *
   */
  def max: Array[Double] = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    /*
    // we don't want nulls to count towards max-value
    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMax(i) < 0.0)) currMax(i) = 0.0
      i += 1
    }
    */
    currMax
  }

  /**
   * Minimum value of each dimension.
   *
   */
  def min: Array[Double] = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    /*
    // we don't want nulls to count towards min-value
    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMin(i) > 0.0)) currMin(i) = 0.0
      i += 1
    }
    */
    currMin
  }

  /**
   * L2 (Euclidean) norm of each dimension.
   *
   */
  def normL2: Array[Double] = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realMagnitude = Array.ofDim[Double](n)

    var i = 0
    val len = currM2.length
    while (i < len) {
      realMagnitude(i) = math.sqrt(currM2(i))
      i += 1
    }
    realMagnitude
  }

  /**
   * L1 norm of each dimension.
   *
   */
  def normL1: Array[Double] = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    currL1
  }

  def fromSerialized(): this.type = {
    if (configuration.frequentItems) {
      longsSketches = longsSketchesBin.map( sketch => LongsSketch.getInstance(Memory.wrap(sketch)) )
    }
    if (configuration.approxDistinctness) {
      hllSketches = hllSketchesBin.map( sketch => jHllSketch.heapify(Memory.wrap(sketch)) )
    }
    if (configuration.approxQuantiles) {
      kllSketches = kllSketchesBin.map( sketch => jKllFloatsSketch.heapify(Memory.wrap(sketch)) )
    }
    this
  }

  def toSerializable(): this.type = {
    if (configuration.frequentItems) {
      longsSketchesBin = longsSketches.map( sketch => sketch.toByteArray )
    }
    if (configuration.approxDistinctness) {
      hllSketchesBin = hllSketches.map( sketch => sketch.toUpdatableByteArray )
    }
    if (configuration.approxQuantiles) {
      kllSketchesBin = kllSketches.map( sketch => sketch.toByteArray )
    }
    this
  }

  def getStats(columnNames: Array[String],
               dataTypes: Array[DataType]): RDDStatistics = {
    val hashingColumnsLookups = configuration.frequentItems match {
      case true => Some(longsSketches.map( sketch => {
        val items = sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES)
        val items_lookup = items.take(Math.min(items.length, configuration.maxFreqItems))
          .zipWithIndex.map(kv => (kv._1.getItem, kv._2)).toMap
        items_lookup
      }))
      case false => None
    }
    val distinctCounts = configuration.approxDistinctness match {
      case true => Some(hllSketches.map( sketch => sketch.getEstimate ).toArray)
      case false => None
    }

    RDDStatistics(columnNames,
      dataTypes,
      mean,
      variance,
      count,
      weightSum,
      numNonzeros,
      max,
      min,
      normL2,
      normL1,
      distinctCounts,
      hashingColumnsLookups)
  }

}

case class RDDStatistics(columnNames: Array[String],
                         dataTypes: Array[DataType],
                         mean: Array[Double],
                         variance: Array[Double],
                         count: Long,
                         weightSum: Double,
                         numNonzeros: Array[Double],
                         max: Array[Double],
                         min: Array[Double],
                         normL2: Array[Double],
                         normL1: Array[Double],
                         approxDistinct: Option[Array[Double]],
                         freqItems: Option[IndexedSeq[Map[Long, Int]]]
                         ) extends
  Serializable

case class RDDStatisticsConfig(approxDistinctness: Boolean = true,
                               frequentItems: Boolean = true, maxFreqItems: Int = 253,
                               freqItemSketchSize: Int = 1024,
                               treatInfinityAsNaN: Boolean = true,
                               approxQuantiles: Boolean = false,
                               kllSketchSize: Int = 2048,
                               kllSketchShrinkingFactor: Double = 0.64)

object NumerizationHelper {
  def numerize(instance: Row, index: Int, dataType: DataType): Double = {
    dataType match {
      case BooleanType => {
        if (instance.getBoolean(index)) 1.0 else 0.0
      }
      case ByteType => {
        instance.getByte(index)
      }
      case ShortType => {
        instance.getShort(index)
      }
      case IntegerType => {
        instance.getInt(index)
      }
      case LongType => {
        instance.getLong(index)
      }
      case FloatType => {
        instance.getFloat(index)
      }
      case DoubleType => {
        instance.getDouble(index)
      }
      case DecimalType() => {
        instance.getAs[java.math.BigDecimal](index).doubleValue()
      }
      case TimestampType => {
        instance.getTimestamp(index).getTime
      }
      case DateType => {
        instance.getDate(index).getTime
      }
      case StringType => {
        // TODO: use 64 bit hash to reduce likelihood of hash collisions
        //XxHash64Function.hash(UTF8String.fromString(instance.getString(index)),
        //  StringType, 42L).toDouble
        //MurmurHash3.stringHash(instance.getString(index), 42)
        instance.getString(index).hashCode
      }
      case _ => {
        // TODO: use 64 bit hash to reduce likelihood of hash collisions
        //XxHash64Function.hash(instance.get(index), dataTypes(index), 42L).toDouble
        instance.get(index).hashCode()
      }
    }
  }
}

object StatisticsEngineRDD {

  private def partitionStats(vectorSize: Int, dataTypes: Array[DataType],
                     statsConfig: RDDStatisticsConfig)
  (rows: Iterator[Row])
  : Iterator[ExtendedMultivariateOnlineSummarizer] = {

    val summarizer = new ExtendedMultivariateOnlineSummarizer(vectorSize, dataTypes, statsConfig)

    while (rows.hasNext) {
      val instance = rows.next()
      summarizer.add(instance)
    }

    Iterator.single(summarizer.toSerializable())
  }

  def computeStatistics(df: DataFrame,
                        statsConfig: RDDStatisticsConfig = RDDStatisticsConfig())
  : RDDStatistics = {
    val rows = df.rdd
    val rowSize = df.schema.length
    val dataTypes = df.schema.map(kv => kv.dataType).toArray
    val columnNames = df.schema.map(kv => kv.name).toArray

    val summaryStatsPartition = partitionStats(rowSize, dataTypes, statsConfig) _
    val finalSummarizer =
      rows
        .mapPartitions(summaryStatsPartition, preservesPartitioning = true)
        .treeReduce { case (columnAndSketchesA, columnAndSketchesB) =>
          val a = columnAndSketchesA.fromSerialized()
          val b = columnAndSketchesB.fromSerialized()
          a.merge(b)
          a.toSerializable()
        }

    finalSummarizer.fromSerialized().getStats(columnNames, dataTypes)
  }

  import com.amazon.deequ.analyzers.DataTypeInstances.{Boolean, Decimal, Fractional, Integral, String, Unknown}
  import com.amazon.deequ.profiles.{ColumnProfile, NumericColumnProfile, StandardColumnProfile}

  def toDeequProfiles(stats: RDDStatistics): Seq[ColumnProfile] = {
    val columnProfiles = new ArrayBuffer[ColumnProfile]()
    for (i <- stats.numNonzeros.indices){
      val profile = stats.dataTypes(i) match {
        case ByteType | ShortType | IntegerType | LongType
             | FloatType | DoubleType | DecimalType() => {
          val dataType = stats.dataTypes(i) match {
            case ByteType | ShortType | IntegerType | LongType => Integral
            case FloatType | DoubleType => Fractional
            case DecimalType() => Decimal
          }
          NumericColumnProfile(
            stats.columnNames(i),
            (stats.count - stats.numNonzeros(i)) / stats.count,
            None,
            None,
            None,
            stats.approxDistinct.get(i).toLong,
            dataType,
            false,
            new HashMap[String, Long](),
            None,
            None,
            Some(stats.mean(i)),
            Some(stats.max(i)),
            Some(stats.min(i)),
            None,
            Some(Math.sqrt(stats.variance(i))),
            None,
            None,
            None
          )
        }
        case _ => {
          val dataType = stats.dataTypes(i) match {
            case BooleanType => Boolean
            case StringType | TimestampType | DateType | BinaryType => String
            case _ => Unknown
          }
          StandardColumnProfile(
            stats.columnNames(i),
            (stats.count - stats.numNonzeros(i)) / stats.count,
            None,
            None,
            None,
            stats.approxDistinct.get(i).toLong,
            dataType,
            false,
            new HashMap[String, Long](),
            None,
            None)
        }

      }
      columnProfiles.append(profile)
    }
    columnProfiles
  }

}