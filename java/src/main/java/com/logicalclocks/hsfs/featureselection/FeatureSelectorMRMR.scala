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
 * based on fast-mRMR (https://github.com/sramirez/fast-mRMR)
 */

package com.logicalclocks.hsfs.featureselection

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ListMap

/**
 * Information Theory function and distributed primitives.
 */
object MutualInformation {

  private var classCol: Array[Byte] = null
  private var marginalProb: RDD[(Int, BDV[Float])] = null
  private var jointProb: RDD[(Int, BDM[Float])] = null

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   * @param n Number of elements
   *
   */
  private def entropy(freqs: Seq[Long], n: Long) = {
    freqs.aggregate(0.0)({ case (h, q) =>
      h + (if (q == 0) 0  else (q.toDouble / n) * (math.log(q.toDouble / n) / math.log(2)))
    }, { case (h1, h2) => h1 + h2 }) * -1
  }

  /**
   * Calculate entropy for the given frequencies.
   *
   * @param freqs Frequencies of each different class
   */
  private def entropy(freqs: Seq[Long]): Double = {
    entropy(freqs, freqs.reduce(_ + _))
  }

  /**
   * Method that calculates mutual information (MI) and conditional mutual information (CMI)
   * simultaneously for several variables. Indexes must be disjoint.
   *
   * @param rawData RDD of data (first element is the class attribute)
   * @param varX Indexes of primary variables (must be disjoint with Y and Z)
   * @param varY Indexes of secondary variable (must be disjoint with X and Z)
   * @param nInstances    Number of instances
   * @param nFeatures Number of features (including output ones)
   * @return  RDD of (primary var, (MI, CMI))
   *
   */
  def computeMI(
                 rawData: RDD[(Long, Byte)],
                 varX: Seq[Int],
                 varY: Int,
                 nInstances: Long,
                 nFeatures: Int,
                 counter: Map[Int, Int]) = {

    // Pre-requisites
    require(varX.size > 0)

    // Broadcast variables
    val sc = rawData.context
    val label = nFeatures - 1
    // A boolean vector that indicates the variables involved on this computation
    val fselected = Array.ofDim[Boolean](nFeatures)
    fselected(varY) = true // output feature
    varX.map(fselected(_) = true)
    val bFeatSelected = sc.broadcast(fselected)
    val getFeat = (k: Long) => (k % nFeatures).toInt
    // Filter data by these variables
    val data = rawData.filter({ case (k, _) => bFeatSelected.value(getFeat(k))})

    // Broadcast Y vector
    val yCol: Array[Byte] = if(varY == label){
      // classCol corresponds with output attribute, which is re-used in the iteration
      classCol = data.filter({ case (k, _) => getFeat(k) == varY}).values.collect()
      classCol
    }  else {
      data.filter({ case (k, _) => getFeat(k) == varY}).values.collect()
    }

    val histograms = computeHistograms(data, (varY, yCol), nFeatures, counter)
    val jointTable = histograms.mapValues(_.map(_.toFloat / nInstances))
    val marginalTable = jointTable.mapValues(h => sum(h(*, ::)).toDenseVector)

    // If y corresponds with output feature, we save for CMI computation
    if(varY == label) {
      marginalProb = marginalTable.cache()
      jointProb = jointTable.cache()
    }

    val yProb = marginalTable.lookup(varY)(0)
    // Remove output feature from the computations
    val fdata = histograms.filter{case (k, _) => k != label}
    computeMutualInfo(fdata, yProb, nInstances)
  }

  private def computeHistograms(
                                 data: RDD[(Long, Byte)],
                                 yCol: (Int, Array[Byte]),
                                 nFeatures: Long,
                                 counter: Map[Int, Int]) = {

    val maxSize = 256
    val byCol = data.context.broadcast(yCol._2)
    val bCounter = data.context.broadcast(counter)
    val ys = counter.getOrElse(yCol._1, maxSize).toInt

    data.mapPartitions({ it =>
      var result = Map.empty[Int, BDM[Long]]
      for((k, x) <- it) {
        val feat = (k % nFeatures).toInt; val inst = (k / nFeatures).toInt
        val xs = bCounter.value.getOrElse(feat, maxSize).toInt
        val m = result.getOrElse(feat, BDM.zeros[Long](xs, ys))
        val shiftedX = if (x < 0) (x+256) else x
        val y = byCol.value(inst)
        val shiftedY = if (y < 0) (y+256) else y
        m(shiftedX, shiftedY) += 1
        //if (x < 0 || byCol.value(inst) < 0 ){
        //  println(feat, inst, xs, x, ys, byCol.value(inst), shiftedX, shiftedY)
        //  println(m(shiftedX, shiftedY))
        //}
        result += feat -> m
      }
      result.toIterator
    }).reduceByKey(_ + _)
  }

  private def computeMutualInfo(
                                 data: RDD[(Int, BDM[Long])],
                                 yProb: BDV[Float],
                                 n: Long) = {

    val byProb = data.context.broadcast(yProb)
    data.mapValues({ m =>
      var mi = 0.0d
      // Aggregate by row (x)
      val xProb = sum(m(*, ::)).map(_.toFloat / n)
      for(i <- 0 until m.rows){
        for(j <- 0 until m.cols){
          val pxy = m(i, j).toFloat / n
          val py = byProb.value(j); val px = xProb(i)
          if(pxy != 0 && px != 0 && py != 0) // To avoid NaNs
            mi += pxy * (math.log(pxy / (px * py)) / math.log(2))
        }
      }
      mi
    })
  }
}

/**
 * Minimum-Redundancy Maximum-Relevance criterion (mRMR)
 */
class MrmrCriterion(var relevance: Double) extends Serializable {

  var redundance: Double = 0.0
  var selectedSize: Int = 0

  def score = {
    if (selectedSize != 0) {
      relevance - redundance / selectedSize
    } else {
      relevance
    }
  }

  def update(mi: Double): MrmrCriterion = {
    redundance += mi
    selectedSize += 1
    this
  }

  override def toString: String = "MRMR"
}

/**
 * Train a info-theory feature selection model according to a criterion.
 */
class FeatureSelectorMRMR protected extends Serializable {

  // Pool of criterions
  private type Pool = RDD[(Int, MrmrCriterion)]
  // Case class for criterions by feature
  protected case class F(feat: Int, crit: Double)

  /**
   * Perform a info-theory selection process.
   *
   * @param data Columnar data (last element is the class attribute).
   * @param nToSelect Number of features to select.
   * @param nFeatures Number of total features in the dataset.
   * @return A list with the most relevant features and its scores.
   *
   */
  private def selectFeatures(
                                       data: RDD[(Long, Byte)],
                                       nToSelect: Int,
                                       nFeatures: Int,
                                       verbose: Boolean) = {

    val label = nFeatures - 1
    val nInstances = data.count() / nFeatures
    // extract max values from bytes, while accounting for scala's singed byte type.
    // if v < 0, add 256, otherwise return v
    // example A: input: 255.toByte = (byte) -1   output: -1 + 256   => (int) 255
    // example B: input: 128.toByte = (byte) -128 output: -128 + 256 => (int) 128
    // example C: input: 175.toByte = (byte) -81  output: -81 + 256  => (int) 175
    // example D: input: 12.toByte  = (byte) 12   output:    12      => (int) 12
    val counterByKey = data.map({ case (k, v) => (k % nFeatures).toInt -> (if (v < 0) (v+256)
      else v)}).distinct().groupByKey().mapValues(_.max + 1).collectAsMap().toMap

    // calculate relevance
    val MiAndCmi = MutualInformation.computeMI(
      data, 0 until label, label, nInstances, nFeatures, counterByKey)
    var pool = MiAndCmi.map{case (x, mi) => (x, new MrmrCriterion(mi))}
      .collectAsMap()
    if (verbose) {
      // Print most relevant features
      val strRels = MiAndCmi.collect().sortBy(-_._2)
        .take(nToSelect)
        .map({case (f, mi) => (f + 1) + "\t" + "%.4f" format mi})
        .mkString("\n")
      println("\n*** MaxRel features ***\nFeature\tScore\n" + strRels)
    }
    // get maximum and select it
    val firstMax = pool.maxBy(_._2.score)
    var selected = Seq(F(firstMax._1, firstMax._2.score))
    pool = pool - firstMax._1

    while (selected.size < nToSelect) {
      // update pool
      val newMiAndCmi = MutualInformation.computeMI(data, pool.keys.toSeq,
        selected.head.feat, nInstances, nFeatures, counterByKey)
        .map({ case (x, crit) => (x, crit) })
        .collectAsMap()

      pool.foreach({ case (k, crit) =>
        newMiAndCmi.get(k) match {
          case Some(_) => crit.update(_)
          case None =>
        }
      })

      // get maximum and save it
      // TODO: takes lowest feature index if scores are equal
      val max = pool.maxBy(_._2.score)
      // select the best feature and remove from the whole set of features
      selected = F(max._1, max._2.score) +: selected
      pool = pool - max._1
    }
    selected.reverse
  }

  private def runColumnar(
                            columnarData: RDD[(Long, Byte)],
                            nToSelect: Int,
                            nAllFeatures: Int,
                            verbose: Boolean = true): Seq[(Int, Double)] = {
    columnarData.persist(StorageLevel.MEMORY_AND_DISK_SER)

    require(nToSelect < nAllFeatures)
    val selected = selectFeatures(columnarData, nToSelect, nAllFeatures, verbose)

    columnarData.unpersist()

    selected.map{case F(feat, rel) => feat -> rel}
  }
}

object FeatureSelectorMRMR {

  /**
   * Train a mRMR selection model according to a given criterion
   * and return a subset of data.
   *
   * @param   data RDD of LabeledPoint (discrete data as integers in range [0, 255]).
   * @param   nToSelect maximum number of features to select
   * @param   nAllFeatures number of features to select.
   * @param   indexToFeatures map of df-indexes and corresponding column names.
   * @param   verbose whether messages should be printed or not.
   * @return  A mRMR selector that selects a subset of features from the original dataset.
   *
   *
   */
  def selectFeatures(
             data: RDD[(Long, Byte)],
             nToSelect: Int = -1,
             nAllFeatures: Int,
             indexToFeatures: Map[Int, String],
             verbose: Boolean = false): Map[String, Double] = {
    // if nToSelect -1 or larger than nAllFeatures, clamp to nAllFeatures-1
    val nSelect = if(nToSelect < 0 || nToSelect > nAllFeatures-1) nAllFeatures-1 else nToSelect

    val selected = new FeatureSelectorMRMR().runColumnar(data, nSelect, nAllFeatures, verbose)

    if (verbose) {
      // Print best features according to the mRMR measure
      val out = selected.map { case (feat, rel) => (indexToFeatures(feat)) + "\t" + "%.4f"
        .format(rel) }.mkString("\n")
      println("\n*** mRMR features ***\nFeature\tScore\n" + out)
    }

    // Return best features and mRMR measure
    ListMap(selected.map( kv => indexToFeatures(kv._1) -> kv._2 ): _*)
  }

}

