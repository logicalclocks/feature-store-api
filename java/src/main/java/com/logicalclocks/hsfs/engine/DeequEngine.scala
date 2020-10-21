package com.logicalclocks.hsfs.engine

import java.util
import com.amazon.deequ.checks.{Check, CheckLevel, CheckResult}
import com.amazon.deequ.constraints.ConstraintResult
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConverters._
import scala.collection.JavaConverters.mapAsJavaMapConverter

object DeequEngine {

  def longBoundary(min: Option[Double], max: Option[Double]): Long => Boolean = {
    (min, max) match {
      case (Some(x), Some(y)) => v => v >= x && v <= y
      case (Some(x), None) => _ >= x
      case (None, Some(y)) => _ <= y
      case _ => _ => true
    }
  }

  def doubleBoundary(min: Option[Double], max: Option[Double]): Double => Boolean = {
    (min, max) match {
      case (Some(x), Some(y)) => v => v >= x && v <= y
      case (Some(x), None) => _ >= x
      case (None, Some(y)) => _ <= y
      case _ => _ => true
    }
  }

  def addConstraint(check: Check, constraint: Constraint): Check = {
    constraint.name match {
      case "HAS_MEAN" => check.hasMean(constraint.columns.get.head,
                                       doubleBoundary(constraint.min, constraint.max),
                                       constraint.hint)
      case "HAS_MIN" => check.hasMin(constraint.columns.get.head,
                                     doubleBoundary(constraint.min, constraint.max),
                                     constraint.hint)
      case "HAS_MAX" => check.hasMax(constraint.columns.get.head,
                                     doubleBoundary(constraint.min, constraint.max),
                                     constraint.hint)
      case "HAS_SUM" => check.hasSum(constraint.columns.get.head,
                                     doubleBoundary(constraint.min, constraint.max),
                                     constraint.hint)
    }
  }

  def checksFromRules(constraintGroups: Seq[ConstraintGroup]): Seq[Check] = {
    constraintGroups
      .map(group => {
        var check = Check(CheckLevel.withName(group.level), group.description);
        group.constraints.foreach(constraint => check = addConstraint(check, constraint))
        check
      })
  }


  def runVerification(data: DataFrame, constraintGroups: Seq[ConstraintGroup]): String = {
    val checks = checksFromRules(constraintGroups)
    val verificationResult = VerificationSuite().onData(data).addChecks(checksFromRules(constraintGroups)).run()
     VerificationResult.checkResultsAsJson(verificationResult, checks)
  }

  def runVerificationDeequ(data: DataFrame, constraintGroups: Seq[ConstraintGroup]): util.Map[Check, CheckResult] = {
    val checks = checksFromRules(constraintGroups)
    VerificationSuite().onData(data).addChecks(checks).run().checkResults.asJava
  }

  def getConstraintResults(constraintResults : Seq[ConstraintResult]): util.List[ConstraintResult] ={
    constraintResults.asJava
  }

}
