package com.logicalclocks.hsfs.engine

case class Constraint(name: String, hint: Option[String], columns: Option[Seq[String]], min: Option[Double],
                      max: Option[Double], value: Option[Double], pattern: Option[String],
                      acceptedType: Option[String], legalValues: Option[Array[String]])
