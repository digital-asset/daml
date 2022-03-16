// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import scala.language.implicitConversions

final class MetricName(private val segments: Vector[String]) extends AnyVal {
  def :+(segment: String): MetricName =
    new MetricName(segments :+ segment)

  override def toString: String =
    segments.mkString(".")
}

object MetricName {

  val Daml: MetricName = MetricName("daml")

  def apply(segments: String*): MetricName =
    new MetricName(segments.toVector)

  implicit def metricNameToString(name: MetricName): String =
    name.toString

  // TODO: these names cannot be used when giving a name to a Metric
  private object LabelNames {
    val ApplicationId: String = "applicationId"
  }

  def nameWithLabel(metricName: MetricName, context: MetricContext): MetricName =
    metricName :+ LabelNames.ApplicationId :+ context.applicationId

  def split(name: String): (String, Option[String]) = {
    val splitted = name.split("\\.")
    val labelMarkerIndex = splitted.lastIndexOf(LabelNames.ApplicationId)
    if (labelMarkerIndex > -1) {
      val base = splitted.take(labelMarkerIndex).mkString(".")
      val applicationId = splitted(labelMarkerIndex + 1)
      base -> Some(applicationId)
    } else {
      name -> None
    }
  }

}
