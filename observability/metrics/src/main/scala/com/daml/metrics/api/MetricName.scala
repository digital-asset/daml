// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import scala.language.implicitConversions

final class MetricName(private val segments: Vector[String]) extends AnyVal {
  def :+(segment: String): MetricName =
    new MetricName(segments :+ segment)

  def asPrometheus = segments.mkString("_")

  override def toString: String =
    segments.mkString(".")
}

object MetricName {

  val Daml: MetricName = MetricName("daml")

  def apply(segments: String*): MetricName =
    new MetricName(segments.toVector)

  implicit def metricNameToString(name: MetricName): String =
    name.toString

}
