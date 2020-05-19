// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  val DAML: MetricName = MetricName("daml")

  def apply(segments: String*): MetricName =
    new MetricName(segments.toVector)

  implicit def metricNameToString(name: MetricName): String =
    name.toString

}
