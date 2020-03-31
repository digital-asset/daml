// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.metrics

import scala.language.implicitConversions

case class MetricName(segments: Vector[String]) extends AnyVal {
  def :+(segment: String): MetricName =
    new MetricName(segments :+ segment)
}

object MetricName {
  def apply(segments: String*): MetricName =
    MetricName(segments.toVector)

  implicit def metricNameToString(name: MetricName): String =
    name.segments.mkString(".")
}
