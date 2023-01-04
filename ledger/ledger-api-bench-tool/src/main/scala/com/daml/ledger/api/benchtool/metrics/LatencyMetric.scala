// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.LatencyMetric.{LatencyNanos, MaxLatency, Value}

import java.time.Duration

case class LatencyMetric(totalNanos: LatencyNanos, numberObservations: Int, maxLatency: MaxLatency)
    extends Metric[LatencyNanos] {
  override type V = LatencyMetric.Value
  override type Objective = MaxLatency

  override def onNext(value: LatencyNanos): Metric[LatencyNanos] =
    copy(
      totalNanos = totalNanos + value,
      numberObservations = numberObservations + 1,
    )

  override def periodicValue(periodDuration: Duration): (Metric[LatencyNanos], Value) =
    this -> currentAverage

  override def finalValue(totalDuration: Duration): Value =
    currentAverage

  override def violatedFinalObjectives(
      totalDuration: Duration
  ): List[(MaxLatency, Value)] = {
    val averageLatency = finalValue(totalDuration)
    val violation = maxLatency.isViolatedBy(averageLatency)
    if (violation) List(maxLatency -> averageLatency)
    else Nil
  }

  private def currentAverage: Value =
    if (numberObservations == 0) Value(0L) else Value(totalNanos / numberObservations)
}

object LatencyMetric {
  type LatencyNanos = Long
  case class Value(latencyNanos: LatencyNanos) extends MetricValue

  def empty(maxLatencyObjectiveMillis: Long): LatencyMetric =
    LatencyMetric(0, 0, MaxLatency(maxLatencyObjectiveMillis * 1000000L))

  final case class MaxLatency(maxLatency: LatencyNanos) extends ServiceLevelObjective[Value] {
    override def isViolatedBy(metricValue: Value): Boolean =
      metricValue.latencyNanos > maxLatency

    def millis: Double = maxLatency / 1000000d
  }
}
