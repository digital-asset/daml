// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics.metrics

import java.time.{Clock, Duration, Instant}

import com.daml.ledger.api.benchtool.metrics.{Metric, MetricValue, ServiceLevelObjective}
import com.daml.ledger.api.benchtool.metrics.metrics.TotalStreamRuntimeMetric.{
  MaxDurationObjective,
  Value,
}
import com.daml.ledger.api.benchtool.util.TimeUtil

object TotalStreamRuntimeMetric {

  case class MaxDurationObjective(maxValue: Duration) extends ServiceLevelObjective[Value] {
    override def isViolatedBy(value: Value): Boolean = value.v.compareTo(maxValue) > 0
  }

  def empty[T](
      clock: Clock,
      startTime: Instant,
      objective: MaxDurationObjective,
  ): TotalStreamRuntimeMetric[T] =
    TotalStreamRuntimeMetric[T](
      clock = clock,
      startTime = startTime,
      objective = objective,
    )

  case class Value(v: Duration) extends MetricValue
}

/** Measures the total runtime since the set start time to the time of receiving the most recent item.
  */
case class TotalStreamRuntimeMetric[T](
    clock: Clock,
    startTime: Instant,
    objective: MaxDurationObjective,
) extends Metric[T] {
  override type V = Value
  override type Objective = MaxDurationObjective

  // NOTE: There's no need to synchronize on this variable
  // as this metric used solely as an internal state of an actor at 'com.daml.ledger.api.benchtool.metrics.MetricsCollector.handlingMessages'
  private var lastSeenItemTime: Instant = startTime

  override def onNext(item: T): Metric[T] = {
    lastSeenItemTime = clock.instant()
    this
  }

  override def periodicValue(periodDuration: Duration): (Metric[T], Value) =
    this -> totalRuntime

  override def finalValue(totalDuration: Duration): Value =
    totalRuntime

  override def violatedFinalObjectives(
      totalDuration: Duration
  ): List[(MaxDurationObjective, Value)] =
    if (objective.isViolatedBy(totalRuntime))
      List((objective, totalRuntime))
    else
      List.empty

  private def totalRuntime: Value = Value(TimeUtil.durationBetween(startTime, lastSeenItemTime))

}
