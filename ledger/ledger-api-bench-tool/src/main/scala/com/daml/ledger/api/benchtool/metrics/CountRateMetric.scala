// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics
import com.daml.ledger.api.benchtool.metrics.objectives.ServiceLevelObjective

import java.time.Duration

final case class CountRateMetric[T](
    countingFunction: T => Int,
    objective: Option[
      (ServiceLevelObjective[CountRateMetric.Value], Option[CountRateMetric.Value])
    ],
    counter: Int = 0,
    lastCount: Int = 0,
) extends Metric[T] {
  import CountRateMetric._

  override type V = Value
  override type Objective = ServiceLevelObjective[Value]

  override def onNext(value: T): CountRateMetric[T] =
    this.copy(counter = counter + countingFunction(value))

  override def periodicValue(periodDuration: Duration): (Metric[T], Value) = {
    val value = Value(periodicRate(periodDuration))
    val updatedMetric = this.copy(
      objective = updatedObjective(value),
      lastCount = counter,
    )
    (updatedMetric, value)
  }

  override def finalValue(totalDuration: Duration): Value =
    Value(ratePerSecond = totalRate(totalDuration))

  override def violatedObjective: Option[(ServiceLevelObjective[Value], Value)] =
    objective.collect { case (objective, Some(value)) =>
      objective -> value
    }

  private def periodicRate(periodDuration: Duration): Double =
    (counter - lastCount) * 1000.0 / periodDuration.toMillis

  private def totalRate(totalDuration: Duration): Double =
    counter / totalDuration.toMillis.toDouble * 1000.0

  private def updatedObjective(
      newValue: Value
  ): Option[(ServiceLevelObjective[CountRateMetric.Value], Option[CountRateMetric.Value])] = {
    objective.map { case (objective, currentMinValue) =>
      if (objective.isViolatedBy(newValue)) {
        currentMinValue match {
          case None => objective -> Some(newValue)
          case Some(currentValue) => objective -> Some(Ordering[Value].min(currentValue, newValue))
        }
      } else {
        objective -> currentMinValue
      }
    }
  }
}

object CountRateMetric {
  final case class Value(ratePerSecond: Double) extends MetricValue

  object Value {
    implicit val ordering: Ordering[Value] =
      Ordering.fromLessThan(_.ratePerSecond < _.ratePerSecond)
  }

  def empty[T](
      countingFunction: T => Int,
      objective: Option[ServiceLevelObjective[Value]] = None,
  ): CountRateMetric[T] = CountRateMetric[T](
    countingFunction,
    objective.map(obj => obj -> None),
  )
}
