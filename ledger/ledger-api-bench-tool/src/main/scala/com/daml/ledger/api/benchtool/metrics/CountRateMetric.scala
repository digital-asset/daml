// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import java.time.Duration

final case class CountRateMetric[T](
    countingFunction: T => Int,
    periodicObjectives: List[
      (CountRateMetric.RateObjective, Option[CountRateMetric.Value])
    ],
    finalObjectives: List[CountRateMetric.RateObjective],
    counter: Int = 0,
    lastCount: Int = 0,
) extends Metric[T] {
  import CountRateMetric._

  override type V = Value
  override type Objective = RateObjective

  override def onNext(value: T): CountRateMetric[T] =
    this.copy(counter = counter + countingFunction(value))

  override def periodicValue(periodDuration: Duration): (Metric[T], Value) = {
    val value = Value(periodicRate(periodDuration))
    val updatedMetric = this.copy(
      periodicObjectives = updatedPeriodicObjectives(value),
      lastCount = counter,
    )
    (updatedMetric, value)
  }

  override def finalValue(totalDuration: Duration): Value =
    Value(ratePerSecond = totalRate(totalDuration))

  override def violatedPeriodicObjectives: List[(RateObjective, Value)] =
    periodicObjectives.collect { case (objective, Some(value)) =>
      objective -> value
    }

  override def violatedFinalObjectives(
      totalDuration: Duration
  ): List[(RateObjective, Value)] =
    finalObjectives.collect {
      case objective if objective.isViolatedBy(finalValue(totalDuration)) =>
        (objective, finalValue(totalDuration))
    }

  private def periodicRate(periodDuration: Duration): Double =
    (counter - lastCount) * 1000.0 / periodDuration.toMillis

  private def totalRate(totalDuration: Duration): Double =
    counter / totalDuration.toMillis.toDouble * 1000.0

  private def updatedPeriodicObjectives(
      newValue: Value
  ): List[(RateObjective, Option[Value])] = {
    periodicObjectives.map { case (objective, currentMinValue) =>
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

  abstract class RateObjective extends ServiceLevelObjective[Value] with Product with Serializable
  object RateObjective {
    final case class MinRate(minAllowedRatePerSecond: Double) extends RateObjective {
      override def isViolatedBy(metricValue: CountRateMetric.Value): Boolean =
        Ordering[CountRateMetric.Value].lt(metricValue, v)

      private val v = CountRateMetric.Value(minAllowedRatePerSecond)
    }

    final case class MaxRate(minAllowedRatePerSecond: Double) extends RateObjective {
      override def isViolatedBy(metricValue: CountRateMetric.Value): Boolean =
        Ordering[CountRateMetric.Value].gt(metricValue, v)

      private val v = CountRateMetric.Value(minAllowedRatePerSecond)
    }
  }

  def empty[T](
      countingFunction: T => Int,
      periodicObjectives: List[RateObjective],
      finalObjectives: List[RateObjective],
  ): CountRateMetric[T] = CountRateMetric[T](
    countingFunction,
    periodicObjectives.map(obj => obj -> None),
    finalObjectives = finalObjectives,
  )
}
