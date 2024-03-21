// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.util.TimeUtil
import com.google.protobuf.timestamp.Timestamp

import java.time.{Duration, Instant}

final case class ConsumptionSpeedMetric[T](
    recordTimeFunction: T => Seq[Timestamp],
    objective: Option[
      (ConsumptionSpeedMetric.MinConsumptionSpeed, Option[ConsumptionSpeedMetric.Value])
    ],
    previousLatest: Option[Instant] = None,
    currentPeriodLatest: Option[Instant] = None,
) extends Metric[T] {
  import ConsumptionSpeedMetric._

  override type V = Value
  override type Objective = MinConsumptionSpeed

  override def onNext(value: T): ConsumptionSpeedMetric[T] = {
    val recordTimes = recordTimeFunction(value)
    val newPreviousLatest =
      previousLatest match {
        case None => recordTimes.headOption.map(TimeUtil.timestampToInstant)
        case v => v
      }
    val newCurrentPeriodLatest = recordTimes.lastOption.map(TimeUtil.timestampToInstant)

    this.copy(
      previousLatest = newPreviousLatest,
      currentPeriodLatest = newCurrentPeriodLatest,
    )
  }

  override def periodicValue(periodDuration: Duration): (Metric[T], Value) = {
    val value = Value(Some(periodicSpeed(periodDuration)))
    val updatedMetric = this.copy(
      previousLatest = if (currentPeriodLatest.isDefined) currentPeriodLatest else previousLatest,
      currentPeriodLatest = None,
      objective = updatedObjectives(value),
    )
    (updatedMetric, value)
  }

  override def finalValue(totalDuration: Duration): Value =
    Value(None)

  override def violatedPeriodicObjectives: List[(MinConsumptionSpeed, Value)] =
    objective.collect {
      case (objective, value) if value.isDefined => objective -> value.get
    }.toList

  override def violatedFinalObjectives(
      totalDuration: Duration
  ): List[(MinConsumptionSpeed, Value)] = Nil

  private def periodicSpeed(periodDuration: Duration): Double =
    (previousLatest, currentPeriodLatest) match {
      case (Some(previous), Some(current)) =>
        (current.toEpochMilli - previous.toEpochMilli).toDouble / periodDuration.toMillis
      case _ =>
        0.0
    }

  private def updatedObjectives(newValue: Value): Option[
    (MinConsumptionSpeed, Option[Value])
  ] =
    objective.map { case (objective, currentMaxValue) =>
      if (objective.isViolatedBy(newValue)) {
        currentMaxValue match {
          case None =>
            objective -> Some(newValue)
          case Some(currentValue) =>
            objective -> Some(Ordering[Value].min(currentValue, newValue))
        }
      } else {
        objective -> currentMaxValue
      }
    }
}

object ConsumptionSpeedMetric {

  def empty[T](
      recordTimeFunction: T => Seq[Timestamp],
      objective: Option[MinConsumptionSpeed] = None,
  ): ConsumptionSpeedMetric[T] =
    ConsumptionSpeedMetric(
      recordTimeFunction,
      objective.map(objective => objective -> None),
    )

  // TODO: remove option
  final case class Value(relativeSpeed: Option[Double]) extends MetricValue

  object Value {
    implicit val ordering: Ordering[Value] = (x: Value, y: Value) => {
      (x.relativeSpeed, y.relativeSpeed) match {
        case (Some(xx), Some(yy)) =>
          if (xx < yy) -1
          else if (xx > yy) 1
          else 0
        case (Some(_), None) => 1
        case (None, Some(_)) => -1
        case (None, None) => 0
      }
    }
  }

  final case class MinConsumptionSpeed(minSpeed: Double) extends ServiceLevelObjective[Value] {
    override def isViolatedBy(metricValue: Value): Boolean =
      Ordering[Value].lt(metricValue, v)

    private val v = Value(Some(minSpeed))
  }
}
