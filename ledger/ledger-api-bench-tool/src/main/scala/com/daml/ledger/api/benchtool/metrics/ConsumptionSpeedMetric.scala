// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.Metric.rounded
import com.daml.ledger.api.benchtool.metrics.objectives.ServiceLevelObjective
import com.google.protobuf.timestamp.Timestamp

import java.time.Instant

final case class ConsumptionSpeedMetric[T](
    periodMillis: Long,
    recordTimeFunction: T => Seq[Timestamp],
    objectives: Map[ServiceLevelObjective[ConsumptionSpeedMetric.Value], Option[
      ConsumptionSpeedMetric.Value
    ]],
    previousLatest: Option[Instant] = None,
    currentPeriodLatest: Option[Instant] = None,
) extends Metric[T] {
  import ConsumptionSpeedMetric._

  override type V = Value
  override type Objective = ServiceLevelObjective[Value]

  override def onNext(value: T): ConsumptionSpeedMetric[T] = {
    val recordTimes = recordTimeFunction(value)
    val newPreviousLatest =
      previousLatest match {
        case None =>
          recordTimes.headOption.map { recordTime =>
            Instant.ofEpochSecond(recordTime.seconds.toLong, recordTime.nanos.toLong)
          }
        case v => v
      }
    val newCurrentPeriodLatest = recordTimes.lastOption.map { recordTime =>
      Instant.ofEpochSecond(recordTime.seconds.toLong, recordTime.nanos.toLong)
    }

    this.copy(
      previousLatest = newPreviousLatest,
      currentPeriodLatest = newCurrentPeriodLatest,
    )
  }

  override def periodicValue(): (Metric[T], Value) = {
    val value = Value(periodicSpeed)
    val updatedMetric = this.copy(
      previousLatest = if (currentPeriodLatest.isDefined) currentPeriodLatest else previousLatest,
      currentPeriodLatest = None,
      objectives = updatedObjectives(value),
    )
    (updatedMetric, value)
  }

  override def finalValue(totalDurationSeconds: Double): Value =
    Value(None)

  override def violatedObjectives: Map[ServiceLevelObjective[Value], Value] =
    objectives
      .collect {
        case (objective, value) if value.isDefined => objective -> value.get
      }

  private def periodicSpeed: Option[Double] =
    (previousLatest, currentPeriodLatest) match {
      case (Some(previous), Some(current)) =>
        Some((current.toEpochMilli - previous.toEpochMilli).toDouble / periodMillis)
      case _ =>
        Some(0.0)
    }

  private def updatedObjectives(newValue: Value): Map[ServiceLevelObjective[Value], Option[Value]] =
    objectives.map { case (objective, currentMaxValue) =>
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
      periodMillis: Long,
      recordTimeFunction: T => Seq[Timestamp],
      objectives: List[ServiceLevelObjective[Value]],
  ): ConsumptionSpeedMetric[T] =
    ConsumptionSpeedMetric(
      periodMillis,
      recordTimeFunction,
      objectives.map(objective => objective -> None).toMap,
    )

  final case class Value(relativeSpeed: Option[Double]) extends MetricValue {
    override def formatted: List[String] =
      List(s"speed: ${relativeSpeed.map(rounded).getOrElse("-")} [-]")
  }

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

}
