// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.util.TimeUtil
import com.google.protobuf.timestamp.Timestamp

import java.time.{Clock, Duration}

final case class DelayMetric[T](
    recordTimeFunction: T => Seq[Timestamp],
    clock: Clock,
    objective: Option[(DelayMetric.MaxDelay, Option[DelayMetric.Value])],
    delaysInCurrentInterval: List[Duration] = List.empty,
) extends Metric[T] {
  import DelayMetric._

  override type V = Value
  override type Objective = MaxDelay

  override def onNext(value: T): DelayMetric[T] = {
    val now = clock.instant()
    val newDelays: List[Duration] = recordTimeFunction(value).toList
      .map(TimeUtil.durationBetween(_, now))
    this.copy(delaysInCurrentInterval = delaysInCurrentInterval ::: newDelays)
  }

  override def periodicValue(periodDuration: Duration): (Metric[T], Value) = {
    val value = Value(periodicMeanDelay.map(_.getSeconds))
    val updatedMetric = this.copy(
      delaysInCurrentInterval = List.empty,
      objective = updatedObjective(value),
    )
    (updatedMetric, value)
  }

  override def finalValue(totalDuration: Duration): Value =
    Value(None)

  override def violatedPeriodicObjectives: List[(MaxDelay, Value)] =
    objective.collect {
      case (objective, value) if value.isDefined => objective -> value.get
    }.toList

  override def violatedFinalObjectives(
      totalDuration: Duration
  ): List[(MaxDelay, Value)] = Nil

  private def updatedObjective(
      newValue: Value
  ): Option[(MaxDelay, Option[DelayMetric.Value])] =
    objective.map { case (objective, currentViolatingValue) =>
      // verify if the new value violates objective's requirements
      if (objective.isViolatedBy(newValue)) {
        currentViolatingValue match {
          case None =>
            // if the new value violates objective's requirements and there is no other violating value,
            // record the new value
            objective -> Some(newValue)
          case Some(currentValue) =>
            // if the new value violates objective's requirements and there is already a value that violates
            // requirements, record the maximum value of the two
            objective -> Some(Ordering[V].max(currentValue, newValue))
        }
      } else {
        objective -> currentViolatingValue
      }
    }

  private def periodicMeanDelay: Option[Duration] =
    if (delaysInCurrentInterval.nonEmpty)
      Some(
        delaysInCurrentInterval
          .reduceLeft(_.plus(_))
          .dividedBy(delaysInCurrentInterval.length.toLong)
      )
    else None
}

object DelayMetric {

  def empty[T](
      recordTimeFunction: T => Seq[Timestamp],
      clock: Clock,
      objective: Option[MaxDelay] = None,
  ): DelayMetric[T] =
    DelayMetric(
      recordTimeFunction = recordTimeFunction,
      clock = clock,
      objective = objective.map(objective => objective -> None),
    )

  final case class Value(meanDelaySeconds: Option[Long]) extends MetricValue

  object Value {
    implicit val valueOrdering: Ordering[Value] = (x: Value, y: Value) => {
      (x.meanDelaySeconds, y.meanDelaySeconds) match {
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

  final case class MaxDelay(maxDelaySeconds: Long)
      extends ServiceLevelObjective[DelayMetric.Value] {
    override def isViolatedBy(metricValue: DelayMetric.Value): Boolean =
      metricValue.meanDelaySeconds.exists(_ > maxDelaySeconds)
  }

}
