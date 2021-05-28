// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.objectives.ServiceLevelObjective
import com.google.protobuf.timestamp.Timestamp

import java.time.{Clock, Duration, Instant}

final case class DelayMetric[T](
    recordTimeFunction: T => Seq[Timestamp],
    clock: Clock,
    objectives: Map[ServiceLevelObjective[DelayMetric.Value], Option[DelayMetric.Value]],
    delaysInCurrentInterval: List[Duration] = List.empty,
) extends Metric[T] {

  override type V = DelayMetric.Value
  override type Objective = ServiceLevelObjective[DelayMetric.Value]

  override def onNext(value: T): DelayMetric[T] = {
    val now = clock.instant()
    val newDelays: List[Duration] = recordTimeFunction(value).toList.map { recordTime =>
      Duration.between(
        Instant.ofEpochSecond(recordTime.seconds.toLong, recordTime.nanos.toLong),
        now,
      )
    }
    this.copy(delaysInCurrentInterval = delaysInCurrentInterval ::: newDelays)
  }

  private def updatedObjectives(
      newValue: DelayMetric.Value
  ): Map[ServiceLevelObjective[DelayMetric.Value], Option[DelayMetric.Value]] = {
    objectives
      .map { case (objective, currentViolatingValue) =>
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
  }

  override def periodicValue(): (Metric[T], DelayMetric.Value) = {
    val value: DelayMetric.Value = DelayMetric.Value(periodicMeanDelay.map(_.getSeconds))
    val updatedMetric = this.copy(
      delaysInCurrentInterval = List.empty,
      objectives = updatedObjectives(value),
    )
    (updatedMetric, value)
  }

  override def finalValue(totalDurationSeconds: Double): DelayMetric.Value =
    DelayMetric.Value(None)

  override def violatedObjectives
      : Map[ServiceLevelObjective[DelayMetric.Value], DelayMetric.Value] =
    objectives
      .collect {
        case (objective, value) if value.isDefined => objective -> value.get
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
      objectives: List[ServiceLevelObjective[Value]],
      clock: Clock,
  ): DelayMetric[T] =
    DelayMetric(
      recordTimeFunction = recordTimeFunction,
      clock = clock,
      objectives = objectives.map(objective => objective -> None).toMap,
    )

  final case class Value(meanDelaySeconds: Option[Long]) extends MetricValue {
    override def formatted: List[String] =
      List(s"mean delay: ${meanDelaySeconds.getOrElse("-")} [s]")
  }

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
}
