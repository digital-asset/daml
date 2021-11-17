// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.daml.ledger.api.benchtool.metrics.objectives.ServiceLevelObjective
import com.daml.ledger.api.benchtool.util.TimeUtil

import java.time.{Clock, Duration, Instant}

object MetricsCollector {

  sealed trait Message
  object Message {
    final case class NewValue[T](value: T) extends Message
    final case class PeriodicReportRequest(replyTo: ActorRef[Response.PeriodicReport])
        extends Message
    final case class FinalReportRequest(replyTo: ActorRef[Response.FinalReport]) extends Message
  }

  sealed trait Response
  object Response {
    final case class PeriodicReport(values: List[MetricValue]) extends Response
    final case class MetricFinalReportData(
        name: String,
        value: MetricValue,
        violatedObjective: Option[(ServiceLevelObjective[_], MetricValue)],
    )
    final case class FinalReport(totalDuration: Duration, metricsData: List[MetricFinalReportData])
        extends Response
  }

  def apply[T](
      metrics: List[Metric[T]],
      exposedMetrics: Option[ExposedMetrics[T]] = None,
  ): Behavior[Message] = {
    val clock = Clock.systemUTC()
    val startTime: Instant = clock.instant()
    new MetricsCollector[T](exposedMetrics, clock).handlingMessages(metrics, startTime, startTime)
  }
}

class MetricsCollector[T](exposedMetrics: Option[ExposedMetrics[T]], clock: Clock) {
  import MetricsCollector._
  import MetricsCollector.Message._
  import MetricsCollector.Response._

  def handlingMessages(
      metrics: List[Metric[T]],
      lastPeriodicCheck: Instant,
      startTime: Instant,
  ): Behavior[Message] = {
    Behaviors.receive { case (_, message) =>
      message match {
        case newValue: NewValue[T] =>
          exposedMetrics.foreach(_.onNext(newValue.value))
          handlingMessages(metrics.map(_.onNext(newValue.value)), lastPeriodicCheck, startTime)

        case request: PeriodicReportRequest =>
          val currentTime = clock.instant()
          val (newMetrics, values) = metrics
            .map(_.periodicValue(TimeUtil.durationBetween(lastPeriodicCheck, currentTime)))
            .unzip
          request.replyTo ! Response.PeriodicReport(values)
          handlingMessages(newMetrics, currentTime, startTime)

        case request: FinalReportRequest =>
          val duration = TimeUtil.durationBetween(startTime, clock.instant())
          val data: List[MetricFinalReportData] =
            metrics.map { metric =>
              MetricFinalReportData(
                name = metric.name,
                value = metric.finalValue(duration),
                violatedObjective = metric.violatedObjective,
              )
            }
          request.replyTo ! FinalReport(duration, data)
          Behaviors.stopped
      }
    }
  }
}
