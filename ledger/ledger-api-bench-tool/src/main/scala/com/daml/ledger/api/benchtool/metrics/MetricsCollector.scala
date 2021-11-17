// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.daml.ledger.api.benchtool.metrics.objectives.ServiceLevelObjective
import com.daml.ledger.api.benchtool.util.TimeUtil

import java.time.Instant

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
    final case class FinalReport(metricsData: List[MetricFinalReportData]) extends Response
  }

  def apply[T](
      metrics: List[Metric[T]],
      exposedMetrics: Option[ExposedMetrics[T]] = None,
  ): Behavior[Message] = {
    val startTime: Instant = Instant.now()
    new MetricsCollector[T](startTime, exposedMetrics).handlingMessages(metrics, startTime)
  }
}

class MetricsCollector[T](
    startTime: Instant,
    exposedMetrics: Option[ExposedMetrics[T]],
) {
  import MetricsCollector._
  import MetricsCollector.Message._
  import MetricsCollector.Response._

  def handlingMessages(metrics: List[Metric[T]], lastPeriodicCheck: Instant): Behavior[Message] = {
    Behaviors.receive { case (context, message) =>
      message match {
        case newValue: NewValue[T] =>
          exposedMetrics.foreach(_.onNext(newValue.value))
          handlingMessages(metrics.map(_.onNext(newValue.value)), lastPeriodicCheck)

        case request: PeriodicReportRequest =>
          val currentTime = Instant.now()
          val (newMetrics, values) = metrics
            .map(_.periodicValue(TimeUtil.durationBetween(lastPeriodicCheck, currentTime)))
            .unzip
          context.log.info(s"RETURNING VALUES")
          request.replyTo ! Response.PeriodicReport(values)
          handlingMessages(newMetrics, currentTime)

        case request: FinalReportRequest =>
          val duration = TimeUtil.durationBetween(startTime, Instant.now())
          val data: List[MetricFinalReportData] =
            metrics.map { metric =>
              MetricFinalReportData(
                name = metric.name,
                value = metric.finalValue(duration),
                violatedObjective = metric.violatedObjective,
              )
            }
          context.log.info(s"RETURNING FINAL DATA")
          request.replyTo ! FinalReport(data)
          Behaviors.stopped
      }
    }
  }
}
