// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.daml.ledger.api.benchtool.util.{MetricFormatter, TimeUtil}

import java.time.Instant
import scala.concurrent.duration._

object MetricsCollector {

  sealed trait Message
  object Message {
    sealed trait MetricsResult
    object MetricsResult {
      final case object Ok extends MetricsResult
      final case object ObjectivesViolated extends MetricsResult
    }
    final case class NewValue[T](value: T) extends Message
    final case class PeriodicUpdateCommand() extends Message
    final case class StreamCompleted(replyTo: ActorRef[MetricsResult]) extends Message
  }

  def apply[T](
      streamName: String,
      metrics: List[Metric[T]],
      logInterval: FiniteDuration,
      reporter: MetricFormatter,
      exposedMetrics: Option[ExposedMetrics[T]] = None,
  ): Behavior[Message] =
    Behaviors.withTimers { timers =>
      val startTime: Instant = Instant.now()
      new MetricsCollector[T](timers, streamName, logInterval, reporter, startTime, exposedMetrics)
        .handlingMessages(metrics, startTime)
    }

}

class MetricsCollector[T](
    timers: TimerScheduler[MetricsCollector.Message],
    streamName: String,
    logInterval: FiniteDuration,
    reporter: MetricFormatter,
    startTime: Instant,
    exposedMetrics: Option[ExposedMetrics[T]],
) {
  import MetricsCollector._
  import MetricsCollector.Message._

  timers.startTimerWithFixedDelay(PeriodicUpdateCommand(), logInterval)

  def handlingMessages(metrics: List[Metric[T]], lastPeriodicCheck: Instant): Behavior[Message] = {
    Behaviors.receive { case (context, message) =>
      message match {
        case newValue: NewValue[T] =>
          exposedMetrics.foreach(_.onNext(newValue.value))
          handlingMessages(metrics.map(_.onNext(newValue.value)), lastPeriodicCheck)

        case _: PeriodicUpdateCommand =>
          val currentTime = Instant.now()
          val (newMetrics, values) = metrics
            .map(_.periodicValue(TimeUtil.durationBetween(lastPeriodicCheck, currentTime)))
            .unzip
          context.log.info(namedMessage(reporter.formattedValues(values)))
          handlingMessages(newMetrics, currentTime)

        case message: StreamCompleted =>
          context.log.info(
            namedMessage(
              reporter.finalReport(
                streamName = streamName,
                metrics = metrics,
                duration = totalDuration,
              )
            )
          )
          message.replyTo ! result(metrics)
          Behaviors.stopped
      }
    }
  }

  private def result(metrics: List[Metric[T]]): MetricsResult = {
    val atLeastOneObjectiveViolated = metrics.exists(_.violatedObjective.nonEmpty)

    if (atLeastOneObjectiveViolated) MetricsResult.ObjectivesViolated
    else MetricsResult.Ok
  }

  private def namedMessage(message: String) = s"[$streamName] $message"

  private def totalDuration: java.time.Duration =
    TimeUtil.durationBetween(startTime, Instant.now())
}
