// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, SpawnProtocol}
import com.daml.ledger.api.benchtool.util.{MetricReporter, TimeUtil}

import java.time.Instant
import scala.concurrent.duration._

object MetricsManager {

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
      reporter: MetricReporter,
  ): Behavior[Message] =
    Behaviors.withTimers { timers =>
      val startTime: Instant = Instant.now()
      new MetricsManager[T](timers, streamName, logInterval, reporter, startTime)
        .handlingMessages(metrics, startTime)
    }

}

class MetricsManager[T](
    timers: TimerScheduler[MetricsManager.Message],
    streamName: String,
    logInterval: FiniteDuration,
    reporter: MetricReporter,
    startTime: Instant,
) {
  import MetricsManager._
  import MetricsManager.Message._

  timers.startTimerWithFixedDelay(PeriodicUpdateCommand(), logInterval)

  def handlingMessages(metrics: List[Metric[T]], lastPeriodicCheck: Instant): Behavior[Message] = {
    Behaviors.receive { case (context, message) =>
      message match {
        case newValue: NewValue[T] =>
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

object Creator {
  def apply(): Behavior[SpawnProtocol.Command] =
    Behaviors.setup { context =>
      context.log.debug(s"Starting Creator actor")
      SpawnProtocol()
    }
}
