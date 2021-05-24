// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, SpawnProtocol}

import java.time.Instant
import scala.concurrent.duration._

object MetricsManager {

  sealed trait Message[T]
  object Message {
    sealed trait MetricsResult
    object MetricsResult {
      final case object Ok extends MetricsResult
      final case object ObjectivesViolated extends MetricsResult
    }
    final case class NewValue[T](value: T) extends Message[T]
    final case class PeriodicUpdateCommand[T]() extends Message[T]
    final case class StreamCompleted[T](replyTo: ActorRef[MetricsResult]) extends Message[T]
  }

  def apply[T](
      streamName: String,
      metrics: List[Metric[T]],
      logInterval: FiniteDuration,
  ): Behavior[Message[T]] =
    Behaviors.withTimers(timers =>
      new MetricsManager[T](timers, streamName, logInterval).handlingMessages(metrics)
    )

}

class MetricsManager[T](
    timers: TimerScheduler[MetricsManager.Message[T]],
    streamName: String,
    logInterval: FiniteDuration,
) {
  import MetricsManager._
  import MetricsManager.Message._

  timers.startTimerWithFixedDelay(PeriodicUpdateCommand(), logInterval)

  private val startTime: Instant = Instant.now()

  def handlingMessages(metrics: List[Metric[T]]): Behavior[Message[T]] = {
    Behaviors.receive { case (context, message) =>
      message match {
        case newValue: NewValue[T] =>
          handlingMessages(metrics.map(_.onNext(newValue.value)))

        case _: PeriodicUpdateCommand[T] =>
          val (newMetrics, values) = metrics.map(_.periodicValue()).unzip
          val formattedValues: List[String] = values.flatMap(_.formatted)
          context.log.info(namedMessage(formattedValues.mkString(", ")))
          handlingMessages(newMetrics)

        case message: StreamCompleted[T] =>
          context.log.info(namedMessage(summary(metrics, totalDurationSeconds)))
          message.replyTo ! result(metrics)
          Behaviors.stopped
      }
    }
  }

  private def result(metrics: List[Metric[T]]): MetricsResult = {
    val atLeastOneObjectiveViolated = metrics.exists(_.violatedObjectives.nonEmpty)

    if (atLeastOneObjectiveViolated) MetricsResult.ObjectivesViolated
    else MetricsResult.Ok
  }

  private def summary(metrics: List[Metric[T]], durationSeconds: Double): String = {
    def indented(str: String): String = s"  $str"
    val reports = metrics.map { metric =>
      val metricValues: String = metric
        .finalValue(totalDurationSeconds)
        .formatted
        .map(indented)
        .mkString("\n")

      s"""${metric.name}:
         |$metricValues""".stripMargin
    }
    val reportWidth = 80
    val bar = "=" * reportWidth
    s"""
       |$bar
       |Stream: $streamName
       |Total duration: $durationSeconds [s]
       |${reports.mkString("\n")}
       |$bar""".stripMargin
  }

  private def namedMessage(message: String) = s"[$streamName] $message"

  private def totalDurationSeconds: Double =
    (Instant.now().toEpochMilli - startTime.toEpochMilli) / 1000.0
}

object Creator {
  def apply(): Behavior[SpawnProtocol.Command] =
    Behaviors.setup { context =>
      context.log.debug(s"Starting Creator actor")
      SpawnProtocol()
    }
}
