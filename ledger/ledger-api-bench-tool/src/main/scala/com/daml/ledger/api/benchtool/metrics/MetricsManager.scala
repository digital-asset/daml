// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{Behavior, SpawnProtocol}
import com.daml.ledger.api.v1.transaction_service.GetTransactionsResponse

import java.time.Instant
import scala.concurrent.duration._

object MetricsManager {

  sealed trait Message
  final case class NewValue(value: GetTransactionsResponse) extends Message
  final case object StreamCompleted extends Message
  final case object PeriodicUpdateCommand extends Message

  def apply(
      streamName: String,
      metrics: List[Metric[GetTransactionsResponse]],
      logInterval: FiniteDuration,
  ): Behavior[Message] =
    Behaviors.withTimers(timers =>
      new MetricsManager(timers, streamName, logInterval, metrics).handlingMessages()
    )

}

// TODO: separate actor for logging?
class MetricsManager(
    timers: TimerScheduler[MetricsManager.Message],
    streamName: String,
    logInterval: FiniteDuration,
    metrics: List[
      Metric[GetTransactionsResponse]
    ], //TODO: make this immutable and part of the behavior
) {
  import MetricsManager._

  timers.startTimerWithFixedDelay(PeriodicUpdateCommand, logInterval)

  private val startTime: Instant = Instant.now()

  def handlingMessages(): Behavior[Message] = {
    Behaviors.receive { case (context, message) =>
      message match {
        case NewValue(value) =>
          metrics.foreach(_.onNext(value))
          Behaviors.same

        case PeriodicUpdateCommand =>
          val periodicUpdates = metrics.map(_.periodicUpdate())
          context.log.info(namedMessage(periodicUpdates.mkString(", ")))
          Behaviors.same

        case StreamCompleted =>
          context.log.info(namedMessage(summary(totalDurationSeconds)))
          Behaviors.stopped
      }
    }
  }

  private def summary(durationSeconds: Double): String = {
    val reports = metrics.flatMap { metric =>
      metric.completeInfo(durationSeconds) match {
        case Nil => Nil
        case results => List(s"""${metric.name}:
                                |${results.map(r => s"  $r").mkString("\n")}""".stripMargin)
      }
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
