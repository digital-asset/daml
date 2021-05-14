// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{Behavior, SpawnProtocol}

import java.time.Instant
import scala.concurrent.duration._

object MetricsManager {

  sealed trait Message[T]
  object Message {
    final case class NewValue[T](value: T) extends Message[T]
    final case class StreamCompleted[T]() extends Message[T]
    final case class PeriodicUpdateCommand[T]() extends Message[T]
  }

  def apply[T](
      streamName: String,
      metrics: List[Metric[T]],
      logInterval: FiniteDuration,
  ): Behavior[Message[T]] =
    Behaviors.withTimers(timers =>
      new MetricsManager[T](timers, streamName, logInterval, metrics).handlingMessages()
    )

}

// TODO: separate actor for logging?
class MetricsManager[T](
    timers: TimerScheduler[MetricsManager.Message[T]],
    streamName: String,
    logInterval: FiniteDuration,
    metrics: List[Metric[T]], //TODO: make this immutable and part of the behavior
) {
  import MetricsManager._
  import MetricsManager.Message._

  timers.startTimerWithFixedDelay(PeriodicUpdateCommand(), logInterval)

  private val startTime: Instant = Instant.now()

  def handlingMessages(): Behavior[Message[T]] = {
    Behaviors.receive { case (context, message) =>
      message match {
        case newValue: NewValue[T] =>
          metrics.foreach(_.onNext(newValue.value))
          Behaviors.same

        case _: PeriodicUpdateCommand[T] =>
          val periodicUpdates = metrics.map(_.periodicUpdate())
          context.log.info(namedMessage(periodicUpdates.mkString(", ")))
          Behaviors.same

        case _: StreamCompleted[T] =>
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
