// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.CoordinatedShutdown
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem, Props, SpawnProtocol}
import akka.util.Timeout
import com.daml.ledger.api.benchtool.util.MetricReporter

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class MetricsManager[T](collector: ActorRef[MetricsCollector.Message])(implicit
    system: ActorSystem[SpawnProtocol.Command]
) {
  CoordinatedShutdown(system).addTask(
    phase = CoordinatedShutdown.PhaseBeforeServiceUnbind,
    taskName = "report-results",
  ) { () =>
    println(s"Coordinated shutdown in progress...")
    result().map(_ => akka.Done)(system.executionContext)
  }

  def sendNewValue(value: T): Unit =
    collector ! MetricsCollector.Message.NewValue(value)

  def result[Result](): Future[MetricsCollector.Message.MetricsResult] = {
    implicit val timeout: Timeout = Timeout(3.seconds)
    collector.ask(MetricsCollector.Message.StreamCompleted)
  }
}

object MetricsManager {
  def apply[StreamElem](
      streamName: String,
      logInterval: FiniteDuration,
      metrics: List[Metric[StreamElem]],
      exposedMetrics: Option[ExposedMetrics[StreamElem]],
  )(implicit
      system: ActorSystem[SpawnProtocol.Command],
      ec: ExecutionContext,
  ): Future[MetricsManager[StreamElem]] = {
    implicit val timeout: Timeout = Timeout(3.seconds)

    val collectorActor: Future[ActorRef[MetricsCollector.Message]] = system.ask(
      SpawnProtocol.Spawn(
        behavior = MetricsCollector(
          streamName = streamName,
          metrics = metrics,
          logInterval = logInterval,
          reporter = MetricReporter.Default,
          exposedMetrics = exposedMetrics,
        ),
        name = s"${streamName}-collector",
        props = Props.empty,
        _,
      )
    )

    collectorActor.map(MetricsManager[StreamElem](_))
  }
}
