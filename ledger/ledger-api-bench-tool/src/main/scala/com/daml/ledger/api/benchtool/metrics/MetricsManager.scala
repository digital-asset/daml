// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.CoordinatedShutdown
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem, Props, SpawnProtocol}
import akka.util.Timeout
import com.daml.ledger.api.benchtool.util.ReportFormatter
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class MetricsManager[T](
    collector: ActorRef[MetricsCollector.Message],
    logInterval: FiniteDuration,
    streamName: String,
)(implicit
    system: ActorSystem[SpawnProtocol.Command]
) {
  CoordinatedShutdown(system).addTask(
    phase = CoordinatedShutdown.PhaseBeforeServiceUnbind,
    taskName = "report-results",
  ) { () =>
    logger.info(s"Shutting down infrastructure for stream: $streamName")
    result().map(_ => akka.Done)(system.executionContext)
  }

  system.scheduler.scheduleWithFixedDelay(logInterval, logInterval)(() => {
    implicit val timeout: Timeout = Timeout(logInterval)
    collector
      .ask(MetricsCollector.Message.PeriodicReportRequest)
      .map { response =>
        logger.info(
          ReportFormatter.formatPeriodicReport(
            streamName = streamName,
            periodicReport = response,
          )
        )
      }(system.executionContext)
    ()
  })(system.executionContext)

  def sendNewValue(value: T): Unit =
    collector ! MetricsCollector.Message.NewValue(value)

  def result[Result](): Future[StreamResult] = {
    implicit val timeout: Timeout = Timeout(3.seconds)
    val result = collector.ask(MetricsCollector.Message.FinalReportRequest)

    result.map { response: MetricsCollector.Response.FinalReport =>
      logger.info(
        ReportFormatter.formatFinalReport(
          streamName = streamName,
          finalReport = response,
        )
      )
      if (response.metricsData.exists(_.violatedObjective.isDefined))
        StreamResult.ObjectivesViolated
      else
        StreamResult.Ok
    }(system.executionContext)
  }

  private val logger = LoggerFactory.getLogger(getClass)
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
          metrics = metrics,
          exposedMetrics = exposedMetrics,
        ),
        name = s"${streamName}-collector",
        props = Props.empty,
        _,
      )
    )

    collectorActor.map(collector =>
      MetricsManager[StreamElem](
        collector = collector,
        logInterval = logInterval,
        streamName = streamName,
      )
    )
  }
}
