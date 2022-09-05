// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.{Cancellable, CoordinatedShutdown}
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem, Props, SpawnProtocol}
import akka.util.Timeout
import com.daml.ledger.api.benchtool.metrics.MetricsCollector.Response
import com.daml.ledger.api.benchtool.util.ReportFormatter
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait MetricsManager[T] {
  def sendNewValue(value: T): Unit
  def result(): Future[BenchmarkResult]
}

case class MetricsManagerImpl[T](
    collector: ActorRef[MetricsCollector.Message],
    logInterval: FiniteDuration,
    observedMetric: String,
)(implicit
    system: ActorSystem[SpawnProtocol.Command]
) extends MetricsManager[T] {
  def sendNewValue(value: T): Unit =
    collector ! MetricsCollector.Message.NewValue(value)

  def result(): Future[BenchmarkResult] = {
    logger.debug(s"Requesting result of stream: $observedMetric")
    periodicRequest.cancel()
    implicit val timeout: Timeout = Timeout(3.seconds)
    collector
      .ask(MetricsCollector.Message.FinalReportRequest)
      .map { response: MetricsCollector.Response.FinalReport =>
        logger.info(
          ReportFormatter.formatFinalReport(
            streamName = observedMetric,
            finalReport = response,
          )
        )
        val atLeastOneObjectiveViolated = response.metricsData.exists(_.violatedObjectives.nonEmpty)
        if (atLeastOneObjectiveViolated) BenchmarkResult.ObjectivesViolated
        else BenchmarkResult.Ok
      }(system.executionContext)
  }

  CoordinatedShutdown(system).addTask(
    phase = CoordinatedShutdown.PhaseBeforeServiceUnbind,
    taskName = "report-results",
  ) { () =>
    logger.debug(s"Shutting down infrastructure for stream: $observedMetric")
    result().map(_ => akka.Done)(system.executionContext)
  }

  private val periodicRequest: Cancellable =
    system.scheduler.scheduleWithFixedDelay(logInterval, logInterval)(() => {
      implicit val timeout: Timeout = Timeout(logInterval)
      collector
        .ask(MetricsCollector.Message.PeriodicReportRequest)
        .collect {
          case Response.ReportNotReady => ()
          case response: Response.PeriodicReport =>
            logger.info(
              ReportFormatter.formatPeriodicReport(
                streamName = observedMetric,
                periodicReport = response,
              )
            )
        }(system.executionContext)
      ()
    })(system.executionContext)

  private val logger = LoggerFactory.getLogger(getClass)
}

object MetricsManager {
  def apply[StreamElem](
      observedMetric: String,
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
        name = s"$observedMetric-collector",
        props = Props.empty,
        _,
      )
    )

    collectorActor.map(collector =>
      MetricsManagerImpl[StreamElem](
        collector = collector,
        logInterval = logInterval,
        observedMetric = observedMetric,
      )
    )
  }

  case class NoOpMetricsManager[T]() extends MetricsManager[T] {
    override def sendNewValue(value: T): Unit = {
      val _ = value
    }
    override def result(): Future[BenchmarkResult] = Future.successful(BenchmarkResult.Ok)
  }
}
