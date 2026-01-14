// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.metrics

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.benchtool.util.ReportFormatter
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Props, SpawnProtocol}
import org.apache.pekko.actor.{Cancellable, CoordinatedShutdown}
import org.apache.pekko.util.Timeout
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

import MetricsCollector.Response

trait MetricsManager[T] {
  def sendNewValue(value: T): Unit
  def result(): Future[BenchmarkResult]
}

final case class MetricsManagerImpl[T](
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
    periodicRequest.cancel().discard
    implicit val timeout: Timeout = Timeout(3.seconds)
    collector
      .ask(MetricsCollector.Message.FinalReportRequest.apply)
      .map { (response: MetricsCollector.Response.FinalReport) =>
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
    result().map(_ => org.apache.pekko.Done)(system.executionContext)
  }

  private val periodicRequest: Cancellable =
    system.scheduler.scheduleWithFixedDelay(logInterval, logInterval) { () =>
      implicit val timeout: Timeout = Timeout(logInterval)
      collector
        .ask(MetricsCollector.Message.PeriodicReportRequest.apply)
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
        .discard
      ()
    }(system.executionContext)

  private val logger = LoggerFactory.getLogger(getClass)
}

object MetricsManager {
  def create[StreamElem](
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

  final case class NoOpMetricsManager[T]() extends MetricsManager[T] {
    override def sendNewValue(value: T): Unit = {
      val _ = value
    }
    override def result(): Future[BenchmarkResult] = Future.successful(BenchmarkResult.Ok)
  }
}
