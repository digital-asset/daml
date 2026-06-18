// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.timer.Delayed
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.StreamConfig
import com.digitalasset.canton.ledger.api.benchtool.metrics.{
  BenchmarkResult,
  MetricsSet,
  StreamMetrics,
}
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.util.ObserverWithResult
import org.apache.pekko.actor.typed.{ActorSystem, SpawnProtocol}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

object Benchmark {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
      streamConfigs: List[StreamConfig],
      reportingPeriod: FiniteDuration,
      apiServices: LedgerApiServices,
      metricsFactory: LabeledMetricsFactory,
      system: ActorSystem[SpawnProtocol.Command],
  )(implicit ec: ExecutionContext): Future[Either[String, Unit]] =
    Future
      .traverse(streamConfigs) {
        case streamConfig: StreamConfig.TransactionsStreamConfig =>
          for {
            _ <- delaySubscriptionIfConfigured(streamConfig)(system)
            observer <- StreamMetrics
              .observer[GetUpdatesResponse](
                streamName = streamConfig.name,
                logInterval = reportingPeriod,
                metrics = MetricsSet.transactionMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet
                    .transactionExposedMetrics(streamConfig.name, metricsFactory)
                ),
                itemCountingFunction = MetricsSet.countTransactionsEvents,
                maxItemCount = streamConfig.maxItemCount,
              )(system, ec)
            _ = streamConfig.timeoutO
              .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
            result <- apiServices.updateService.transactions(streamConfig, observer)
          } yield result
        case streamConfig: StreamConfig.TransactionLedgerEffectsStreamConfig =>
          for {
            _ <- delaySubscriptionIfConfigured(streamConfig)(system)
            observer <- StreamMetrics
              .observer[GetUpdatesResponse](
                streamName = streamConfig.name,
                logInterval = reportingPeriod,
                metrics = MetricsSet.transactionMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet.transactionExposedMetrics(
                    streamConfig.name,
                    metricsFactory,
                  )
                ),
                itemCountingFunction = MetricsSet.countTransactionsEvents,
                maxItemCount = streamConfig.maxItemCount,
              )(system, ec)
            _ = streamConfig.timeoutO
              .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
            result <- apiServices.updateService.transactionsLedgerEffects(streamConfig, observer)
          } yield result
        case streamConfig: StreamConfig.ActiveContractsStreamConfig =>
          for {
            _ <- delaySubscriptionIfConfigured(streamConfig)(system)
            observer <- StreamMetrics
              .observer[GetActiveContractsResponse](
                streamName = streamConfig.name,
                logInterval = reportingPeriod,
                metrics = MetricsSet.activeContractsMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet.activeContractsExposedMetrics(
                    streamConfig.name,
                    metricsFactory,
                  )
                ),
                itemCountingFunction = response => MetricsSet.countActiveContracts(response).toLong,
                maxItemCount = streamConfig.maxItemCount,
              )(system, ec)
            _ = streamConfig.timeoutO
              .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
            result <- apiServices.stateService.getActiveContracts(streamConfig, observer)
          } yield result
        case streamConfig: StreamConfig.CompletionsStreamConfig =>
          for {
            _ <- delaySubscriptionIfConfigured(streamConfig)(system)
            observer <- StreamMetrics
              .observer[CompletionStreamResponse](
                streamName = streamConfig.name,
                logInterval = reportingPeriod,
                metrics = MetricsSet.completionsMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet
                    .completionsExposedMetrics(streamConfig.name, metricsFactory)
                ),
                itemCountingFunction = response => MetricsSet.countCompletions(response).toLong,
                maxItemCount = streamConfig.maxItemCount,
              )(system, ec)
            _ = streamConfig.timeoutO
              .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
            result <- apiServices.commandCompletionService.completions(streamConfig, observer)
          } yield result
      }
      .map { results =>
        Either.cond(
          !results.contains(BenchmarkResult.ObjectivesViolated),
          (),
          "Metrics objectives not met.",
        )
      }

  def scheduleCancelStreamTask(timeoutDuration: Duration, observer: ObserverWithResult[?, ?])(
      implicit ec: ExecutionContext
  ): Unit = {
    val _ = Delayed.by(t = timeoutDuration)(
      observer.cancel()
    )
  }

  private def delaySubscriptionIfConfigured(
      streamConfig: StreamConfig
  )(implicit system: ActorSystem[SpawnProtocol.Command]): Future[Unit] =
    streamConfig.subscriptionDelay match {
      case Some(delay) =>
        logger.info(
          s"Delaying stream subscription with $delay for stream $streamConfig"
        )
        org.apache.pekko.pattern.after(delay)(Future.unit)
      case None => Future.unit
    }
}
