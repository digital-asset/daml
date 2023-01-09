// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import java.util.concurrent.TimeUnit

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig
import com.daml.ledger.api.benchtool.metrics.{BenchmarkResult, MetricsSet, StreamMetrics}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.metrics.api.MetricHandle.Factory
import com.daml.timer.Delayed
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

object Benchmark {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
      streamConfigs: List[StreamConfig],
      reportingPeriod: FiniteDuration,
      apiServices: LedgerApiServices,
      metricsFactory: Factory,
      system: ActorSystem[SpawnProtocol.Command],
  )(implicit ec: ExecutionContext): Future[Either[String, Unit]] =
    Future
      .traverse(streamConfigs) {
        case streamConfig: StreamConfig.TransactionsStreamConfig =>
          for {
            _ <- delaySubscriptionIfConfigured(streamConfig)(system)
            observer <- StreamMetrics
              .observer[GetTransactionsResponse](
                streamName = streamConfig.name,
                logInterval = reportingPeriod,
                metrics = MetricsSet.transactionMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet
                    .transactionExposedMetrics(streamConfig.name, metricsFactory)
                ),
                itemCountingFunction = MetricsSet.countFlatTransactionsEvents,
                maxItemCount = streamConfig.maxItemCount,
              )(system, ec)
            _ = streamConfig.timeoutInSecondsO
              .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
            result <- apiServices.transactionService.transactions(streamConfig, observer)
          } yield result
        case streamConfig: StreamConfig.TransactionTreesStreamConfig =>
          for {
            _ <- delaySubscriptionIfConfigured(streamConfig)(system)
            observer <- StreamMetrics
              .observer[GetTransactionTreesResponse](
                streamName = streamConfig.name,
                logInterval = reportingPeriod,
                metrics = MetricsSet.transactionTreesMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet.transactionTreesExposedMetrics(
                    streamConfig.name,
                    metricsFactory,
                  )
                ),
                itemCountingFunction = MetricsSet.countTreeTransactionsEvents,
                maxItemCount = streamConfig.maxItemCount,
              )(system, ec)
            _ = streamConfig.timeoutInSecondsO
              .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
            result <- apiServices.transactionService.transactionTrees(streamConfig, observer)
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
                itemCountingFunction =
                  (response) => MetricsSet.countActiveContracts(response).toLong,
                maxItemCount = streamConfig.maxItemCount,
              )(system, ec)
            _ = streamConfig.timeoutInSecondsO
              .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
            result <- apiServices.activeContractsService.getActiveContracts(streamConfig, observer)
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
                itemCountingFunction = (response) => MetricsSet.countCompletions(response).toLong,
                maxItemCount = streamConfig.maxItemCount,
              )(system, ec)
            _ = streamConfig.timeoutInSecondsO
              .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
            result <- apiServices.commandCompletionService.completions(streamConfig, observer)
          } yield result
      }
      .map { results =>
        if (results.contains(BenchmarkResult.ObjectivesViolated))
          Left("Metrics objectives not met.")
        else Right(())
      }

  def scheduleCancelStreamTask(timeoutInSeconds: Long, observer: ObserverWithResult[_, _])(implicit
      ec: ExecutionContext
  ): Unit = {
    val _ = Delayed.by(t = Duration(timeoutInSeconds, TimeUnit.SECONDS))(
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
        akka.pattern.after(delay)(Future.unit)
      case None => Future.unit
    }
}
