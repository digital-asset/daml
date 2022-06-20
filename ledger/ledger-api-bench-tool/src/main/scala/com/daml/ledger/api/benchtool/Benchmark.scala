// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import java.util.concurrent.TimeUnit

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig
import com.daml.ledger.api.benchtool.metrics.{
  BenchmarkResult,
  MeteredStreamObserver,
  MetricsSet,
  StreamMetrics,
}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
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
      metricRegistry: MetricRegistry,
      system: ActorSystem[SpawnProtocol.Command],
  )(implicit ec: ExecutionContext): Future[Either[String, Unit]] =
    Future
      .traverse(streamConfigs) {
        case streamConfig: StreamConfig.TransactionsStreamConfig =>
          StreamMetrics
            .observer[GetTransactionsResponse](
              streamName = streamConfig.name,
              logInterval = reportingPeriod,
              metrics = MetricsSet.transactionMetrics(streamConfig.objectives),
              logger = logger,
              exposedMetrics = Some(
                MetricsSet
                  .transactionExposedMetrics(streamConfig.name, metricRegistry, reportingPeriod)
              ),
              itemCountingFunction = MetricsSet.countFlatTransactionsEvents,
              maxItemCount = streamConfig.maxItemCount,
            )(system, ec)
            .flatMap { observer =>
              streamConfig.timeoutInSecondsO
                .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
              apiServices.transactionService.transactions(streamConfig, observer)
            }
        case streamConfig: StreamConfig.TransactionTreesStreamConfig =>
          StreamMetrics
            .observer[GetTransactionTreesResponse](
              streamName = streamConfig.name,
              logInterval = reportingPeriod,
              metrics = MetricsSet.transactionTreesMetrics(streamConfig.objectives),
              logger = logger,
              exposedMetrics = Some(
                MetricsSet.transactionTreesExposedMetrics(
                  streamConfig.name,
                  metricRegistry,
                  reportingPeriod,
                )
              ),
              itemCountingFunction = MetricsSet.countTreeTransactionsEvents,
              maxItemCount = streamConfig.maxItemCount,
            )(system, ec)
            .flatMap { observer =>
              streamConfig.timeoutInSecondsO
                .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
              apiServices.transactionService.transactionTrees(streamConfig, observer)
            }
        case streamConfig: StreamConfig.ActiveContractsStreamConfig =>
          StreamMetrics
            .observer[GetActiveContractsResponse](
              streamName = streamConfig.name,
              logInterval = reportingPeriod,
              metrics = MetricsSet.activeContractsMetrics(streamConfig.objectives),
              logger = logger,
              exposedMetrics = Some(
                MetricsSet.activeContractsExposedMetrics(
                  streamConfig.name,
                  metricRegistry,
                  reportingPeriod,
                )
              ),
              itemCountingFunction = (response) => MetricsSet.countActiveContracts(response).toLong,
              maxItemCount = streamConfig.maxItemCount,
            )(system, ec)
            .flatMap { observer =>
              streamConfig.timeoutInSecondsO
                .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
              apiServices.activeContractsService.getActiveContracts(streamConfig, observer)
            }
        case streamConfig: StreamConfig.CompletionsStreamConfig =>
          StreamMetrics
            .observer[CompletionStreamResponse](
              streamName = streamConfig.name,
              logInterval = reportingPeriod,
              metrics = MetricsSet.completionsMetrics(streamConfig.objectives),
              logger = logger,
              exposedMetrics = Some(
                MetricsSet
                  .completionsExposedMetrics(streamConfig.name, metricRegistry, reportingPeriod)
              ),
              itemCountingFunction = (response) => MetricsSet.countCompletions(response).toLong,
              maxItemCount = streamConfig.maxItemCount,
            )(system, ec)
            .flatMap { observer: MeteredStreamObserver[CompletionStreamResponse] =>
              streamConfig.timeoutInSecondsO
                .foreach(timeout => scheduleCancelStreamTask(timeout, observer))
              apiServices.commandCompletionService.completions(streamConfig, observer)
            }
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
}
