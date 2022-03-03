// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig
import com.daml.ledger.api.benchtool.metrics.{MetricsSet, StreamMetrics, BenchmarkResult}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
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
            .observer(
              streamName = streamConfig.name,
              logInterval = reportingPeriod,
              metrics = MetricsSet.transactionMetrics(streamConfig.objectives),
              logger = logger,
              exposedMetrics = Some(
                MetricsSet
                  .transactionExposedMetrics(streamConfig.name, metricRegistry, reportingPeriod)
              ),
            )(system, ec)
            .flatMap { observer =>
              apiServices.transactionService.transactions(streamConfig, observer)
            }
        case streamConfig: StreamConfig.TransactionTreesStreamConfig =>
          StreamMetrics
            .observer(
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
            )(system, ec)
            .flatMap { observer =>
              apiServices.transactionService.transactionTrees(streamConfig, observer)
            }
        case streamConfig: StreamConfig.ActiveContractsStreamConfig =>
          StreamMetrics
            .observer(
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
            )(system, ec)
            .flatMap { observer =>
              apiServices.activeContractsService.getActiveContracts(streamConfig, observer)
            }
        case streamConfig: StreamConfig.CompletionsStreamConfig =>
          StreamMetrics
            .observer(
              streamName = streamConfig.name,
              logInterval = reportingPeriod,
              metrics = MetricsSet.completionsMetrics(streamConfig.objectives),
              logger = logger,
              exposedMetrics = Some(
                MetricsSet
                  .completionsExposedMetrics(streamConfig.name, metricRegistry, reportingPeriod)
              ),
            )(system, ec)
            .flatMap { observer =>
              apiServices.commandCompletionService.completions(streamConfig, observer)
            }
      }
      .map { results =>
        if (results.contains(BenchmarkResult.ObjectivesViolated))
          Left("Metrics objectives not met.")
        else Right(())
      }
}
