// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.WorkflowConfig.StreamConfig
import com.daml.ledger.api.benchtool.metrics.{
  MetricRegistryOwner,
  MetricsCollector,
  MetricsSet,
  StreamMetrics,
}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.util.TypedActorSystemResourceOwner
import com.daml.ledger.resources.ResourceContext
import com.daml.metrics.MetricsReporter
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object Benchmark {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
      streams: List[StreamConfig],
      reportingPeriod: FiniteDuration,
      apiServices: LedgerApiServices,
      metricsReporter: MetricsReporter,
  )(implicit ec: ExecutionContext, resourceContext: ResourceContext): Future[Unit] = {
    val resources = for {
      system <- TypedActorSystemResourceOwner.owner()
      registry <- new MetricRegistryOwner(
        reporter = metricsReporter,
        reportingInterval = reportingPeriod,
        logger = logger,
      )
    } yield (system, registry)

    resources.use { case (system, registry) =>
      Future
        .traverse(streams) {
          case streamConfig: StreamConfig.TransactionsStreamConfig =>
            StreamMetrics
              .observer(
                streamName = streamConfig.name,
                logInterval = reportingPeriod,
                metrics = MetricsSet.transactionMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet
                    .transactionExposedMetrics(streamConfig.name, registry, reportingPeriod)
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
                    registry,
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
                metrics = MetricsSet.activeContractsMetrics,
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet.activeContractsExposedMetrics(
                    streamConfig.name,
                    registry,
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
                metrics = MetricsSet.completionsMetrics,
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet
                    .completionsExposedMetrics(streamConfig.name, registry, reportingPeriod)
                ),
              )(system, ec)
              .flatMap { observer =>
                apiServices.commandCompletionService.completions(streamConfig, observer)
              }
        }
        .transform {
          case Success(results) =>
            if (results.contains(MetricsCollector.Message.MetricsResult.ObjectivesViolated))
              Failure(new RuntimeException("Metrics objectives not met."))
            else Success(())
          case Failure(ex) =>
            Failure(ex)
        }
    }
  }
}
