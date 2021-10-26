// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.{
  MetricRegistryOwner,
  MetricsCollector,
  MetricsSet,
  StreamMetrics,
}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.util.TypedActorSystemResourceOwner
import com.daml.ledger.resources.ResourceContext
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object Benchmark {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
      config: Config,
      apiServices: LedgerApiServices,
  )(implicit ec: ExecutionContext, resourceContext: ResourceContext): Future[Unit] = {
    val resources = for {
      system <- TypedActorSystemResourceOwner.owner()
      registry <- new MetricRegistryOwner(
        reporter = config.metricsReporter,
        reportingInterval = config.reportingPeriod,
        logger = logger,
      )
    } yield (system, registry)

    resources.use { case (system, registry) =>
      Future
        .traverse(config.streams) {
          case streamConfig: Config.StreamConfig.TransactionsStreamConfig =>
            StreamMetrics
              .observer(
                streamName = streamConfig.name,
                logInterval = config.reportingPeriod,
                metrics = MetricsSet.transactionMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet
                    .transactionExposedMetrics(streamConfig.name, registry, config.reportingPeriod)
                ),
              )(system, ec)
              .flatMap { observer =>
                apiServices.transactionService.transactions(streamConfig, observer)
              }
          case streamConfig: Config.StreamConfig.TransactionTreesStreamConfig =>
            StreamMetrics
              .observer(
                streamName = streamConfig.name,
                logInterval = config.reportingPeriod,
                metrics = MetricsSet.transactionTreesMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet.transactionTreesExposedMetrics(
                    streamConfig.name,
                    registry,
                    config.reportingPeriod,
                  )
                ),
              )(system, ec)
              .flatMap { observer =>
                apiServices.transactionService.transactionTrees(streamConfig, observer)
              }
          case streamConfig: Config.StreamConfig.ActiveContractsStreamConfig =>
            StreamMetrics
              .observer(
                streamName = streamConfig.name,
                logInterval = config.reportingPeriod,
                metrics = MetricsSet.activeContractsMetrics,
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet.activeContractsExposedMetrics(
                    streamConfig.name,
                    registry,
                    config.reportingPeriod,
                  )
                ),
              )(system, ec)
              .flatMap { observer =>
                apiServices.activeContractsService.getActiveContracts(streamConfig, observer)
              }
          case streamConfig: Config.StreamConfig.CompletionsStreamConfig =>
            StreamMetrics
              .observer(
                streamName = streamConfig.name,
                logInterval = config.reportingPeriod,
                metrics = MetricsSet.completionsMetrics,
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet
                    .completionsExposedMetrics(streamConfig.name, registry, config.reportingPeriod)
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
