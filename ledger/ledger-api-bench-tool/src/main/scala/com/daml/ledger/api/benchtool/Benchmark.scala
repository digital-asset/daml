// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig
import com.daml.ledger.api.benchtool.metrics.{
  MetricRegistryOwner,
  MetricsSet,
  StreamMetrics,
  StreamResult,
}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.util.TypedActorSystemResourceOwner
import com.daml.ledger.resources.ResourceContext
import com.daml.metrics.MetricsReporter
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object Benchmark {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
      streamConfigs: List[StreamConfig],
      reportingPeriod: FiniteDuration,
      apiServices: LedgerApiServices,
      metricsReporter: MetricsReporter,
  )(implicit
      ec: ExecutionContext,
      resourceContext: ResourceContext,
  ): Future[Either[String, Unit]] = {
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
                metrics = MetricsSet.activeContractsMetrics(streamConfig.objectives),
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
                metrics = MetricsSet.completionsMetrics(streamConfig.objectives),
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
        .map { results =>
          if (results.contains(StreamResult.ObjectivesViolated))
            Left("Metrics objectives not met.")
          else Right(())
        }
    }
  }
}
