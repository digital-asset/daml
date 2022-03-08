// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig
import com.daml.ledger.api.benchtool.metrics.{MetricsSet, StreamMetrics, StreamResult}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.util.TypedActorSystemResourceOwner
import com.daml.ledger.resources.ResourceContext
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object Benchmark {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
      streamConfigs: List[StreamConfig],
      reportingPeriod: FiniteDuration,
      apiServices: LedgerApiServices,
  )(implicit
      ec: ExecutionContext,
      resourceContext: ResourceContext,
  ): Future[Either[String, Unit]] = {
    val resources = for {
      system <- TypedActorSystemResourceOwner.owner()
    } yield system

    // TODO Prometheus metrics: implement exposed metrics
    resources.use { case system =>
      Future
        .traverse(streamConfigs) {
          case streamConfig: StreamConfig.TransactionsStreamConfig =>
            StreamMetrics
              .observer(
                streamName = streamConfig.name,
                logInterval = reportingPeriod,
                metrics = MetricsSet.transactionMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = None,
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
                exposedMetrics = None,
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
                exposedMetrics = None,
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
                exposedMetrics = None,
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
