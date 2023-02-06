// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.metrics.{BenchmarkResult, MetricsManager, MetricsSet}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.v1.admin.participant_pruning_service.PruneRequest

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class PruningBenchmark(reportingPeriod: FiniteDuration) {
  def benchmarkPruning(
      pruningConfig: WorkflowConfig.PruningConfig,
      regularUserServices: LedgerApiServices,
      adminServices: LedgerApiServices,
      actorSystem: ActorSystem[SpawnProtocol.Command],
  )(implicit ec: ExecutionContext): Future[Either[String, Unit]] = for {
    endOffset <- regularUserServices.transactionService.getLedgerEnd()
    durationMetric = MetricsSet.createTotalRuntimeMetric[Unit](pruningConfig.maxDurationObjective)
    metricsManager <- MetricsManager.create(
      observedMetric = "benchtool-pruning",
      logInterval = reportingPeriod,
      metrics = List(durationMetric),
      exposedMetrics = None,
    )(actorSystem, ec)
    _ <- adminServices.pruningService
      .prune(
        new PruneRequest(
          pruneUpTo = endOffset,
          submissionId = "benchtool-pruning",
          pruneAllDivulgedContracts = pruningConfig.pruneAllDivulgedContracts,
        )
      )
      .map { _ =>
        metricsManager.sendNewValue(())
        metricsManager.result().map {
          case BenchmarkResult.ObjectivesViolated => Left("Metrics objectives not met.")
          case BenchmarkResult.Ok => Right(())
        }
      }
  } yield Right(())

}
