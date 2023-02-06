// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.metrics.{BenchmarkResult, MetricsManager, MetricsSet}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.submission.Names
import com.daml.ledger.api.v1.admin.participant_pruning_service.PruneRequest
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.benchtool.Foo.Dummy
import com.daml.ledger.api.v1.commands.Commands

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class PruningBenchmark(reportingPeriod: FiniteDuration) {
  def benchmarkPruning(
      signatory: Primitive.Party,
      pruningConfig: WorkflowConfig.PruningConfig,
      regularUserServices: LedgerApiServices,
      adminServices: LedgerApiServices,
      actorSystem: ActorSystem[SpawnProtocol.Command],
      names: Names,
  )(implicit ec: ExecutionContext): Future[Either[String, Unit]] = for {
    endOffset <- regularUserServices.transactionService.getLedgerEnd()
    // Submit on more command so that we're not pruning exactly at the ledger end offset
    _ <- adminServices.commandService.submitAndWait(
      Commands(
        applicationId = names.benchtoolApplicationId,
        commandId = "pruning-benchmarking-dummy-command",
        commands = Seq(Dummy(signatory).create.command),
        actAs = Seq(signatory.toString),
      )
    )
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
