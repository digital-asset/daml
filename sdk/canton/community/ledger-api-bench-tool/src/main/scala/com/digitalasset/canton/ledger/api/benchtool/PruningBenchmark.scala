// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool

import cats.syntax.either.*
import com.daml.ledger.api.v2.admin.participant_pruning_service.PruneRequest
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod.Empty
import com.daml.ledger.api.v2.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.infrastructure.TestDars
import com.digitalasset.canton.ledger.api.benchtool.metrics.{
  BenchmarkResult,
  MetricsManager,
  MetricsSet,
}
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.{FooTemplateDescriptor, Names}
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.actor.typed.{ActorSystem, SpawnProtocol}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class PruningBenchmark(reportingPeriod: FiniteDuration) {

  private val packageRef: Ref.PackageRef = TestDars.benchtoolDarPackageRef

  def benchmarkPruning(
      signatory: Party,
      pruningConfig: WorkflowConfig.PruningConfig,
      regularUserServices: LedgerApiServices,
      adminServices: LedgerApiServices,
      actorSystem: ActorSystem[SpawnProtocol.Command],
      names: Names,
  )(implicit ec: ExecutionContext): Future[Either[String, Unit]] = for {
    endOffset <- regularUserServices.stateService.getLedgerEnd()
    // Submit one more command so that we're not pruning exactly at the ledger end offset
    _ <- adminServices.commandService.submitAndWait(
      Commands(
        workflowId = "",
        userId = names.benchtoolUserId,
        commandId = "pruning-benchmarking-dummy-command",
        commands = Seq(makeCreateDummyCommand(signatory)),
        deduplicationPeriod = Empty,
        minLedgerTimeAbs = None,
        minLedgerTimeRel = None,
        actAs = Seq(signatory.getValue),
        readAs = Nil,
        submissionId = "",
        disclosedContracts = Nil,
        synchronizerId = "",
        packageIdSelectionPreference = Nil,
        prefetchContractKeys = Nil,
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
          case BenchmarkResult.Ok => Either.unit
        }
      }
  } yield Either.unit

  private def makeCreateDummyCommand(
      signatory: Party
  ) = {
    val createArguments: Option[Record] = Some(
      Record(
        None,
        Seq(
          RecordField(
            label = "signatory",
            value = Some(Value(Value.Sum.Party(signatory.getValue))),
          )
        ),
      )
    )
    val c: Command = Command(
      command = Command.Command.Create(
        CreateCommand(
          templateId = Some(FooTemplateDescriptor.dummyTemplateId(packageId = packageRef.toString)),
          createArguments = createArguments,
        )
      )
    )
    c
  }

}
