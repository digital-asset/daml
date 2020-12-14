// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores

import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.daml.api.util.TimeProvider
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.v1.{
  Configuration,
  Offset,
  PruningResult,
  SubmissionId,
  SubmissionResult,
  SubmittedTransaction,
  SubmitterInfo,
  TransactionMeta,
  WriteService
}
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Time
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.platform.sandbox.stores.ledger.{Ledger, PartyIdGenerator}
import com.daml.metrics.TelemetryContext

import io.grpc.Status

import scala.compat.java8.FutureConverters

private[stores] final class LedgerBackedWriteService(ledger: Ledger, timeProvider: TimeProvider)(
    implicit loggingContext: LoggingContext,
) extends WriteService {

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    withEnrichedLoggingContext(
      "submitter" -> submitterInfo.singleSubmitterOrThrow(),
      "applicationId" -> submitterInfo.applicationId,
      "commandId" -> submitterInfo.commandId,
      "deduplicateUntil" -> submitterInfo.deduplicateUntil.toString,
      "submissionTime" -> transactionMeta.submissionTime.toInstant.toString,
      "workflowId" -> transactionMeta.workflowId.getOrElse(""),
      "ledgerTime" -> transactionMeta.ledgerEffectiveTime.toInstant.toString,
    ) { implicit loggingContext =>
      FutureConverters.toJava(
        ledger.publishTransaction(submitterInfo, transactionMeta, transaction)
      )
    }

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] = {
    val party = hint.getOrElse(PartyIdGenerator.generateRandomId())
    withEnrichedLoggingContext(
      "party" -> party,
      "submissionId" -> submissionId,
    ) { implicit loggingContext =>
      FutureConverters.toJava(ledger.publishPartyAllocation(submissionId, party, displayName))
    }
  }

  // WritePackagesService
  override def uploadPackages(
      submissionId: SubmissionId,
      payload: List[Archive],
      sourceDescription: Option[String]
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    withEnrichedLoggingContext(
      "submissionId" -> submissionId,
      "description" -> sourceDescription.getOrElse(""),
      "packageHashes" -> payload.iterator.map(_.getHash).mkString(","),
    ) { implicit loggingContext =>
      FutureConverters.toJava(
        ledger
          .uploadPackages(submissionId, timeProvider.getCurrentTime, sourceDescription, payload))
    }

  // WriteConfigService
  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    withEnrichedLoggingContext(
      "maxRecordTime" -> maxRecordTime.toInstant.toString,
      "submissionId" -> submissionId,
      "configGeneration" -> config.generation.toString,
      "configMaxDeduplicationTime" -> config.maxDeduplicationTime.toString,
    ) { implicit loggingContext =>
      FutureConverters.toJava(ledger.publishConfiguration(maxRecordTime, submissionId, config))
    }

  // WriteParticipantPruningService - not supported by sandbox-classic
  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: SubmissionId): CompletionStage[PruningResult] =
    CompletableFuture.completedFuture(PruningResult.NotPruned(Status.UNIMPLEMENTED))
}
