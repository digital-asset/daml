// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores

import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.daml.api.util.TimeProvider
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.platform.sandbox.stores.ledger.{Ledger, PartyIdGenerator}
import com.daml.telemetry.TelemetryContext
import io.grpc.Status

import scala.compat.java8.FutureConverters

private[stores] final class LedgerBackedWriteService(
    ledger: Ledger,
    timeProvider: TimeProvider,
    enablePruning: Boolean,
)(implicit
    loggingContext: LoggingContext
) extends state.WriteService {

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def submitTransaction(
      submitterInfo: state.SubmitterInfo,
      transactionMeta: state.TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[state.SubmissionResult] =
    withEnrichedLoggingContext(
      "actAs" -> submitterInfo.actAs,
      "applicationId" -> submitterInfo.applicationId,
      "commandId" -> submitterInfo.commandId,
      "deduplicationPeriod" -> submitterInfo.deduplicationPeriod,
      "submissionTime" -> transactionMeta.submissionTime.toInstant,
      "workflowId" -> transactionMeta.workflowId,
      "ledgerTime" -> transactionMeta.ledgerEffectiveTime.toInstant,
    ) { implicit loggingContext =>
      FutureConverters.toJava(
        ledger.publishTransaction(submitterInfo, transactionMeta, transaction)
      )
    }

  override def allocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[state.SubmissionResult] = {
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
      submissionId: Ref.SubmissionId,
      payload: List[DamlLf.Archive],
      sourceDescription: Option[String],
  )(implicit telemetryContext: TelemetryContext): CompletionStage[state.SubmissionResult] =
    withEnrichedLoggingContext(
      "submissionId" -> submissionId,
      "description" -> sourceDescription,
      "packageHashes" -> payload.view.map(_.getHash),
    ) { implicit loggingContext =>
      FutureConverters.toJava(
        ledger.uploadPackages(submissionId, timeProvider.getCurrentTime, sourceDescription, payload)
      )
    }

  // WriteConfigService
  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: Ref.SubmissionId,
      config: Configuration,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[state.SubmissionResult] =
    withEnrichedLoggingContext(
      "maxRecordTime" -> maxRecordTime.toInstant,
      "submissionId" -> submissionId,
      "configGeneration" -> config.generation,
      "configMaxDeduplicationTime" -> config.maxDeduplicationTime,
    ) { implicit loggingContext =>
      FutureConverters.toJava(ledger.publishConfiguration(maxRecordTime, submissionId, config))
    }

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[state.PruningResult] =
    CompletableFuture.completedFuture {
      if (enablePruning) state.PruningResult.ParticipantPruned
      else state.PruningResult.NotPruned(Status.UNIMPLEMENTED)
    }

  override def isApiDeduplicationEnabled: Boolean = true
}
