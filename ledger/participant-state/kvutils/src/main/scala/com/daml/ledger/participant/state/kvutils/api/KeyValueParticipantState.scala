// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.{Configuration, LedgerInitialConditions}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{
  PruningResult,
  ReadService,
  SubmissionResult,
  SubmitterInfo,
  TransactionMeta,
  Update,
  WriteService,
}
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.logging.LoggingContext
import com.daml.telemetry.TelemetryContext

/** Implements read and write operations required for running a participant server.
  *
  * Unifies [[KeyValueParticipantStateReader]] and [[KeyValueParticipantStateWriter]] interfaces
  * Will report [[com.daml.ledger.api.health.Healthy]] as health status only if both
  * `reader` and `writer` are healthy.
  *
  * @param reader       [[LedgerReader]] instance to delegate to
  * @param writer       [[LedgerWriter]] instance to delegate to
  */
class KeyValueParticipantState(
    reader: KeyValueParticipantStateReader,
    writer: KeyValueParticipantStateWriter,
) extends ReadService
    with WriteService {

  override def isApiDeduplicationEnabled: Boolean = writer.isApiDeduplicationEnabled

  override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    reader.ledgerInitialConditions()

  override def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit loggingContext: LoggingContext): Source[(Offset, Update), NotUsed] =
    reader.stateUpdates(beginAfter)

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    writer.submitTransaction(
      submitterInfo,
      transactionMeta,
      transaction,
      estimatedInterpretationCost,
    )

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: Ref.SubmissionId,
      config: Configuration,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    writer.submitConfiguration(maxRecordTime, submissionId, config)

  override def uploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    writer.uploadPackages(submissionId, archives, sourceDescription)

  override def allocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    writer.allocateParty(hint, displayName, submissionId)

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult] =
    writer.prune(pruneUpToInclusive, submissionId, pruneAllDivulgedContracts)

  override def currentHealth(): HealthStatus =
    reader.currentHealth() and writer.currentHealth()
}
