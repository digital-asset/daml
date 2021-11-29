// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.{Configuration, LedgerInitialConditions}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.deduplication.{
  DeduplicationPeriodConverter,
  DeduplicationPeriodSupport,
}
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
import com.daml.metrics.Metrics
import com.daml.platform.server.api.validation.{DeduplicationPeriodValidator, ErrorFactories}
import com.daml.telemetry.TelemetryContext

import scala.concurrent.ExecutionContext

/** Implements read and write operations required for running a participant server.
  *
  * Adapts [[LedgerReader]] and [[LedgerWriter]] interfaces to [[com.daml.ledger.participant.state.v2.ReadService]] and
  * [[com.daml.ledger.participant.state.v2.WriteService]], respectively.
  * Will report [[com.daml.ledger.api.health.Healthy]] as health status only if both
  * `reader` and `writer` are healthy.
  *
  * @param reader                       [[LedgerReader]] instance to adapt
  * @param writer                       [[LedgerWriter]] instance to adapt
  * @param metrics                      used to record timing metrics for [[LedgerWriter]] calls
  * @param deduplicationPeriodConverter used to add support for [[com.daml.ledger.api.DeduplicationPeriod.DeduplicationOffset]]
  */
class KeyValueParticipantState(
    reader: LedgerReader,
    writer: LedgerWriter,
    metrics: Metrics,
    enableSelfServiceErrorCodes: Boolean,
    deduplicationPeriodConverter: DeduplicationPeriodConverter,
)(implicit mat: Materializer, ec: ExecutionContext)
    extends ReadService
    with WriteService {
  private val errorFactories = ErrorFactories(
    enableSelfServiceErrorCodes
  )
  private val readerAdapter =
    KeyValueParticipantStateReader(reader, metrics, enableSelfServiceErrorCodes)
  private val writerAdapter = {
    new WriteServiceWithDeduplicationSupport(
      new KeyValueParticipantStateWriter(
        new TimedLedgerWriter(writer, metrics),
        metrics,
      ),
      new DeduplicationPeriodSupport(
        deduplicationPeriodConverter,
        new DeduplicationPeriodValidator(errorFactories),
        errorFactories,
      ),
    )
  }

  override def isApiDeduplicationEnabled: Boolean = writerAdapter.isApiDeduplicationEnabled

  override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    readerAdapter.ledgerInitialConditions()

  override def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit loggingContext: LoggingContext): Source[(Offset, Update), NotUsed] =
    readerAdapter.stateUpdates(beginAfter)

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    writerAdapter.submitTransaction(
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
    writerAdapter.submitConfiguration(maxRecordTime, submissionId, config)

  override def uploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    writerAdapter.uploadPackages(submissionId, archives, sourceDescription)

  override def allocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    writerAdapter.allocateParty(hint, displayName, submissionId)

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult] =
    writerAdapter.prune(pruneUpToInclusive, submissionId, pruneAllDivulgedContracts)

  override def currentHealth(): HealthStatus =
    reader.currentHealth() and writer.currentHealth()
}
