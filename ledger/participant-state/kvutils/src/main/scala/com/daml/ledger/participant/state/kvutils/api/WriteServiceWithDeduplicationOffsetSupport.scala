// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.util.concurrent.CompletionStage

import akka.stream.Materializer
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.domain.ApplicationId
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.deduplication.DeduplicationPeriodService
import com.daml.ledger.participant.state.v2.{
  PruningResult,
  SubmissionResult,
  SubmitterInfo,
  TransactionMeta,
  WriteService,
}
import com.daml.lf.data.Ref.{Party, SubmissionId}
import com.daml.lf.data.Time
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.telemetry.TelemetryContext

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters._

class WriteServiceWithDeduplicationOffsetSupport(
    delegate: WriteService,
    deduplicationPeriodService: DeduplicationPeriodService,
)(implicit mat: Materializer, ec: ExecutionContext)
    extends WriteService {

  private val logger = ContextualizedLogger.get(getClass)

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] = {
    implicit val contextualizedLogger: DamlContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, submitterInfo.submissionId)
    deduplicationPeriodService
      .supportedDeduplicationPeriod(
        submitterInfo.deduplicationPeriod,
        submitterInfo.ledgerConfiguration.maxDeduplicationTime,
        ApplicationId(submitterInfo.applicationId),
        submitterInfo.actAs.toSet,
        transactionMeta.submissionTime.toInstant,
      )
      .flatMap(supportedDeduplicationPeriod => {
        val submitterInfoWithSupportedDeduplicationPeriod =
          submitterInfo.copy(deduplicationPeriod = supportedDeduplicationPeriod)
        delegate
          .submitTransaction(
            submitterInfoWithSupportedDeduplicationPeriod,
            transactionMeta,
            transaction,
            estimatedInterpretationCost,
          )
          .asScala
      })
      .asJava

  }

  override def currentHealth(): HealthStatus = delegate.currentHealth()

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    delegate.uploadPackages(submissionId, archives, sourceDescription)

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult] =
    delegate.prune(pruneUpToInclusive, submissionId, pruneAllDivulgedContracts)

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    delegate.allocateParty(hint, displayName, submissionId)

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    delegate.submitConfiguration(maxRecordTime, submissionId, config)
}
