// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.metrics

import java.util.concurrent.CompletionStage

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Time
import com.daml.metrics.{Metrics, Timed, TelemetryContext}

final class TimedWriteService(delegate: WriteService, metrics: Metrics) extends WriteService {

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    Timed.timedAndTrackedCompletionStage(
      metrics.daml.services.write.submitTransaction,
      metrics.daml.services.write.submitTransactionRunning,
      delegate.submitTransaction(
        submitterInfo,
        transactionMeta,
        transaction,
        estimatedInterpretationCost,
      ),
    )

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String]
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.daml.services.write.uploadPackages,
      delegate.uploadPackages(submissionId, archives, sourceDescription))

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.daml.services.write.allocateParty,
      delegate.allocateParty(hint, displayName, submissionId))

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.daml.services.write.submitConfiguration,
      delegate.submitConfiguration(maxRecordTime, submissionId, config))

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: SubmissionId): CompletionStage[PruningResult] =
    Timed.completionStage(
      metrics.daml.services.write.prune,
      delegate.prune(pruneUpToInclusive, submissionId))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}
