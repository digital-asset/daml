// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.metrics

import java.util.concurrent.CompletionStage

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Time
import com.daml.metrics.{Metrics, Timed}

final class TimedWriteService(delegate: WriteService, metrics: Metrics) extends WriteService {

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction
  ): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.daml.services.write.submitTransaction,
      delegate.submitTransaction(submitterInfo, transactionMeta, transaction))

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String]
  ): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.daml.services.write.uploadPackages,
      delegate.uploadPackages(submissionId, archives, sourceDescription))

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId
  ): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.daml.services.write.allocateParty,
      delegate.allocateParty(hint, displayName, submissionId))

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration
  ): CompletionStage[SubmissionResult] =
    Timed.completionStage(
      metrics.daml.services.write.submitConfiguration,
      delegate.submitConfiguration(maxRecordTime, submissionId, config))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}
