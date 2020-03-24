// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.state

import java.util.concurrent.CompletionStage

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.{
  Configuration,
  Party,
  SubmissionId,
  SubmissionResult,
  SubmittedTransaction,
  SubmitterInfo,
  TransactionMeta,
  WriteService
}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.platform.metrics.timedFuture

final class TimedWriteService(delegate: WriteService, metrics: MetricRegistry, prefix: String)
    extends WriteService {
  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction
  ): CompletionStage[SubmissionResult] =
    time(
      "submitTransaction",
      delegate.submitTransaction(submitterInfo, transactionMeta, transaction))

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String]
  ): CompletionStage[SubmissionResult] =
    time("uploadPackages", delegate.uploadPackages(submissionId, archives, sourceDescription))

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId
  ): CompletionStage[SubmissionResult] =
    time("allocateParty", delegate.allocateParty(hint, displayName, submissionId))

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration
  ): CompletionStage[SubmissionResult] =
    time("submitConfiguration", delegate.submitConfiguration(maxRecordTime, submissionId, config))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()

  private def time[T](name: String, future: => CompletionStage[T]): CompletionStage[T] =
    timedFuture(metrics.timer(s"$prefix.$name"), future)
}
