// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.metrics

import java.util.concurrent.CompletionStage

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.metrics.{MetricName, Metrics}
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
import com.daml.lf.data.Time
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus

final class TimedWriteService(delegate: WriteService, metrics: MetricRegistry, prefix: MetricName)
    extends WriteService {
  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction
  ): CompletionStage[SubmissionResult] =
    time(
      "submit_transaction",
      delegate.submitTransaction(submitterInfo, transactionMeta, transaction))

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String]
  ): CompletionStage[SubmissionResult] =
    time("upload_packages", delegate.uploadPackages(submissionId, archives, sourceDescription))

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId
  ): CompletionStage[SubmissionResult] =
    time("allocate_party", delegate.allocateParty(hint, displayName, submissionId))

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration
  ): CompletionStage[SubmissionResult] =
    time("submit_configuration", delegate.submitConfiguration(maxRecordTime, submissionId, config))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()

  private def time[T](name: String, future: => CompletionStage[T]): CompletionStage[T] =
    Metrics.timedCompletionStage(metrics.timer(prefix :+ name), future)
}
