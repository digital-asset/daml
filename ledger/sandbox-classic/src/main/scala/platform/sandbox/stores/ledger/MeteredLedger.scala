// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.index.MeteredReadOnlyLedger

import scala.concurrent.Future

private class MeteredLedger(ledger: Ledger, metrics: Metrics)
    extends MeteredReadOnlyLedger(ledger, metrics)
    with Ledger {

  override def publishTransaction(
      submitterInfo: state.SubmitterInfo,
      transactionMeta: state.TransactionMeta,
      transaction: SubmittedTransaction,
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] =
    Timed.future(
      metrics.daml.index.publishTransaction,
      ledger.publishTransaction(submitterInfo, transactionMeta, transaction),
    )

  def publishPartyAllocation(
      submissionId: Ref.SubmissionId,
      party: Party,
      displayName: Option[String],
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] =
    Timed.future(
      metrics.daml.index.publishPartyAllocation,
      ledger.publishPartyAllocation(submissionId, party, displayName),
    )

  def uploadPackages(
      submissionId: Ref.SubmissionId,
      knownSince: Time.Timestamp,
      sourceDescription: Option[String],
      payload: List[Archive],
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] =
    Timed.future(
      metrics.daml.index.uploadPackages,
      ledger.uploadPackages(submissionId, knownSince, sourceDescription, payload),
    )

  override def publishConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: String,
      config: Configuration,
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] =
    Timed.future(
      metrics.daml.index.publishConfiguration,
      ledger.publishConfiguration(maxRecordTime, submissionId, config),
    )

  override def close(): Unit = {
    ledger.close()
  }

}

private[sandbox] object MeteredLedger {
  def apply(ledger: Ledger, metrics: Metrics): Ledger = new MeteredLedger(ledger, metrics)
}
