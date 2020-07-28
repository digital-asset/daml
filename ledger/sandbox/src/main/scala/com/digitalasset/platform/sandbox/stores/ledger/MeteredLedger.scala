// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import java.time.Instant

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Time
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.index.MeteredReadOnlyLedger

import scala.concurrent.Future

private class MeteredLedger(ledger: Ledger, metrics: Metrics)
    extends MeteredReadOnlyLedger(ledger, metrics)
    with Ledger {

  override def publishTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Future[SubmissionResult] =
    Timed.future(
      metrics.daml.index.publishTransaction,
      ledger.publishTransaction(submitterInfo, transactionMeta, transaction))

  def publishPartyAllocation(
      submissionId: SubmissionId,
      party: Party,
      displayName: Option[String]): Future[SubmissionResult] =
    Timed.future(
      metrics.daml.index.publishPartyAllocation,
      ledger.publishPartyAllocation(submissionId, party, displayName))

  def uploadPackages(
      submissionId: SubmissionId,
      knownSince: Instant,
      sourceDescription: Option[String],
      payload: List[Archive]): Future[SubmissionResult] =
    Timed.future(
      metrics.daml.index.uploadPackages,
      ledger.uploadPackages(submissionId, knownSince, sourceDescription, payload))

  override def publishConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: String,
      config: Configuration): Future[SubmissionResult] =
    Timed.future(
      metrics.daml.index.publishConfiguration,
      ledger.publishConfiguration(maxRecordTime, submissionId, config))

  override def close(): Unit = {
    ledger.close()
  }

}

object MeteredLedger {
  def apply(ledger: Ledger, metrics: Metrics): Ledger = new MeteredLedger(ledger, metrics)
}
