// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.platform.index.MeteredReadOnlyLedger
import com.digitalasset.platform.metrics.timedFuture

import scala.concurrent.Future

private class MeteredLedger(ledger: Ledger, metrics: MetricRegistry)
    extends MeteredReadOnlyLedger(ledger, metrics)
    with Ledger {

  private object Metrics {
    val publishHeartbeat: Timer = metrics.timer("daml.index.publish_heartbeat")
    val publishTransaction: Timer = metrics.timer("daml.index.publish_transaction")
    val publishPartyAllocation: Timer = metrics.timer("daml.index.publish_party_allocation")
    val uploadPackages: Timer = metrics.timer("daml.index.upload_packages")
    val publishConfiguration: Timer = metrics.timer("daml.index.publish_configuration")
  }

  override def publishHeartbeat(time: Instant): Future[Unit] =
    timedFuture(Metrics.publishHeartbeat, ledger.publishHeartbeat(time))

  override def publishTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Future[SubmissionResult] =
    timedFuture(
      Metrics.publishTransaction,
      ledger.publishTransaction(submitterInfo, transactionMeta, transaction))

  def publishPartyAllocation(
      submissionId: SubmissionId,
      party: Party,
      displayName: Option[String]): Future[SubmissionResult] =
    timedFuture(
      Metrics.publishPartyAllocation,
      ledger.publishPartyAllocation(submissionId, party, displayName))

  def uploadPackages(
      submissionId: SubmissionId,
      knownSince: Instant,
      sourceDescription: Option[String],
      payload: List[Archive]): Future[SubmissionResult] =
    timedFuture(
      Metrics.uploadPackages,
      ledger.uploadPackages(submissionId, knownSince, sourceDescription, payload))

  override def publishConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: String,
      config: Configuration): Future[SubmissionResult] =
    timedFuture(
      Metrics.publishConfiguration,
      ledger.publishConfiguration(maxRecordTime, submissionId, config))

  override def close(): Unit = {
    ledger.close()
  }

}

object MeteredLedger {
  def apply(ledger: Ledger, metrics: MetricRegistry): Ledger = new MeteredLedger(ledger, metrics)
}
