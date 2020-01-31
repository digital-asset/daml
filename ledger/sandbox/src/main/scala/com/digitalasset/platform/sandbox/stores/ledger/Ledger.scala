// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.platform.store.ReadOnlyLedger

import scala.concurrent.Future

trait Ledger extends ReadOnlyLedger {

  def publishHeartbeat(time: Instant): Future[Unit]

  def publishTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction
  ): Future[SubmissionResult]

  // Party management
  def publishPartyAllocation(
      submissionId: SubmissionId,
      party: Party,
      displayName: Option[String]
  ): Future[SubmissionResult]

  // Package management
  def uploadPackages(
      submissionId: SubmissionId,
      knownSince: Instant,
      sourceDescription: Option[String],
      payload: List[Archive]
  ): Future[SubmissionResult]

  // Configuration management
  def publishConfiguration(
      maxRecordTime: Timestamp,
      submissionId: String,
      config: Configuration
  ): Future[SubmissionResult]

}
