// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.health.HealthStatus

/**
  * Implements read and write operations required for running a participant server.
  *
  * Adapts [[LedgerReader]] and [[LedgerWriter]] interfaces to [[ReadService]] and [[WriteService]],
  * respectively.
  * Will report [[com.digitalasset.ledger.api.health.Healthy]] as health status only if both
  * `reader` and `writer` are healthy.
  *
  * @param reader  [[LedgerReader]] instance to adapt
  * @param writer  [[LedgerWriter]] instance to adapt
  * @param materializer materializer to use when streaming updates from `reader`
  */
class KeyValueParticipantState(reader: LedgerReader, writer: LedgerWriter)(
    implicit materializer: Materializer)
    extends ReadService
    with WriteService {
  private val readerAdapter =
    new KeyValueParticipantStateReader(reader)
  private val writerAdapter =
    new KeyValueParticipantStateWriter(writer)(materializer.executionContext)

  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    readerAdapter.getLedgerInitialConditions()

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    readerAdapter.stateUpdates(beginAfter)

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): CompletionStage[SubmissionResult] =
    writerAdapter.submitTransaction(submitterInfo, transactionMeta, transaction)

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration): CompletionStage[SubmissionResult] =
    writerAdapter.submitConfiguration(maxRecordTime, submissionId, config)

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String]): CompletionStage[SubmissionResult] =
    writerAdapter.uploadPackages(submissionId, archives, sourceDescription)

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId): CompletionStage[SubmissionResult] =
    writerAdapter.allocateParty(hint, displayName, submissionId)

  override def currentHealth(): HealthStatus =
    reader.currentHealth() and writer.currentHealth()
}
