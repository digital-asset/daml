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

class KeyValueParticipantState(reader: ReadService, writer: WriteService)
    extends ReadService
    with WriteService {
  private val readerAdapter = reader
  private val writerAdapter = writer

  def this(reader: LedgerReader, writer: LedgerWriter)(implicit materializer: Materializer) {
    this(
      new KeyValueParticipantStateReader(reader),
      new KeyValueParticipantStateWriter(writer)(materializer.executionContext))
  }

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
