// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy, Unhealthy}

class KeyValueParticipantState(reader: LedgerReader, writer: LedgerWriter)(
    implicit materializer: Materializer)
    extends ReadService
    with WriteService
    with AutoCloseable {
  private val readerAdaptor = new KeyValueParticipantStateReader(reader)
  private val writerAdaptor =
    new KeyValueParticipantStateWriter(writer)(materializer.executionContext)

  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    readerAdaptor.getLedgerInitialConditions()

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    readerAdaptor.stateUpdates(beginAfter)

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): CompletionStage[SubmissionResult] =
    writerAdaptor.submitTransaction(submitterInfo, transactionMeta, transaction)

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration): CompletionStage[SubmissionResult] =
    writerAdaptor.submitConfiguration(maxRecordTime, submissionId, config)

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String]): CompletionStage[SubmissionResult] =
    writerAdaptor.uploadPackages(submissionId, archives, sourceDescription)

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId): CompletionStage[SubmissionResult] =
    writerAdaptor.allocateParty(hint, displayName, submissionId)

  override def currentHealth(): HealthStatus =
    if (Seq(reader.checkHealth(), writer.checkHealth()).forall(_ == Healthy)) {
      Healthy
    } else {
      Unhealthy
    }

  override def close(): Unit = writerAdaptor.close()
}
