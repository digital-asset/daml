// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.util.UUID
import java.util.concurrent.CompletionStage

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueSubmission}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.health.HealthStatus

import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext

class KeyValueParticipantStateWriter(writer: LedgerWriter)(
    implicit executionContext: ExecutionContext)
    extends WriteService {

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
  ): CompletionStage[SubmissionResult] = {
    val submission =
      KeyValueSubmission.transactionToSubmission(
        submitterInfo,
        transactionMeta,
        transaction.assertNoRelCid(cid => s"Unexpected relative contract id: $cid"),
      )
    val correlationId = nextSubmissionId()
    commit(correlationId, submission)
  }

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String]): CompletionStage[SubmissionResult] = {
    val submission = KeyValueSubmission
      .archivesToSubmission(
        submissionId,
        archives,
        sourceDescription.getOrElse(""),
        writer.participantId)
    commit(submissionId, submission)
  }

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration): CompletionStage[SubmissionResult] = {
    val submission =
      KeyValueSubmission
        .configurationToSubmission(maxRecordTime, submissionId, writer.participantId, config)
    commit(submissionId, submission)
  }

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId): CompletionStage[SubmissionResult] = {
    val party = hint.getOrElse(generateRandomParty())
    val submission =
      KeyValueSubmission.partyToSubmission(
        submissionId,
        Some(party),
        displayName,
        writer.participantId)
    commit(submissionId, submission)
  }

  override def currentHealth(): HealthStatus = writer.currentHealth()

  private def generateRandomParty(): Ref.Party =
    Ref.Party.assertFromString(s"party-${UUID.randomUUID().toString.take(8)}")

  private def nextSubmissionId(): String = UUID.randomUUID().toString

  private def commit(
      correlationId: String,
      submission: DamlSubmission): CompletionStage[SubmissionResult] =
    FutureConverters.toJava(writer.commit(correlationId, Envelope.enclose(submission).toByteArray))
}
