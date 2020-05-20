// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.util.UUID
import java.util.concurrent.CompletionStage

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueSubmission}
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.{Ref, Time}
import com.daml.metrics.Metrics

import scala.compat.java8.FutureConverters

class KeyValueParticipantStateWriter(writer: LedgerWriter, metrics: Metrics) extends WriteService {

  private val keyValueSubmission = new KeyValueSubmission(metrics)

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
  ): CompletionStage[SubmissionResult] = {
    val submission =
      keyValueSubmission.transactionToSubmission(
        submitterInfo,
        transactionMeta,
        transaction.assertNoRelCid(cid => s"Unexpected relative contract id: $cid"),
      )
    commit(correlationId = submitterInfo.commandId, submission = submission)
  }

  override def uploadPackages(
      submissionId: SubmissionId,
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String]): CompletionStage[SubmissionResult] = {
    val submission = keyValueSubmission
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
      keyValueSubmission
        .configurationToSubmission(maxRecordTime, submissionId, writer.participantId, config)
    commit(submissionId, submission)
  }

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId): CompletionStage[SubmissionResult] = {
    val party = hint.getOrElse(generateRandomParty())
    val submission =
      keyValueSubmission.partyToSubmission(
        submissionId,
        Some(party),
        displayName,
        writer.participantId)
    commit(submissionId, submission)
  }

  override def currentHealth(): HealthStatus = writer.currentHealth()

  private def generateRandomParty(): Ref.Party =
    Ref.Party.assertFromString(s"party-${UUID.randomUUID().toString.take(8)}")

  private def commit(
      correlationId: String,
      submission: DamlSubmission): CompletionStage[SubmissionResult] =
    FutureConverters.toJava(writer.commit(correlationId, Envelope.enclose(submission)))
}
