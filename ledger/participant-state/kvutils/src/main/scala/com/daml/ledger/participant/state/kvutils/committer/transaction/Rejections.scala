// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.codahale.metrics.Counter
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlTransactionRejectionEntry,
  Disputed,
  Inconsistent,
  InvalidLedgerTime,
  PartyNotKnownOnLedger,
  ResourcesExhausted,
  SubmitterCannotActViaParticipant,
}
import com.daml.ledger.participant.state.kvutils.committer.Committer.buildLogEntryWithOptionalRecordTime
import com.daml.ledger.participant.state.kvutils.committer.{StepResult, StepStop}
import com.daml.ledger.participant.state.v1.RejectionReasonV0
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

private[transaction] class Rejections(metrics: Metrics) {

  private final val logger = ContextualizedLogger.get(getClass)

  def reject[A](
      transactionEntry: DamlTransactionEntrySummary,
      rejection: Rejection,
      recordTime: Option[Timestamp],
  )(implicit loggingContext: LoggingContext): StepResult[A] =
    reject(
      buildRejectionEntry(transactionEntry, rejection),
      rejection.description,
      recordTime,
    )

  def reject[A](
      rejectionEntry: DamlTransactionRejectionEntry.Builder,
      rejectionDescription: String,
      recordTime: Option[Timestamp],
  )(implicit loggingContext: LoggingContext): StepResult[A] = {
    Metrics.rejections(rejectionEntry.getReasonCase.getNumber).inc()
    logger.trace(s"Transaction rejected, $rejectionDescription.")
    StepStop(
      buildLogEntryWithOptionalRecordTime(
        recordTime,
        _.setTransactionRejectionEntry(rejectionEntry),
      )
    )
  }

  def buildRejectionEntry(
      transactionEntry: DamlTransactionEntrySummary,
      rejection: Rejection,
  ): DamlTransactionRejectionEntry.Builder = {
    val builder = DamlTransactionRejectionEntry.newBuilder
    builder
      .setSubmitterInfo(transactionEntry.submitterInfo)

    rejection.toStateV1RejectionReason match {
      case RejectionReasonV0.Inconsistent(reason) =>
        builder.setInconsistent(Inconsistent.newBuilder.setDetails(reason))
      case RejectionReasonV0.Disputed(reason) =>
        builder.setDisputed(Disputed.newBuilder.setDetails(reason))
      case RejectionReasonV0.ResourcesExhausted(reason) =>
        builder.setResourcesExhausted(ResourcesExhausted.newBuilder.setDetails(reason))
      case RejectionReasonV0.PartyNotKnownOnLedger(reason) =>
        builder.setPartyNotKnownOnLedger(PartyNotKnownOnLedger.newBuilder.setDetails(reason))
      case RejectionReasonV0.SubmitterCannotActViaParticipant(details) =>
        builder.setSubmitterCannotActViaParticipant(
          SubmitterCannotActViaParticipant.newBuilder.setDetails(details)
        )
      case RejectionReasonV0.InvalidLedgerTime(reason) =>
        builder.setInvalidLedgerTime(InvalidLedgerTime.newBuilder.setDetails(reason))
    }
    builder
  }

  private object Metrics {
    val rejections: Map[Int, Counter] =
      DamlTransactionRejectionEntry.ReasonCase.values
        .map(reasonCase =>
          reasonCase.getNumber -> metrics.daml.kvutils.committer.transaction
            .rejection(reasonCase.name())
        )
        .toMap
  }
}
