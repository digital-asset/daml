// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import java.time.Instant

import com.codahale.metrics.Counter
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlTransactionRejectionEntry._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlTransactionRejectionEntry,
  InvalidLedgerTime,
  SubmitterCannotActViaParticipant,
}
import com.daml.ledger.participant.state.kvutils.committer.Committer.buildLogEntryWithOptionalRecordTime
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection.{
  ExternallyInconsistentTransaction,
  InternallyInconsistentTransaction,
}
import com.daml.ledger.participant.state.kvutils.committer.{StepResult, StepStop}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

import scala.jdk.CollectionConverters._

private[transaction] class Rejections(metrics: Metrics) {

  final private val logger = ContextualizedLogger.get(getClass)

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

  def preExecutionOutOfTimeBoundsRejectionEntry(
      transactionEntry: DamlTransactionEntrySummary,
      minimumRecordTime: Instant,
      maximumRecordTime: Instant,
  ): DamlTransactionRejectionEntry =
    buildRejectionEntry(
      transactionEntry,
      Rejection.RecordTimeOutOfRange(minimumRecordTime, maximumRecordTime),
    ).build

  private def buildRejectionEntry(
      transactionEntry: DamlTransactionEntrySummary,
      rejection: Rejection,
  ): DamlTransactionRejectionEntry.Builder = {
    val builder = DamlTransactionRejectionEntry.newBuilder
    builder
      .setSubmitterInfo(transactionEntry.submitterInfo)

    rejection match {
      case Rejection.ValidationFailure(error) =>
        builder.setValidationFailure(
          DamlTransactionRejectionEntry.ValidationFailure.newBuilder().setDetails(error.message)
        )
      case InternallyInconsistentTransaction.DuplicateKeys =>
        builder.setInternalDuplicateKeys(DuplicateKeys.newBuilder())
      case InternallyInconsistentTransaction.InconsistentKeys =>
        builder.setInternalInconsistentKeys(InconsistentKeys.newBuilder())
      case ExternallyInconsistentTransaction.InconsistentContracts =>
        builder.setExternalInconsistentContracts(
          InconsistentContracts.newBuilder()
        )
      case ExternallyInconsistentTransaction.DuplicateKeys =>
        builder.setExternalDuplicateKeys(DuplicateKeys.newBuilder())
      case ExternallyInconsistentTransaction.InconsistentKeys =>
        builder.setExternalInconsistentKeys(InconsistentKeys.newBuilder())
      case Rejection.MissingInputState(key) =>
        builder.setMissingInputState(
          MissingInputState.newBuilder().setKey(key)
        )
      case Rejection.InvalidParticipantState(error) =>
        builder.setInvalidParticipantState(
          InvalidParticipantState
            .newBuilder()
            .setDetails(
              error.getMessage
            )
        )
      case Rejection.LedgerTimeOutOfRange(outOfRange) =>
        builder.setInvalidLedgerTime(
          InvalidLedgerTime
            .newBuilder(
            )
            .setDetails(outOfRange.message)
            .setLedgerTime(buildTimestamp(outOfRange.ledgerTime))
            .setLowerBound(buildTimestamp(outOfRange.lowerBound))
            .setUpperBound(buildTimestamp(outOfRange.upperBound))
        )
      case Rejection.RecordTimeOutOfRange(minimumRecordTime, maximumRecordTime) =>
        builder.setRecordTimeOutOfRange(
          RecordTimeOutOfRange
            .newBuilder()
            .setMaximumRecordTime(buildTimestamp(maximumRecordTime))
            .setMinimumRecordTime(buildTimestamp(minimumRecordTime))
        )
      case Rejection.CausalMonotonicityViolated =>
        builder.setCausalMonotonicityViolated(
          CausalMonotonicityViolated.newBuilder()
        )
      case Rejection.SubmittingPartyNotKnownOnLedger(submitter) =>
        builder.setSubmittingPartyNotKnownOnLedger(
          SubmittingPartyNotKnownOnLedger
            .newBuilder()
            .setSubmitterParty(submitter)
        )
      case Rejection.PartiesNotKnownOnLedger(parties) =>
        val stringParties: Iterable[String] = parties
        builder.setPartiesNotKnownOnLedger(
          PartiesNotKnownOnLedger
            .newBuilder()
            .addAllParties(stringParties.asJava)
        )
      case rejection @ Rejection.SubmitterCannotActViaParticipant(submitter, participantId) =>
        builder.setSubmitterCannotActViaParticipant(
          SubmitterCannotActViaParticipant
            .newBuilder()
            .setSubmitterParty(submitter)
            .setParticipantId(participantId)
            .setDetails(rejection.description)
        )
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
