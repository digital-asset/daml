// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.updates

import com.daml.error.definitions.LedgerApiErrors

import java.time.Instant
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.participant.state.kvutils.Conversions.parseCompletionInfo
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection.{
  ExternallyInconsistentTransaction,
  InternallyInconsistentTransaction,
}
import com.daml.ledger.participant.state.kvutils.errors.KVErrors
import com.daml.ledger.participant.state.kvutils.store.events._
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.lf.data.Time.Timestamp
import com.google.rpc.status.Status

import scala.jdk.CollectionConverters._

/** Utilities for converting between rejection log entries and updates and/or gRPC statuses.
  */
private[kvutils] object TransactionRejections {

  def invalidRecordTimeRejectionUpdate(
      recordTime: Timestamp,
      tooEarlyUntil: Option[Timestamp],
      tooLateFrom: Option[Timestamp],
      rejectionEntry: DamlTransactionRejectionEntry,
  )(implicit loggingContext: ContextualizedErrorLogger): Update.CommandRejected = {
    Update.CommandRejected(
      recordTime = recordTime,
      completionInfo = parseCompletionInfo(
        recordTime,
        rejectionEntry.getSubmitterInfo,
      ),
      reasonTemplate = FinalReason(
        KVErrors.Time.InvalidRecordTime
          .Reject(
            rejectionEntry.getDefiniteAnswer,
            invalidRecordTimeReason(recordTime, tooEarlyUntil, tooLateFrom),
            recordTime.toInstant,
            tooEarlyUntil.map(_.toInstant),
            tooLateFrom.map(_.toInstant),
          )
          .asStatus
      ),
    )
  }

  def duplicateCommandsRejectionUpdate(
      recordTime: Timestamp,
      rejectionEntry: DamlTransactionRejectionEntry,
      existingCommandSubmissionId: Option[String],
  )(implicit loggingContext: ContextualizedErrorLogger): Update.CommandRejected = {
    val definiteAnswer = rejectionEntry.getDefiniteAnswer
    Update.CommandRejected(
      recordTime = recordTime,
      completionInfo = parseCompletionInfo(
        recordTime,
        rejectionEntry.getSubmitterInfo,
      ),
      reasonTemplate = FinalReason(
        duplicateCommandsRejectionStatus(definiteAnswer, existingCommandSubmissionId)
      ),
    )
  }

  def rejectionReasonNotSetStatus()(implicit loggingContext: ContextualizedErrorLogger): Status =
    KVErrors.Internal.RejectionReasonNotSet
      .Reject()
      .asStatus

  def invalidParticipantStateStatus(
      rejection: InvalidParticipantState
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    val metadata = rejection.getMetadataMap.asScala.toMap
    KVErrors.Internal.InvalidParticipantState
      .Reject(details, metadata)
      .asStatus
  }

  def partiesNotKnownOnLedgerStatus(
      rejection: PartiesNotKnownOnLedger
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val parties = rejection.getPartiesList
    LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
      .Reject(parties.asScala.toSet)
      .rpcStatus()
  }

  def submittingPartyNotKnownOnLedgerStatus(
      rejection: SubmittingPartyNotKnownOnLedger
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val submitter = rejection.getSubmitterParty
    LedgerApiErrors.WriteServiceRejections.SubmittingPartyNotKnownOnLedger
      .Reject(submitter)
      .rpcStatus()
  }

  def causalMonotonicityViolatedStatus()(implicit
      loggingContext: ContextualizedErrorLogger
  ): Status =
    KVErrors.Time.CausalMonotonicityViolated
      .Reject()
      .asStatus

  def recordTimeOutOfRangeStatus(
      rejection: RecordTimeOutOfRange
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val minRecordTime = Instant
      .ofEpochSecond(
        rejection.getMinimumRecordTime.getSeconds,
        rejection.getMinimumRecordTime.getNanos.toLong,
      )
    val maxRecordTime = Instant
      .ofEpochSecond(
        rejection.getMaximumRecordTime.getSeconds,
        rejection.getMaximumRecordTime.getNanos.toLong,
      )
    KVErrors.Time.RecordTimeOutOfRange
      .Reject(minRecordTime, maxRecordTime)
      .asStatus
  }

  def externallyDuplicateKeysStatus()(implicit loggingContext: ContextualizedErrorLogger): Status =
    LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
      .Reject(ExternallyInconsistentTransaction.DuplicateKeys.description)
      .rpcStatus()

  def externallyInconsistentKeysStatus()(implicit
      loggingContext: ContextualizedErrorLogger
  ): Status =
    LedgerApiErrors.ConsistencyErrors.InconsistentContractKey
      .Reject(ExternallyInconsistentTransaction.InconsistentKeys.description)
      .rpcStatus()

  def externallyInconsistentContractsStatus()(implicit
      loggingContext: ContextualizedErrorLogger
  ): Status =
    LedgerApiErrors.ConsistencyErrors.InconsistentContracts
      .Reject(ExternallyInconsistentTransaction.InconsistentContracts.description)
      .rpcStatus()

  def missingInputStateStatus(
      rejection: MissingInputState
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val key = rejection.getKey.toString
    KVErrors.Internal.MissingInputState
      .Reject(key)
      .asStatus
  }

  def internallyInconsistentKeysStatus(
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    LedgerApiErrors.WriteServiceRejections.Internal.InternallyInconsistentKeys
      .Reject(InternallyInconsistentTransaction.InconsistentKeys.description)
      .rpcStatus()

  def internallyDuplicateKeysStatus(
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    LedgerApiErrors.WriteServiceRejections.Internal.InternallyDuplicateKeys
      .Reject(InternallyInconsistentTransaction.DuplicateKeys.description)
      .rpcStatus()

  def validationFailureStatus(
      rejection: ValidationFailure
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    KVErrors.Consistency.ValidationFailure
      .Reject(details)
      .asStatus
  }

  def duplicateCommandStatus(
      entry: DamlTransactionRejectionEntry
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val rejectionReason = entry.getDuplicateCommand
    duplicateCommandsRejectionStatus(existingCommandSubmissionId =
      Some(rejectionReason.getSubmissionId).filter(_.nonEmpty)
    )
  }

  def submitterCannotActViaParticipantStatus(
      rejection: SubmitterCannotActViaParticipant
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val submitter = rejection.getSubmitterParty
    val participantId = rejection.getParticipantId
    val details = rejection.getDetails
    LedgerApiErrors.WriteServiceRejections.SubmitterCannotActViaParticipant
      .RejectWithSubmitterAndParticipantId(
        details,
        submitter,
        participantId,
      )
      .rpcStatus()
  }

  def invalidLedgerTimeStatus(
      rejection: InvalidLedgerTime
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    val ledgerTime = rejection.getLedgerTime
    val ledgerTimeLowerBound = rejection.getLowerBound
    val ledgerTimeUpperBound = rejection.getUpperBound

    LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
      .RejectEnriched(
        cause = details,
        ledgerTime = Instant
          .ofEpochSecond(
            ledgerTime.getSeconds,
            ledgerTime.getNanos.toLong,
          ),
        ledgerTimeLowerBound = Instant
          .ofEpochSecond(
            ledgerTimeLowerBound.getSeconds,
            ledgerTimeLowerBound.getNanos.toLong,
          ),
        ledgerTimeUpperBound = Instant
          .ofEpochSecond(
            ledgerTimeUpperBound.getSeconds,
            ledgerTimeUpperBound.getNanos.toLong,
          ),
      )
      .rpcStatus()
  }

  def resourceExhaustedStatus(
      rejection: ResourcesExhausted
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    KVErrors.Resources.ResourceExhausted
      .Reject(details)
      .asStatus
  }

  @deprecated
  def partyNotKnownOnLedgerStatus(
      rejection: PartyNotKnownOnLedger
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails

    LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
      .RejectDeprecated(details)
      .rpcStatus()
  }

  @deprecated
  def inconsistentStatus(
      rejection: Inconsistent
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    LedgerApiErrors.ConsistencyErrors.Inconsistent
      .Reject(details)
      .rpcStatus()
  }

  @deprecated
  def disputedStatus(
      rejection: Disputed
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    LedgerApiErrors.WriteServiceRejections.Disputed
      .Reject(details)
      .rpcStatus()
  }

  private def duplicateCommandsRejectionStatus(
      definiteAnswer: Boolean = false,
      existingCommandSubmissionId: Option[String],
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    LedgerApiErrors.ConsistencyErrors.DuplicateCommand
      .Reject(definiteAnswer, existingCommandSubmissionId)
      .rpcStatus()

  private def invalidRecordTimeReason(
      recordTime: Timestamp,
      tooEarlyUntil: Option[Timestamp],
      tooLateFrom: Option[Timestamp],
  ): String =
    (tooEarlyUntil, tooLateFrom) match {
      case (Some(lowerBound), Some(upperBound)) =>
        s"Record time $recordTime outside of range [$lowerBound, $upperBound]"
      case (Some(lowerBound), None) =>
        s"Record time $recordTime  outside of valid range ($recordTime < $lowerBound)"
      case (None, Some(upperBound)) =>
        s"Record time $recordTime  outside of valid range ($recordTime > $upperBound)"
      case _ =>
        "Record time outside of valid range"
    }

}
