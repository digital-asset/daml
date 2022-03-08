// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.updates

import com.daml.error.definitions.LedgerApiErrors

import java.time.Instant
import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.GrpcStatus
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
    Update.CommandRejected(
      recordTime = recordTime,
      completionInfo = parseCompletionInfo(
        recordTime,
        rejectionEntry.getSubmitterInfo,
      ),
      reasonTemplate = FinalReason(
        duplicateCommandsRejectionStatus(existingCommandSubmissionId)
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
    GrpcStatus.toProto(
      LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
        .Reject(parties.asScala.toSet)
        .asGrpcStatusFromContext
    )
  }

  def submittingPartyNotKnownOnLedgerStatus(
      rejection: SubmittingPartyNotKnownOnLedger
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val submitter = rejection.getSubmitterParty
    GrpcStatus.toProto(
      LedgerApiErrors.WriteServiceRejections.SubmittingPartyNotKnownOnLedger
        .Reject(submitter)
        .asGrpcStatusFromContext
    )
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
    GrpcStatus.toProto(
      LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
        .Reject(ExternallyInconsistentTransaction.DuplicateKeys.description)
        .asGrpcStatusFromContext
    )

  def externallyInconsistentKeysStatus()(implicit
      loggingContext: ContextualizedErrorLogger
  ): Status =
    GrpcStatus.toProto(
      LedgerApiErrors.ConsistencyErrors.InconsistentContractKey
        .Reject(ExternallyInconsistentTransaction.InconsistentKeys.description)
        .asGrpcStatusFromContext
    )

  def externallyInconsistentContractsStatus()(implicit
      loggingContext: ContextualizedErrorLogger
  ): Status =
    GrpcStatus.toProto(
      LedgerApiErrors.ConsistencyErrors.InconsistentContracts
        .Reject(ExternallyInconsistentTransaction.InconsistentContracts.description)
        .asGrpcStatusFromContext
    )

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
    GrpcStatus.toProto(
      LedgerApiErrors.WriteServiceRejections.Internal.InternallyInconsistentKeys
        .Reject(InternallyInconsistentTransaction.InconsistentKeys.description)
        .asGrpcStatusFromContext
    )

  def internallyDuplicateKeysStatus(
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    GrpcStatus.toProto(
      LedgerApiErrors.WriteServiceRejections.Internal.InternallyDuplicateKeys
        .Reject(InternallyInconsistentTransaction.DuplicateKeys.description)
        .asGrpcStatusFromContext
    )

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
    GrpcStatus.toProto(
      LedgerApiErrors.WriteServiceRejections.SubmitterCannotActViaParticipant
        .RejectWithSubmitterAndParticipantId(
          details,
          submitter,
          participantId,
        )
        .asGrpcStatusFromContext
    )
  }

  def invalidLedgerTimeStatus(
      rejection: InvalidLedgerTime
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    val ledgerTime = rejection.getLedgerTime
    val ledgerTimeLowerBound = rejection.getLowerBound
    val ledgerTimeUpperBound = rejection.getUpperBound

    GrpcStatus.toProto(
      LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
        .RejectEnriched(
          cause = details,
          ledger_time = Instant
            .ofEpochSecond(
              ledgerTime.getSeconds,
              ledgerTime.getNanos.toLong,
            ),
          ledger_time_lower_bound = Instant
            .ofEpochSecond(
              ledgerTimeLowerBound.getSeconds,
              ledgerTimeLowerBound.getNanos.toLong,
            ),
          ledger_time_upper_bound = Instant
            .ofEpochSecond(
              ledgerTimeUpperBound.getSeconds,
              ledgerTimeUpperBound.getNanos.toLong,
            ),
        )
        .asGrpcStatusFromContext
    )
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
    GrpcStatus.toProto(
      LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
        .RejectDeprecated(details)
        .asGrpcStatusFromContext
    )
  }

  @deprecated
  def inconsistentStatus(
      rejection: Inconsistent
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    GrpcStatus.toProto(
      LedgerApiErrors.ConsistencyErrors.Inconsistent
        .Reject(details)
        .asGrpcStatusFromContext
    )
  }

  @deprecated
  def disputedStatus(
      rejection: Disputed
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    GrpcStatus.toProto(
      LedgerApiErrors.WriteServiceRejections.Disputed
        .Reject(details)
        .asGrpcStatusFromContext
    )
  }

  private def duplicateCommandsRejectionStatus(
      existingCommandSubmissionId: Option[String]
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    GrpcStatus.toProto(
      LedgerApiErrors.ConsistencyErrors.DuplicateCommand
        .Reject(existingCommandSubmissionId)
        .asGrpcStatusFromContext
    )

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
