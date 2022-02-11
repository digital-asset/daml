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

import scala.annotation.nowarn
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
        V2.invalidRecordTimeRejectionStatus(
          rejectionEntry,
          recordTime,
          tooEarlyUntil,
          tooLateFrom,
        )
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
        V2.duplicateCommandsRejectionStatus(definiteAnswer, existingCommandSubmissionId)
      ),
    )
  }

  def rejectionReasonNotSetStatus()(implicit loggingContext: ContextualizedErrorLogger): Status =
    V2.rejectionReasonNotSetStatus()

  def invalidParticipantStateStatus(
      rejection: InvalidParticipantState
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    val metadata = rejection.getMetadataMap.asScala.toMap
    V2.invalidParticipantStateStatus(details, metadata)
  }

  def partiesNotKnownOnLedgerStatus(
      rejection: PartiesNotKnownOnLedger
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val parties = rejection.getPartiesList
    V2.partiesNotKnownOnLedgerStatus(parties.asScala.toSeq)
  }

  def submittingPartyNotKnownOnLedgerStatus(
      rejection: SubmittingPartyNotKnownOnLedger
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val submitter = rejection.getSubmitterParty
    V2.submittingPartyNotKnownOnLedgerStatus(submitter)
  }

  def causalMonotonicityViolatedStatus()(implicit
      loggingContext: ContextualizedErrorLogger
  ): Status =
    V2.causalMonotonicityViolatedStatus()

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
    V2.recordTimeOutOfRangeStatus(minRecordTime, maxRecordTime)
  }

  def externallyDuplicateKeysStatus()(implicit loggingContext: ContextualizedErrorLogger): Status =
    V2.externallyDuplicateKeysStatus()

  def externallyInconsistentKeysStatus()(implicit
      loggingContext: ContextualizedErrorLogger
  ): Status =
    V2.externallyInconsistentKeysStatus()

  def externallyInconsistentContractsStatus()(implicit
      loggingContext: ContextualizedErrorLogger
  ): Status =
    V2.externallyInconsistentContractsStatus()

  def missingInputStateStatus(
      rejection: MissingInputState
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val key = rejection.getKey.toString
    V2.missingInputStateStatus(key)
  }

  def internallyInconsistentKeysStatus(
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    V2.internallyInconsistentKeysStatus() // Code.INTERNAL as it should have been caught by the participant

  def internallyDuplicateKeysStatus(
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    V2.internallyDuplicateKeysStatus() // Code.INTERNAL as it should have been caught by the participant

  def validationFailureStatus(
      rejection: ValidationFailure
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    V2.validationFailureStatus(details)
  }

  def duplicateCommandStatus(
      entry: DamlTransactionRejectionEntry
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val rejectionReason = entry.getDuplicateCommand
    V2.duplicateCommandsRejectionStatus(existingCommandSubmissionId =
      Some(rejectionReason.getSubmissionId).filter(_.nonEmpty)
    )
  }

  def submitterCannotActViaParticipantStatus(
      rejection: SubmitterCannotActViaParticipant
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val submitter = rejection.getSubmitterParty
    val participantId = rejection.getParticipantId
    val details = rejection.getDetails
    V2.submitterCannotActViaParticipantStatus(details, submitter, participantId)
  }

  @nowarn("msg=deprecated")
  def invalidLedgerTimeStatus(
      rejection: InvalidLedgerTime
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    val ledgerTime = rejection.getLedgerTime
    val ledgerTimeLowerBound = rejection.getLowerBound
    val ledgerTimeUpperBound = rejection.getUpperBound

    V2.invalidLedgerTimeStatus(
      details,
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
  }

  def resourceExhaustedStatus(
      rejection: ResourcesExhausted
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    V2.resourceExhaustedStatus(details)
  }

  @deprecated
  def partyNotKnownOnLedgerStatus(
      rejection: PartyNotKnownOnLedger
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    V2.partyNotKnownOnLedgerStatus(details)
  }

  @deprecated
  def inconsistentStatus(
      rejection: Inconsistent
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    V2.inconsistentStatus(details)
  }

  @deprecated
  def disputedStatus(
      rejection: Disputed
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    V2.disputedStatus(details)
  }

  private object V2 {

    def externallyDuplicateKeysStatus()(implicit
        loggingContext: ContextualizedErrorLogger
    ): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
          .Reject(ExternallyInconsistentTransaction.DuplicateKeys.description)
          .asGrpcStatusFromContext
      )

    def externallyInconsistentKeysStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.InconsistentContractKey
          .Reject(ExternallyInconsistentTransaction.InconsistentKeys.description)
          .asGrpcStatusFromContext
      )

    def externallyInconsistentContractsStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.InconsistentContracts
          .Reject(ExternallyInconsistentTransaction.InconsistentContracts.description)
          .asGrpcStatusFromContext
      )

    def submitterCannotActViaParticipantStatus(
        details: String,
        submitter: String,
        participantId: String,
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.WriteServiceRejections.SubmitterCannotActViaParticipant
          .RejectWithSubmitterAndParticipantId(
            details,
            submitter,
            participantId,
          )
          .asGrpcStatusFromContext
      )

    def recordTimeOutOfRangeStatus(
        minimumRecordTime: Instant,
        maximumRecordTime: Instant,
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVErrors.Time.RecordTimeOutOfRange
        .Reject(minimumRecordTime, maximumRecordTime)
        .asStatus

    def invalidRecordTimeRejectionStatus(
        rejectionEntry: DamlTransactionRejectionEntry,
        recordTime: Timestamp,
        tooEarlyUntil: Option[Timestamp],
        tooLateFrom: Option[Timestamp],
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVErrors.Time.InvalidRecordTime
        .Reject(
          rejectionEntry.getDefiniteAnswer,
          invalidRecordTimeReason(recordTime, tooEarlyUntil, tooLateFrom),
          recordTime.toInstant,
          tooEarlyUntil.map(_.toInstant),
          tooLateFrom.map(_.toInstant),
        )
        .asStatus

    def causalMonotonicityViolatedStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVErrors.Time.CausalMonotonicityViolated
        .Reject()
        .asStatus

    def duplicateCommandsRejectionStatus(
        definiteAnswer: Boolean = false,
        existingCommandSubmissionId: Option[String],
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.DuplicateCommand
          .Reject(definiteAnswer, existingCommandSubmissionId)
          .asGrpcStatusFromContext
      )

    def rejectionReasonNotSetStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVErrors.Internal.RejectionReasonNotSet
        .Reject()
        .asStatus

    def invalidParticipantStateStatus(
        details: String,
        metadata: Map[String, String],
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVErrors.Internal.InvalidParticipantState
        .Reject(details, metadata)
        .asStatus

    def validationFailureStatus(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVErrors.Consistency.ValidationFailure
        .Reject(details)
        .asStatus

    def missingInputStateStatus(
        key: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVErrors.Internal.MissingInputState
        .Reject(key)
        .asStatus

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

    def submittingPartyNotKnownOnLedgerStatus(
        submitter: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.WriteServiceRejections.SubmittingPartyNotKnownOnLedger
          .Reject(submitter)
          .asGrpcStatusFromContext
      )

    def partiesNotKnownOnLedgerStatus(
        parties: Seq[String]
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
          .Reject(parties.toSet)
          .asGrpcStatusFromContext
      )

    def resourceExhaustedStatus(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVErrors.Resources.ResourceExhausted
        .Reject(details)
        .asStatus

    @deprecated
    def invalidLedgerTimeStatus(
        details: String,
        ledgerTime: Instant,
        ledgerTimeLowerBound: Instant,
        ledgerTimeUpperBound: Instant,
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
          .RejectEnriched(details, ledgerTime, ledgerTimeLowerBound, ledgerTimeUpperBound)
          .asGrpcStatusFromContext
      )

    @deprecated
    def partyNotKnownOnLedgerStatus(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
          .RejectDeprecated(details)
          .asGrpcStatusFromContext
      )

    @deprecated
    def inconsistentStatus(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.Inconsistent
          .Reject(details)
          .asGrpcStatusFromContext
      )

    @deprecated
    def disputedStatus(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      GrpcStatus.toProto(
        LedgerApiErrors.WriteServiceRejections.Disputed
          .Reject(details)
          .asGrpcStatusFromContext
      )
  }

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
