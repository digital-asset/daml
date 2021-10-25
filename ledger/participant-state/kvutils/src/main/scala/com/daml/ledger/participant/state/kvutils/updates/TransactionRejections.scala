// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.updates

import java.time.Instant

import com.google.protobuf.any.{Any => AnyProto}
import com.google.rpc.code.Code
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.Status

import com.daml.error.{ContextualizedErrorLogger, ValueSwitch}
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.ledger.participant.state.kvutils.Conversions.parseCompletionInfo
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection.{
  ExternallyInconsistentTransaction,
  InternallyInconsistentTransaction,
}
import com.daml.ledger.participant.state.kvutils.errors.KVCompletionErrors
import com.daml.ledger.participant.state.kvutils.store.events._
import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.lf.data.Time.Timestamp

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

/** Utilities for converting between rejection log entries and updates and/or gRPC statuses.
  */
private[kvutils] object TransactionRejections {

  @nowarn("msg=deprecated")
  def invalidRecordTimeRejectionUpdate(
      recordTime: Timestamp,
      tooEarlyUntil: Option[Timestamp],
      tooLateFrom: Option[Timestamp],
      rejectionEntry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Update.CommandRejected = {
    Update.CommandRejected(
      recordTime = recordTime,
      completionInfo = parseCompletionInfo(
        Conversions.parseInstant(recordTime),
        rejectionEntry.getSubmitterInfo,
      ),
      reasonTemplate = FinalReason(
        errorVersionSwitch.choose(
          V1.invalidRecordTimeRejectionStatus(
            rejectionEntry,
            invalidRecordTimeReason(recordTime, tooEarlyUntil, tooEarlyUntil),
            Code.ABORTED,
          ),
          V2.invalidRecordTimeRejectionStatus(
            rejectionEntry,
            recordTime,
            tooEarlyUntil,
            tooLateFrom,
          ),
        )
      ),
    )
  }

  @nowarn("msg=deprecated")
  def duplicateCommandsRejectionUpdate(
      recordTime: Timestamp,
      rejectionEntry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Update.CommandRejected = {
    val definiteAnswer = rejectionEntry.getDefiniteAnswer
    Update.CommandRejected(
      recordTime = recordTime,
      completionInfo = parseCompletionInfo(
        Conversions.parseInstant(recordTime),
        rejectionEntry.getSubmitterInfo,
      ),
      reasonTemplate = FinalReason(
        errorVersionSwitch.choose(
          V1.duplicateCommandsRejectionStatus(definiteAnswer, Code.ALREADY_EXISTS),
          V2.duplicateCommandsRejectionStatus(definiteAnswer),
        )
      ),
    )
  }

  @nowarn("msg=deprecated")
  def rejectionReasonNotSetStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    errorVersionSwitch.choose(
      V1.status(entry, Code.UNKNOWN, "No reason set for rejection"),
      V2.rejectionReasonNotSetStatus(),
    )

  @nowarn("msg=deprecated")
  def invalidParticipantStateStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: InvalidParticipantState,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    val metadata = rejection.getMetadataMap.asScala.toMap
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.INVALID_ARGUMENT,
        s"Disputed: $details",
        metadata,
      ),
      V2.invalidParticipantStateStatus(details, metadata),
    )
  }

  @nowarn("msg=deprecated")
  def partiesNotKnownOnLedgerStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: PartiesNotKnownOnLedger,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val parties = rejection.getPartiesList
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.INVALID_ARGUMENT,
        s"Party not known on ledger: Parties not known on ledger ${parties.asScala.mkString("[", ",", "]")}",
        Map("parties" -> Conversions.objectToJsonString(parties)),
      ),
      V2.partiesNotKnownOnLedgerStatus(parties.asScala.toSeq),
    )
  }

  @nowarn("msg=deprecated")
  def submittingPartyNotKnownOnLedgerStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: SubmittingPartyNotKnownOnLedger,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val submitter = rejection.getSubmitterParty
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.INVALID_ARGUMENT,
        s"Party not known on ledger: Submitting party '$submitter' not known",
        Map("submitter_party" -> submitter),
      ),
      V2.submittingPartyNotKnownOnLedgerStatus(submitter),
    )
  }

  @nowarn("msg=deprecated")
  def causalMonotonicityViolatedStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.ABORTED,
        "Invalid ledger time: Causal monotonicity violated",
      ),
      V2.causalMonotonicityViolatedStatus(),
    )

  @nowarn("msg=deprecated")
  def recordTimeOutOfRangeStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: RecordTimeOutOfRange,
      errorVersionSwitch: ValueSwitch[Status],
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
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.ABORTED,
        s"Invalid ledger time: Record time is outside of valid range [${rejection.getMinimumRecordTime}, ${rejection.getMaximumRecordTime}]",
        Map(
          "minimum_record_time" -> minRecordTime.toString,
          "maximum_record_time" -> maxRecordTime.toString,
        ),
      ),
      V2.recordTimeOutOfRangeStatus(minRecordTime, maxRecordTime),
    )
  }

  @nowarn("msg=deprecated")
  def externallyDuplicateKeysStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.ABORTED,
        s"Inconsistent: ${ExternallyInconsistentTransaction.DuplicateKeys.description}",
      ),
      V2.externallyDuplicateKeysStatus(),
    )

  @nowarn("msg=deprecated")
  def externallyInconsistentKeysStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.ABORTED,
        s"Inconsistent: ${ExternallyInconsistentTransaction.InconsistentKeys.description}",
      ),
      V2.externallyInconsistentKeysStatus(),
    )

  @nowarn("msg=deprecated")
  def externallyInconsistentContractsStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.ABORTED,
        s"Inconsistent: ${ExternallyInconsistentTransaction.InconsistentContracts.description}",
      ),
      V2.externallyInconsistentContractsStatus(),
    )

  @nowarn("msg=deprecated")
  def missingInputStateStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: MissingInputState,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val key = rejection.getKey.toString
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.ABORTED,
        s"Inconsistent: Missing input state for key $key",
        Map("key" -> key),
      ),
      V2.missingInputStateStatus(key),
    )
  }

  @nowarn("msg=deprecated")
  def internallyInconsistentKeysStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.INVALID_ARGUMENT,
        s"Disputed: ${InternallyInconsistentTransaction.InconsistentKeys.description}",
      ),
      V2.internallyInconsistentKeysStatus(), // Code.INTERNAL as it should have been caught by the participant
    )

  @nowarn("msg=deprecated")
  def internallyDuplicateKeysStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.INVALID_ARGUMENT,
        s"Disputed: ${InternallyInconsistentTransaction.DuplicateKeys.description}",
      ),
      V2.internallyDuplicateKeysStatus(), // Code.INTERNAL as it should have been caught by the participant
    )

  @nowarn("msg=deprecated")
  def validationFailureStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: ValidationFailure,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    errorVersionSwitch.choose(
      V1.disputedStatus(entry, details, Code.INVALID_ARGUMENT),
      V2.validationFailureStatus(details),
    )
  }

  @nowarn("msg=deprecated")
  def duplicateCommandStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status =
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.ALREADY_EXISTS,
        "Duplicate commands",
      ),
      V2.duplicateCommandsRejectionStatus(),
    )

  @nowarn("msg=deprecated")
  def submitterCannotActViaParticipantStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: SubmitterCannotActViaParticipant,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val submitter = rejection.getSubmitterParty
    val participantId = rejection.getParticipantId
    val details = rejection.getDetails
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.PERMISSION_DENIED,
        s"Submitter cannot act via participant: $details",
        Map(
          "submitter_party" -> submitter,
          "participant_id" -> participantId,
        ),
      ),
      V2.submitterCannotActViaParticipantStatus(details, submitter, participantId),
    )
  }

  @nowarn("msg=deprecated")
  def invalidLedgerTimeStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: InvalidLedgerTime,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    val ledgerTime = rejection.getLedgerTime
    val ledgerTimeLowerBound = rejection.getLowerBound
    val ledgerTimeUpperBound = rejection.getUpperBound

    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.ABORTED,
        s"Invalid ledger time: $details",
        Map(
          "ledger_time" -> ledgerTime.toString,
          "lower_bound" -> ledgerTimeLowerBound.toString,
          "upper_bound" -> ledgerTimeUpperBound.toString,
        ),
      ),
      V2.invalidLedgerTimeStatus(
        details,
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
      ),
    )
  }

  @deprecated
  def partyNotKnownOnLedgerStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: PartyNotKnownOnLedger,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.INVALID_ARGUMENT,
        s"Party not known on ledger: $details",
      ),
      V2.partyNotKnownOnLedgerStatus(details),
    )
  }

  @deprecated
  def resourceExhaustedStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: ResourcesExhausted,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.ABORTED,
        s"Resources exhausted: $details",
      ),
      V2.resourceExhaustedStatus(details),
    )
  }

  @deprecated
  def inconsistentStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: Inconsistent,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    errorVersionSwitch.choose(
      V1.status(
        entry,
        Code.ABORTED,
        s"Inconsistent: $details",
      ),
      V2.inconsistentStatus(details),
    )
  }

  @deprecated
  def disputedStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: Disputed,
      errorVersionSwitch: ValueSwitch[Status],
  )(implicit loggingContext: ContextualizedErrorLogger): Status = {
    val details = rejection.getDetails
    errorVersionSwitch.choose(
      V1.disputedStatus(entry, details, Code.INVALID_ARGUMENT),
      V2.disputedStatus(details),
    )
  }

  private object V2 {

    def externallyDuplicateKeysStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.SubmissionRaces.ExternallyDuplicateKeys
        .Reject()
        .asStatus

    def externallyInconsistentKeysStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.SubmissionRaces.ExternallyInconsistentKeys
        .Reject()
        .asStatus

    def externallyInconsistentContractsStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.SubmissionRaces.ExternallyInconsistentContracts
        .Reject()
        .asStatus

    def submitterCannotActViaParticipantStatus(
        details: String,
        submitter: String,
        participantId: String,
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Unauthorized.SubmitterCannotActViaParticipant
        .Reject(
          details,
          submitter,
          participantId,
        )
        .asStatus

    def recordTimeOutOfRangeStatus(
        minimumRecordTime: Instant,
        maximumRecordTime: Instant,
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Time.RecordTimeOutOfRange
        .Reject(minimumRecordTime, maximumRecordTime)
        .asStatus

    def invalidRecordTimeRejectionStatus(
        rejectionEntry: DamlTransactionRejectionEntry,
        recordTime: Timestamp,
        tooEarlyUntil: Option[Timestamp],
        tooLateFrom: Option[Timestamp],
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Time.InvalidRecordTime
        .Reject(
          rejectionEntry.getDefiniteAnswer,
          invalidRecordTimeReason(recordTime, tooEarlyUntil, tooLateFrom),
          recordTime.toInstant,
          tooEarlyUntil.map(_.toInstant),
          tooLateFrom.map(_.toInstant),
        )
        .asStatus

    def invalidLedgerTimeStatus(
        details: String,
        ledger_time: Instant,
        ledger_time_lower_bound: Instant,
        ledger_time_upper_bound: Instant,
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Time.InvalidLedgerTime
        .Reject(details, ledger_time, ledger_time_lower_bound, ledger_time_upper_bound)
        .asStatus

    def causalMonotonicityViolatedStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Time.CausalMonotonicityViolated
        .Reject()
        .asStatus

    def duplicateCommandsRejectionStatus(
        definiteAnswer: Boolean = false
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.DuplicateCommand
        .Reject(definiteAnswer)
        .asStatus

    def rejectionReasonNotSetStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Internal.RejectionReasonNotSet
        .Reject()
        .asStatus

    def invalidParticipantStateStatus(
        details: String,
        metadata: Map[String, String],
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Internal.InvalidParticipantState
        .Reject(details, metadata)
        .asStatus

    def validationFailureStatus(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Internal.ValidationFailure
        .Reject(details)
        .asStatus

    def missingInputStateStatus(
        key: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Internal.MissingInputState
        .Reject(key)
        .asStatus

    def internallyInconsistentKeysStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Internal.InternallyInconsistentKeys
        .Reject()
        .asStatus

    def internallyDuplicateKeysStatus(
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Internal.InternallyDuplicateKeys
        .Reject()
        .asStatus

    def submittingPartyNotKnownOnLedgerStatus(
        submitter: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Parties.SubmittingPartyNotKnownOnLedger
        .Reject(submitter)
        .asStatus

    def partiesNotKnownOnLedgerStatus(
        parties: Seq[String]
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Parties.PartiesNotKnownOnLedger
        .Reject(parties)
        .asStatus

    @deprecated
    def partyNotKnownOnLedgerStatus(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Parties.PartyNotKnownOnLedger
        .Reject(details)
        .asStatus

    @deprecated
    def resourceExhaustedStatus(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Resources.ResourceExhausted
        .Reject(details)
        .asStatus

    @deprecated
    def inconsistentStatus(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Inconsistent
        .Reject(details)
        .asStatus

    @deprecated
    def disputedStatus(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger): Status =
      KVCompletionErrors.Internal.Disputed
        .Reject(details)
        .asStatus
  }

  @deprecated
  private object V1 {
    def status(
        entry: DamlTransactionRejectionEntry,
        code: Code,
        message: String,
        additionalMetadata: Map[String, String] = Map.empty,
    ): Status = Status.of(
      code.value,
      message,
      Seq(
        AnyProto.pack[ErrorInfo](
          ErrorInfo(metadata =
            additionalMetadata + (GrpcStatuses.DefiniteAnswerKey -> entry.getDefiniteAnswer.toString)
          )
        )
      ),
    )

    def disputedStatus(
        entry: DamlTransactionRejectionEntry,
        rejectionString: String,
        code: Code,
    ): Status = status(
      entry,
      code,
      s"Disputed: $rejectionString",
    )

    def invalidRecordTimeRejectionStatus(
        rejectionEntry: DamlTransactionRejectionEntry,
        reason: String,
        errorCode: Code,
    ): Status = Status.of(
      errorCode.value,
      reason,
      Seq(
        AnyProto.pack[ErrorInfo](
          ErrorInfo(metadata =
            Map(
              GrpcStatuses.DefiniteAnswerKey -> rejectionEntry.getDefiniteAnswer.toString
            )
          )
        )
      ),
    )

    def duplicateCommandsRejectionStatus(
        definiteAnswer: Boolean,
        errorCode: Code,
    ): Status = Status.of(
      errorCode.value,
      "Duplicate commands",
      Seq(
        AnyProto.pack[ErrorInfo](
          // the definite answer is false, as the rank-based deduplication is not yet implemented
          ErrorInfo(metadata =
            Map(
              GrpcStatuses.DefiniteAnswerKey -> definiteAnswer.toString
            )
          )
        )
      ),
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
