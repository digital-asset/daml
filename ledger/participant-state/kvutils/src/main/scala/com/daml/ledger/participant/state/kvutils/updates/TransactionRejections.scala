// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.updates

import com.daml.error.ValueSwitch
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.ledger.participant.state.kvutils.Conversions.parseCompletionInfo
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection.{
  ExternallyInconsistentTransaction,
  InternallyInconsistentTransaction,
}
import com.daml.ledger.participant.state.kvutils.store.events._
import com.daml.ledger.participant.state.kvutils.CorrelationId
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.lf.data.Time.Timestamp
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.any.{Any => AnyProto}
import com.google.rpc.code.Code
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.Status

import java.io.StringWriter
import java.time.Instant
import scala.jdk.CollectionConverters._

/** Utilities for converting between rejection log entries and updates and/or gRPC statuses.
  */
private[kvutils] object TransactionRejections {

  def invalidRecordTimeRejectionUpdate(
      recordTime: Timestamp,
      rejectionEntry: DamlTransactionRejectionEntry,
      reason: CorrelationId,
      errorVersionSwitch: ValueSwitch[Status],
  ): Update.CommandRejected = {
    val statusBuilder = invalidRecordTimeRejectionStatus(rejectionEntry, reason, _)
    Update.CommandRejected(
      recordTime = recordTime,
      completionInfo = parseCompletionInfo(
        recordTime,
        rejectionEntry.getSubmitterInfo,
      ),
      reasonTemplate = FinalReason(
        errorVersionSwitch.choose(
          statusBuilder(Code.ABORTED),
          statusBuilder(Code.FAILED_PRECONDITION),
        )
      ),
    )
  }

  def duplicateCommandsRejectionUpdate(
      recordTime: Timestamp,
      rejectionEntry: DamlTransactionRejectionEntry,
  ): Update.CommandRejected =
    Update.CommandRejected(
      recordTime = recordTime,
      completionInfo = parseCompletionInfo(
        recordTime,
        rejectionEntry.getSubmitterInfo,
      ),
      reasonTemplate = FinalReason(
        duplicateCommandsRejectionStatus(rejectionEntry, Code.ALREADY_EXISTS)
      ),
    )

  def reasonNotSetStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(entry, _, "No reason set for rejection")
    errorVersionSwitch.choose(
      statusBuilder(Code.UNKNOWN),
      statusBuilder(Code.INTERNAL), // We should always set a reason
    )
  }

  def invalidParticipantStateStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: InvalidParticipantState,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Disputed: ${rejection.getDetails}",
      rejection.getMetadataMap.asScala.toMap,
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.INVALID_ARGUMENT),
      statusBuilder(Code.INTERNAL),
    )
  }

  def partiesNotKnownOnLedgerStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: PartiesNotKnownOnLedger,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    val parties = rejection.getPartiesList
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Party not known on ledger: Parties not known on ledger ${parties.asScala.mkString("[", ",", "]")}",
      Map("parties" -> objectToJsonString(parties)),
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.INVALID_ARGUMENT),
      statusBuilder(Code.FAILED_PRECONDITION), // The party may become known at a later time
    )
  }

  def submittingPartyNotKnownOnLedgerStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: SubmittingPartyNotKnownOnLedger,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Party not known on ledger: Submitting party '${rejection.getSubmitterParty}' not known",
      Map("submitter_party" -> rejection.getSubmitterParty),
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.INVALID_ARGUMENT),
      statusBuilder(Code.FAILED_PRECONDITION), // The party may become known at a later time
    )
  }

  def partyNotKnownOnLedgerStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: PartyNotKnownOnLedger,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Party not known on ledger: ${rejection.getDetails}",
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.INVALID_ARGUMENT),
      statusBuilder(Code.FAILED_PRECONDITION), // The party may become known at a later time
    )
  }

  def causalMonotonicityViolatedStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      "Invalid ledger time: Causal monotonicity violated",
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.ABORTED),
      statusBuilder(Code.FAILED_PRECONDITION), // May succeed at a later time
    )
  }

  def recordTimeOutOfRangeStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: RecordTimeOutOfRange,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Invalid ledger time: Record time is outside of valid range [${rejection.getMinimumRecordTime}, ${rejection.getMaximumRecordTime}]",
      Map(
        "minimum_record_time" -> Instant
          .ofEpochSecond(
            rejection.getMinimumRecordTime.getSeconds,
            rejection.getMinimumRecordTime.getNanos.toLong,
          )
          .toString,
        "maximum_record_time" -> Instant
          .ofEpochSecond(
            rejection.getMaximumRecordTime.getSeconds,
            rejection.getMaximumRecordTime.getNanos.toLong,
          )
          .toString,
      ),
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.ABORTED),
      statusBuilder(Code.FAILED_PRECONDITION), // May succeed at a later time
    )
  }

  def missingInputStateStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: MissingInputState,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Inconsistent: Missing input state for key ${rejection.getKey.toString}",
      Map("key" -> rejection.getKey.toString),
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.ABORTED),
      statusBuilder(Code.INTERNAL), // The inputs should have been provided by the participant
    )
  }

  def externallyInconsistentKeysStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Inconsistent: ${ExternallyInconsistentTransaction.InconsistentKeys.description}",
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.ABORTED),
      statusBuilder(Code.FAILED_PRECONDITION), // May succeed at a later time
    )
  }

  def externallyDuplicateKeysStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Inconsistent: ${ExternallyInconsistentTransaction.DuplicateKeys.description}",
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.ABORTED),
      statusBuilder(Code.FAILED_PRECONDITION), // May succeed at a later time
    )
  }

  def externallyInconsistentContractsStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Inconsistent: ${ExternallyInconsistentTransaction.InconsistentContracts.description}",
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.ABORTED),
      statusBuilder(Code.FAILED_PRECONDITION), // May succeed at a later time
    )
  }

  def internallyInconsistentKeysStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Disputed: ${InternallyInconsistentTransaction.InconsistentKeys.description}",
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.INVALID_ARGUMENT),
      statusBuilder(Code.INTERNAL), // Should have been caught by the participant
    )
  }

  def internallyDuplicateKeysStatus(
      entry: DamlTransactionRejectionEntry,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Disputed: ${InternallyInconsistentTransaction.DuplicateKeys.description}",
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.INVALID_ARGUMENT),
      statusBuilder(Code.INTERNAL), // Should have been caught by the participant
    )
  }

  def validationFailureStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: ValidationFailure,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    val statusBuilder = disputedStatusBuilder(entry, rejection.getDetails)
    errorVersionSwitch.choose(
      statusBuilder(Code.INVALID_ARGUMENT),
      statusBuilder(Code.INTERNAL), // Should have been caught by the participant
    )
  }

  def duplicateCommandStatus(
      entry: DamlTransactionRejectionEntry
  ): Status = buildStatus(
    entry,
    Code.ALREADY_EXISTS,
    "Duplicate commands",
  )

  def resourceExhaustedStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: ResourcesExhausted,
  ): Status = buildStatus(
    entry,
    Code.ABORTED,
    s"Resources exhausted: ${rejection.getDetails}",
  )

  def inconsistentStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: Inconsistent,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    def statusBuilder: Code => Status = buildStatus(
      entry,
      _,
      s"Inconsistent: ${rejection.getDetails}",
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.ABORTED),
      statusBuilder(Code.FAILED_PRECONDITION), // May succeed at a later time
    )
  }

  def submitterCannotActViaParticipantStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: SubmitterCannotActViaParticipant,
  ): Status = buildStatus(
    entry,
    Code.PERMISSION_DENIED,
    s"Submitter cannot act via participant: ${rejection.getDetails}",
    Map(
      "submitter_party" -> rejection.getSubmitterParty,
      "participant_id" -> rejection.getParticipantId,
    ),
  )

  def disputedStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: Disputed,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    val statusBuilder = disputedStatusBuilder(entry, rejection.getDetails)
    errorVersionSwitch.choose(
      statusBuilder(Code.INVALID_ARGUMENT),
      statusBuilder(Code.INTERNAL), // Should have been caught by the participant
    )
  }

  def invalidLedgerTimeStatus(
      entry: DamlTransactionRejectionEntry,
      rejection: InvalidLedgerTime,
      errorVersionSwitch: ValueSwitch[Status],
  ): Status = {
    val statusBuilder = buildStatus(
      entry,
      _,
      s"Invalid ledger time: ${rejection.getDetails}",
      Map(
        "ledger_time" -> rejection.getLedgerTime.toString,
        "lower_bound" -> rejection.getLowerBound.toString,
        "upper_bound" -> rejection.getUpperBound.toString,
      ),
    )
    errorVersionSwitch.choose(
      statusBuilder(Code.ABORTED),
      statusBuilder(Code.FAILED_PRECONDITION), // May succeed at a later time
    )
  }

  private def buildStatus(
      entry: DamlTransactionRejectionEntry,
      code: Code,
      message: String,
      additionalMetadata: Map[String, String] = Map.empty,
  ) = Status.of(
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

  private def invalidRecordTimeRejectionStatus(
      rejectionEntry: DamlTransactionRejectionEntry,
      reason: CorrelationId,
      errorCode: Code,
  ) = Status.of(
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

  private def duplicateCommandsRejectionStatus(
      rejectionEntry: DamlTransactionRejectionEntry,
      errorCode: Code,
  ) = Status.of(
    errorCode.value,
    "Duplicate commands",
    Seq(
      AnyProto.pack[ErrorInfo](
        // the definite answer is false, as the rank-based deduplication is not yet implemented
        ErrorInfo(metadata =
          Map(
            GrpcStatuses.DefiniteAnswerKey -> rejectionEntry.getDefiniteAnswer.toString
          )
        )
      )
    ),
  )

  private def objectToJsonString(obj: Object): String = {
    val stringWriter = new StringWriter
    val objectMapper = new ObjectMapper
    objectMapper.writeValue(stringWriter, obj)
    stringWriter.toString
  }

  private def disputedStatusBuilder(
      entry: DamlTransactionRejectionEntry,
      rejectionString: String,
  ): Code => Status = buildStatus(
    entry,
    _,
    s"Disputed: $rejectionString",
  )
}
