// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.errors

import java.time.Instant

import com.daml.error.{
  ContextualizedErrorLogger,
  ErrorCategory,
  ErrorCode,
  ErrorGroup,
  ErrorResource,
}
import com.daml.error.definitions.ErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LedgerApiErrorGroup
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection.{
  ExternallyInconsistentTransaction,
  InternallyInconsistentTransaction,
}

object KVErrors extends LedgerApiErrorGroup {

  object Time extends ErrorGroup() {

    object InvalidRecordTime
        extends ErrorCode(
          id = "INVALID_RECORD_TIME",
          ErrorCategory.InvalidGivenCurrentSystemStateOther, // It may succeed at a later time
        ) {
      case class Reject(
          override val definiteAnswer: Boolean,
          override val cause: String,
          recordTime: Instant,
          tooEarlyUntil: Option[Instant],
          tooLateAfter: Option[Instant],
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(cause)
    }

    object RecordTimeOutOfRange
        extends ErrorCode(
          id = "RECORD_TIME_OUT_OF_RANGE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther, // It may succeed at a later time
        ) {
      case class Reject(
          minimum_record_time: Instant,
          maximum_record_time: Instant,
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause =
              s"Invalid ledger time: Record time is outside of valid range [$minimum_record_time, $maximum_record_time]"
          )
    }

    object InvalidLedgerTime
        extends ErrorCode(
          id = "INVALID_LEDGER_TIME",
          ErrorCategory.InvalidGivenCurrentSystemStateOther, // It may succeed at a later time
        ) {
      case class Reject(
          details: String,
          ledger_time: Instant,
          ledger_time_lower_bound: Instant,
          ledger_time_upper_bound: Instant,
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(cause = s"Invalid ledger time: $details")
    }

    object CausalMonotonicityViolated
        extends ErrorCode(
          id = "CAUSAL_MONOTONICITY_VIOLATED",
          ErrorCategory.InvalidGivenCurrentSystemStateOther, // It may succeed at a later time
        ) {
      case class Reject(
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Invalid ledger time: Causal monotonicity violated"
          )
    }

  }

  object SubmissionRaces extends ErrorGroup() {

    object ExternallyDuplicateKeys
        extends ErrorCode(
          id = "EXTERNALLY_DUPLICATE_KEYS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther, // It may succeed at a later time
        ) {
      case class Reject(
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Inconsistent: ${ExternallyInconsistentTransaction.DuplicateKeys.description}"
          )
    }

    object ExternallyInconsistentKeys
        extends ErrorCode(
          id = "EXTERNALLY_INCONSISTENT_KEYS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther, // It may succeed at a later time
        ) {
      case class Reject(
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause =
              s"Inconsistent: ${ExternallyInconsistentTransaction.InconsistentKeys.description}"
          )
    }

    object ExternallyInconsistentContracts
        extends ErrorCode(
          id = "EXTERNALLY_INCONSISTENT_CONTRACTS",
          ErrorCategory.InvalidGivenCurrentSystemStateOther, // It may succeed at a later time
        ) {
      case class Reject(
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause =
              s"Inconsistent: ${ExternallyInconsistentTransaction.InconsistentContracts.description}"
          )
    }

  }

  object Parties extends ErrorGroup {

    object SubmittingPartyNotKnownOnLedger
        extends ErrorCode(
          id = "SUBMITTING_PARTY_NOT_KNOWN_ON_LEDGER",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing, // It may become known at a later time
        ) {
      case class Reject(
          submitter_party: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Party not known on ledger: Submitting party '$submitter_party' not known"
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          ErrorResource.Party -> submitter_party
        )
      }
    }

    object PartiesNotKnownOnLedger
        extends ErrorCode(
          id = "PARTIES_NOT_KNOWN_ON_LEDGER",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing, // They may become known at a later time
        ) {
      case class Reject(
          parties: Seq[String]
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Party not known on ledger: Parties not known on ledger ${parties
              .mkString("[", ",", "]")}"
          ) {
        override def resources: Seq[(ErrorResource, String)] = parties.map((ErrorResource.Party, _))
      }
    }

    @deprecated // For backwards compatibility with rejections generated by participant.state.v1 API
    object PartyNotKnownOnLedger
        extends ErrorCode(
          id = "PARTY_NOT_KNOWN_ON_LEDGER",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing, // It may become known at a later time
        ) {
      case class Reject(
          party: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Party not known on ledger: $party"
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(ErrorResource.Party -> party)
      }
    }

  }

  object Resources extends ErrorGroup() {

    object ResourceExhausted
        extends ErrorCode(
          id = "RESOURCE_EXHAUSTED",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      case class Reject(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Resources exhausted: $details"
          )
    }

  }

  object Unauthorized extends ErrorGroup() {

    object SubmitterCannotActViaParticipant
        extends ErrorCode(
          id = "SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT",
          ErrorCategory.InsufficientPermission,
        ) {
      case class Reject(
          details: String,
          submitter: String,
          participantId: String,
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Inconsistent: $details"
          )
    }

  }

  object Internal extends ErrorGroup() {

    object RejectionReasonNotSet
        extends ErrorCode(
          id = "REJECTION_REASON_NOT_SET",
          ErrorCategory.SystemInternalAssumptionViolated, // A reason should always be set
        ) {
      case class Reject(
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = "No reason set for rejection"
          )
    }

    object ValidationFailure
        extends ErrorCode(
          id = "VALIDATION_FAILURE",
          ErrorCategory.SystemInternalAssumptionViolated, // It should have been caught by the participant
        ) {
      case class Reject(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Validation failure: $details"
          )
    }

    object MissingInputState
        extends ErrorCode(
          id = "MISSING_INPUT_STATE",
          ErrorCategory.SystemInternalAssumptionViolated, // The participant should have provided the input
        ) {
      case class Reject(
          key: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Inconsistent: Missing input state for key $key"
          )
    }

    object InternallyDuplicateKeys
        extends ErrorCode(
          id = "INTERNALLY_DUPLICATE_KEYS",
          ErrorCategory.SystemInternalAssumptionViolated, // Should have been caught by the participant
        ) {
      case class Reject(
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Disputed: ${InternallyInconsistentTransaction.DuplicateKeys.description}"
          )
    }

    object InternallyInconsistentKeys
        extends ErrorCode(
          id = "INTERNALLY_INCONSISTENT_KEYS",
          ErrorCategory.SystemInternalAssumptionViolated, // Should have been caught by the participant
        ) {
      case class Reject(
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Disputed: ${InternallyInconsistentTransaction.InconsistentKeys.description}"
          )
    }

    object InvalidParticipantState
        extends ErrorCode(
          id = "INVALID_PARTICIPANT_STATE",
          ErrorCategory.SystemInternalAssumptionViolated,
        ) {
      case class Reject(
          details: String,
          metadata: Map[String, String],
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Disputed: $details"
          ) {
        override def context: Map[String, String] =
          super.context ++ metadata // Only in logs as the category is security sensitive
      }
    }

    @deprecated // For backwards compatibility with rejections generated by participant.state.v1 API
    object Disputed
        extends ErrorCode(
          id = "DISPUTED",
          ErrorCategory.SystemInternalAssumptionViolated, // It should have been caught by the participant
        ) {
      case class Reject(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Disputed: $details"
          )
    }

  }

  object DuplicateCommand
      extends ErrorCode(
        id = "DUPLICATE_COMMAND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists, // It may succeed at a later time
      ) {
    case class Reject(
        override val definiteAnswer: Boolean
    )(implicit loggingContext: ContextualizedErrorLogger)
        extends KVLoggingTransactionErrorImpl(
          cause = "A command with the given submission ID has already been successfully processed"
        )
  }

  @deprecated // For backwards compatibility with rejections generated by participant.state.v1 API
  object Inconsistent
      extends ErrorCode(
        id = "INCONSISTENT",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists, // It may succeed at a later time
      ) {
    case class Reject(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger)
        extends KVLoggingTransactionErrorImpl(
          cause = s"Inconsistent: $details"
        )
  }
}
