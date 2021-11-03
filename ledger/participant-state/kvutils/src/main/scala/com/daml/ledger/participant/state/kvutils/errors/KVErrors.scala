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
  Explanation,
  Resolution,
}
import com.daml.error.definitions.ErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LedgerApiErrorGroup
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection.{
  ExternallyInconsistentTransaction,
  InternallyInconsistentTransaction,
}

@Explanation(
  "Errors that are specific to ledgers based on the KV architecture. " +
    "Note that this section will soon cover all ledgers due to an ongoing error consolidation effort."
)
object KVErrors extends LedgerApiErrorGroup {

  @Explanation("Errors that relate to the Daml concepts of time.")
  object Time extends ErrorGroup() {

    @Explanation(
      "The record time is not within bounds for reasons other than deduplication, such as " +
        "excessive latency. Excessive clock skew between the participant and the committer " +
        "or a time model that is too restrictive may also produce this rejection."
    )
    @Resolution("Retry the submission or contact the participant operator.")
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

    @Explanation(
      "The record time is not within bounds for reasons other than deduplication, such as " +
        "excessive latency. Excessive clock skew between the participant and the committer " +
        "or a time model that is too restrictive may also produce this rejection."
    )
    @Resolution("Retry the transaction submission or contact the participant operator.")
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

    @Explanation(
      "At least one input contract's ledger time is later than that of the submitted transaction."
    )
    @Resolution("Retry the transaction submission.")
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

  @Explanation(
    "Errors that can arise due to concurrent processing of transactions in the participant."
  )
  object SubmissionRaces extends ErrorGroup() {
    @Explanation("An input contract has been archived by a concurrent transaction submission.")
    @Resolution(
      "The correct resolution depends on the business flow, for example it may be possible to " +
        "proceed without the archived contract as an input, or a different contract could be used."
    )
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

  @Explanation("Errors that relate to parties.")
  object Parties extends ErrorGroup {

    @Explanation("The submitting party has not been allocated.")
    @Resolution(
      "Check that the party identifier is correct, allocate the submitting party, " +
        "request its allocation or wait for it to be allocated before retrying the transaction submission."
    )
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
  }

  @Explanation("Errors that relate to system resources.")
  object Resources extends ErrorGroup() {
    @Explanation("A system resource has been exhausted.")
    @Resolution(
      "Retry the transaction submission or provide the details to the participant operator."
    )
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

  @Explanation("Errors that arise from an internal system misbehavior.")
  object Internal extends ErrorGroup() {

    @Explanation("A rejection reason has not been set.")
    @Resolution("Contact support.")
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

    @Explanation("An invalid transaction submission was not detected by the participant.")
    @Resolution("Contact support.")
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

    @Explanation("The participant didn't provide a necessary transaction submission input.")
    @Resolution("Contact support.")
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

    @Explanation(
      "The participant didn't detect an attempt by the transaction submission " +
        "to use the same key for two active contracts."
    )
    @Resolution("Contact support.")
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

    @Explanation(
      "The participant didn't detect an attempt by the transaction submission " +
        "to use a stale contract key."
    )
    @Resolution("Contact support.")
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

    @Explanation("An invalid participant state has been detected.")
    @Resolution("Contact support.")
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
  }
}
