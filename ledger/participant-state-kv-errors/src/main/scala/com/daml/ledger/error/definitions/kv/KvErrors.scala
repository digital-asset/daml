// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.error.definitions.kv

import com.daml.error.{ContextualizedErrorLogger, DamlErrorWithDefiniteAnswer, ErrorCategory, ErrorCode, ErrorGroup, ErrorGroups, Explanation, Resolution}
import java.time.Instant

@Explanation(
  "Errors that are specific to ledgers based on the KV architecture: Daml Sandbox and VMBC."
)
object KvErrors extends ErrorGroup()(ErrorGroups.rootErrorClass) {

  @Explanation("Errors that highlight transaction consistency issues in the committer context.")
  object Consistency extends ErrorGroup() {

    @Explanation("Validation of a transaction submission failed using on-ledger data.")
    @Resolution("Either some input contracts have been pruned or the participant is misbehaving.")
    object ValidationFailure
        extends ErrorCode(
          id = "VALIDATION_FAILURE",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      case class Reject(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Validation failure: $details"
          )
    }

    @Explanation("At least one input has been altered by a concurrent transaction submission.")
    @Resolution(
      "The correct resolution depends on the business flow, for example it may be possible to proceed " +
        "without an archived contract as an input, or the transaction submission may be retried " +
        "to load the up-to-date value of a contract key."
    )
    object PostExecutionConflicts
        extends ErrorCode(
          id = "INCONSISTENT_INPUT",
          ErrorCategory.ContentionOnSharedResources,
        ) {

      case class Reject(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends DamlErrorWithDefiniteAnswer(
            cause = s"Inconsistent: $details"
          )

    }

  }

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
          minimumRecordTime: Instant,
          maximumRecordTime: Instant,
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause =
              s"Invalid ledger time: Record time is outside of valid range [$minimumRecordTime, $maximumRecordTime]"
          ) {
        override def context: Map[String, String] = Map(
          "minimum_record_time" -> minimumRecordTime.toString,
          "maximum_record_time" -> maximumRecordTime.toString,
        )
      }
    }

    @Explanation(
      "The record time is not within bounds for reasons such as " +
        "excessive latency, excessive clock skew between the participant and the committer " +
        "or a time model that is too restrictive."
    )
    @Resolution("Retry the submission or contact the participant operator.")
    object RecordTimeOutOfBounds
        extends ErrorCode(
          id = "RECORD_TIME_OUT_OF_BOUNDS",
          ErrorCategory.ContentionOnSharedResources, // It may succeed at a later time
        ) {
      case class Reject(
          override val cause: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(cause)
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

  @Explanation("Errors that relate to system resources.")
  object Resources extends ErrorGroup() {

    @Explanation("A system resource has been exhausted.")
    @Resolution(
      "Retry the transaction submission or provide the details to the participant operator."
    )
    @deprecated(since = "2.3.0", message = "Replaced by RESOURCE_OVERLOADED.")
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

    @Explanation("A system resource is overloaded.")
    @Resolution(
      "Retry the transaction submission or provide the details to the participant operator."
    )
    object ResourceOverloaded
        extends ErrorCode(
          id = "RESOURCE_OVERLOADED",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      case class Reject(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Resources overloaded: $details"
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

    @Explanation("An unexpected error occurred while submitting a command to the ledger.")
    @Resolution("Contact support.")
    object SubmissionFailed
        extends ErrorCode(
          id = "SUBMISSION_FAILED",
          ErrorCategory.SystemInternalAssumptionViolated,
        ) {
      case class Reject(
          details: String
      )(implicit loggingContext: ContextualizedErrorLogger)
          extends KVLoggingTransactionErrorImpl(
            cause = s"Submission failure: $details"
          )
    }
  }
}
