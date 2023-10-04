// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.definitions.{ChangeId, LedgerApiErrors}
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  ErrorGroup,
  ErrorResource,
  Explanation,
  Resolution,
}
import com.daml.lf.value.Value

@Explanation(
  "Potential consistency errors raised due to race conditions during command submission or returned as submission rejections by the backing ledger."
)
object ConsistencyErrors extends ErrorGroup()(LedgerApiErrors.errorClass) {

  @Explanation("A command with the given command id has already been successfully processed.")
  @Resolution(
    """The correct resolution depends on the use case. If the error received pertains to a submission retried due to a timeout,
      |do nothing, as the previous command has already been accepted.
      |If the intent is to submit a new command, re-submit using a distinct command id.
      |"""
  )
  object DuplicateCommand
      extends ErrorCode(
        id = "DUPLICATE_COMMAND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {

    final case class Reject(
        override val definiteAnswer: Boolean = false,
        existingCommandSubmissionId: Option[String],
        changeId: Option[ChangeId] = None,
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = "A command with the given command id has already been successfully processed",
          definiteAnswer = definiteAnswer,
        ) {
      override def context: Map[String, String] =
        super.context ++ existingCommandSubmissionId
          .map("existing_submission_id" -> _)
          .toList ++ changeId
          .map(changeId => Seq("changeId" -> changeId.toString))
          .getOrElse(Seq.empty)
    }
  }

  @Explanation(
    """This error occurs if the Daml engine can not find a referenced contract. This
      |can be caused by either the contract not being known to the participant, or not being known to
      |the submitting parties or already being archived."""
  )
  @Resolution("This error type occurs if there is contention on a contract.")
  object ContractNotFound
      extends ErrorCode(
        id = "CONTRACT_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {

    final case class Reject(
        override val cause: String,
        cid: Value.ContractId,
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = cause
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        (ErrorResource.ContractId, cid.coid)
      )
    }

  }

  @Explanation(
    "An input contract key was re-assigned to a different contract by a concurrent transaction submission."
  )
  @Resolution("Retry the transaction submission.")
  object InconsistentContractKey
      extends ErrorCode(
        id = "INCONSISTENT_CONTRACT_KEY",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Reject(reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = reason)

  }

  @Explanation(
    """This error signals that within the transaction we got to a point where two contracts with the same key were active."""
  )
  @Resolution("This error indicates an application error.")
  object DuplicateContractKey
      extends ErrorCode(
        id = "DUPLICATE_CONTRACT_KEY",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {

    final case class Reject(override val cause: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = cause)

  }

  @Explanation(
    "Another command submission with the same change ID (application ID, command ID, actAs) is already being processed."
  )
  @Resolution(
    """Listen to the command completion stream until a completion for the in-flight command submission is published.
      |Alternatively, resubmit the command. If the in-flight submission has finished successfully by then,
      |this will return more detailed information about the earlier one.
      |If the in-flight submission has failed by then, the resubmission will attempt to record the new transaction on the ledger.
      |"""
  )
  // This command deduplication error is currently used only by Canton.
  // It is defined here so that the general command deduplication documentation can refer to it.
  object SubmissionAlreadyInFlight
      extends ErrorCode(
        id = "SUBMISSION_ALREADY_IN_FLIGHT",
        ErrorCategory.ContentionOnSharedResources,
      )
}
