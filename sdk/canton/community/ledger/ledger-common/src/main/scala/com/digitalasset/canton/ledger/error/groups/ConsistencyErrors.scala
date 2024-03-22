// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error.groups

import com.daml.error.{
  ContextualizedErrorLogger,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  ErrorResource,
  Explanation,
  Resolution,
}
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.digitalasset.canton.ledger.error.ParticipantErrorGroup.LedgerApiErrorGroup.ConsistencyErrorGroup
import com.digitalasset.canton.ledger.participant.state.v2.ChangeId

import java.time.Instant

@Explanation(
  "Potential consistency errors raised due to race conditions during command submission or returned as submission rejections by the backing ledger."
)
object ConsistencyErrors extends ConsistencyErrorGroup {

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

  @Explanation("An input contract has been archived by a concurrent transaction submission.")
  @Resolution(
    "The correct resolution depends on the business flow, for example it may be possible to " +
      "proceed without the archived contract as an input, or a different contract could be used."
  )
  object InconsistentContracts
      extends ErrorCode(
        id = "INCONSISTENT_CONTRACTS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Reject(override val cause: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = cause)

  }

  @Explanation("At least one input has been altered by a concurrent transaction submission.")
  @Resolution(
    "The correct resolution depends on the business flow, for example it may be possible to proceed " +
      "without an archived contract as an input, or the transaction submission may be retried " +
      "to load the up-to-date value of a contract key."
  )
  object Inconsistent
      extends ErrorCode(
        id = "INCONSISTENT",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Reject(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          cause = s"Inconsistent: $details"
        )

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

    final case class RejectWithContractKeyArg(
        override val cause: String,
        key: GlobalKey,
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = cause
        ) {
      override def resources: Seq[(ErrorResource, String)] =
        CommandExecutionErrors.withEncodedValue(key.key) { encodedKey =>
          Seq(
            // TODO(i12763): Reconsider the transport format for the contract key.
            //                   If the key is big, it can force chunking other resources.
            (ErrorResource.TemplateId, key.templateId.toString),
            (ErrorResource.ContractKey, encodedKey),
          )
        }
    }

    final case class Reject(reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = reason)

  }

  @Explanation(
    """This error occurs if the disclosed payload or metadata of one of the contracts
      |does not match the actual payload or metadata of the contract."""
  )
  @Resolution("Re-submit the command using valid disclosed contract payload and metadata.")
  object DisclosedContractInvalid
      extends ErrorCode(
        id = "DISCLOSED_CONTRACT_INVALID",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Reject(cid: Value.ContractId)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"Invalid disclosed contract: ${cid.coid}"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        (ErrorResource.ContractId, cid.coid)
      )
    }
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

    final case class RejectWithContractKeyArg(
        override val cause: String,
        key: GlobalKey,
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = cause
        ) {
      override def resources: Seq[(ErrorResource, String)] =
        CommandExecutionErrors.withEncodedValue(key.key) { encodedKey =>
          Seq(
            // TODO(i12763): Reconsider the transport format for the contract key.
            //                   If the key is big, it can force chunking other resources.
            (ErrorResource.TemplateId, key.templateId.toString),
            (ErrorResource.ContractKey, encodedKey),
          )
        }
    }

    final case class Reject(override val cause: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = cause)

  }

  @Explanation(
    "The ledger time of the submission violated some constraint on the ledger time."
  )
  @Resolution("Retry the transaction submission.")
  object InvalidLedgerTime
      extends ErrorCode(
        id = "INVALID_LEDGER_TIME",
        ErrorCategory.InvalidGivenCurrentSystemStateOther, // It may succeed at a later time
      ) {

    final case class RejectEnriched(
        override val cause: String,
        ledgerTime: Instant,
        ledgerTimeLowerBound: Instant,
        ledgerTimeUpperBound: Instant,
    )(implicit loggingContext: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(cause = cause) {
      override def context: Map[String, String] = super.context ++ Map(
        "ledger_time" -> ledgerTime.toString,
        "ledger_time_lower_bound" -> ledgerTimeLowerBound.toString,
        "ledger_time_upper_bound" -> ledgerTimeUpperBound.toString,
      )
    }

    final case class RejectSimple(
        override val cause: String
    )(implicit loggingContext: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(cause = cause)

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
