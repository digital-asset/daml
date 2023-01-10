// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.definitions.{DamlErrorWithDefiniteAnswer, LedgerApiErrors}
import com.daml.error.{
  ContextualizedErrorLogger,
  ErrorCategory,
  ErrorCode,
  ErrorGroup,
  ErrorResource,
  Explanation,
  Resolution,
}
import com.daml.lf.transaction.GlobalKey

@Explanation(
  "Generic submission rejection errors returned by the backing ledger's write service."
)
object WriteServiceRejections extends LedgerApiErrors.WriteServiceRejections {
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
        submitterParty: String
    )(implicit loggingContext: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          cause = s"Party not known on ledger: Submitting party '$submitterParty' not known"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> submitterParty
      )
    }
  }

  @Explanation("One or more informee parties have not been allocated.")
  @Resolution(
    "Check that all the informee party identifiers are correct, allocate all the informee parties, " +
      "request their allocation or wait for them to be allocated before retrying the transaction submission."
  )
  object PartyNotKnownOnLedger
      extends ErrorCode(
        id = "PARTY_NOT_KNOWN_ON_LEDGER",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    case class Reject(parties: Set[String])(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = s"Parties not known on ledger: ${parties
            .mkString("[", ",", "]")}") {
      override def resources: Seq[(ErrorResource, String)] =
        parties.map((ErrorResource.Party, _)).toSeq
    }

    @deprecated(
      "Corresponds to transaction submission rejections that are not produced anymore.",
      since = "1.18.0",
    )
    case class RejectDeprecated(
        description: String
    )(implicit loggingContext: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          cause = s"Party not known on ledger: $description"
        )
  }

  @Explanation("An invalid transaction submission was not detected by the participant.")
  @Resolution("Contact support.")
  @deprecated(
    "Corresponds to transaction submission rejections that are not produced anymore.",
    since = "1.18.0",
  )
  object Disputed
      extends ErrorCode(
        id = "DISPUTED",
        ErrorCategory.SystemInternalAssumptionViolated, // It should have been caught by the participant
      ) {
    case class Reject(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          cause = s"Disputed: $details"
        )
  }

  @Explanation(
    "The Participant node did not have sufficient resource quota to submit the transaction."
  )
  @Resolution(
    "Inspect the error message and retry after after correcting the underlying issue."
  )
  @deprecated(
    "Corresponds to transaction submission rejections that are not produced anymore.",
    since = "1.18.0",
  )
  object OutOfQuota
      extends ErrorCode(id = "OUT_OF_QUOTA", ErrorCategory.ContentionOnSharedResources) {
    case class Reject(reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = reason)
  }

  @Explanation("A submitting party is not authorized to act through the participant.")
  @Resolution("Contact the participant operator or re-submit with an authorized party.")
  object SubmitterCannotActViaParticipant
      extends ErrorCode(
        id = "SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT",
        ErrorCategory.InsufficientPermission,
      ) {
    case class RejectWithSubmitterAndParticipantId(
        details: String,
        submitter: String,
        participantId: String,
    )(implicit loggingContext: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          cause = s"Inconsistent: $details",
          extraContext = Map(
            "submitter" -> submitter,
            "participantId" -> participantId,
          ),
        )

    case class Reject(
        details: String
    )(implicit loggingContext: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(cause = s"Inconsistent: $details")
  }

  @Explanation("Errors that arise from an internal system misbehavior.")
  object Internal extends ErrorGroup() {
    @Explanation(
      "The participant didn't detect an inconsistent key usage in the transaction. " +
        "Within the transaction, an exercise, fetch or lookupByKey failed because " +
        "the mapping of `key -> contract ID` was inconsistent with earlier actions."
    )
    @Resolution("Contact support.")
    object InternallyInconsistentKeys
        extends ErrorCode(
          id = "INTERNALLY_INCONSISTENT_KEYS",
          ErrorCategory.SystemInternalAssumptionViolated, // Should have been caught by the participant
        ) {
      case class Reject(override val cause: String, keyO: Option[GlobalKey] = None)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = cause) {
        override def resources: Seq[(ErrorResource, String)] =
          super.resources ++ keyO.map(key => ErrorResource.ContractKey -> key.toString).toList
      }
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
      case class Reject(override val cause: String, keyO: Option[GlobalKey] = None)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = cause) {
        override def resources: Seq[(ErrorResource, String)] =
          super.resources ++ keyO.map(key => ErrorResource.ContractKey -> key.toString).toList
      }
    }
  }
}
