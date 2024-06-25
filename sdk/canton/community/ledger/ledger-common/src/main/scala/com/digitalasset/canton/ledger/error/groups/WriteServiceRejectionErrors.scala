// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error.groups

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
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.canton.ledger.error.ParticipantErrorGroup.LedgerApiErrorGroup.WriteServiceRejectionErrorGroup

@Explanation(
  "Generic submission rejection errors returned by the backing ledger's write service."
)
object WriteServiceRejectionErrors extends WriteServiceRejectionErrorGroup {
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
    final case class Reject(
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
    final case class Reject(parties: Set[String])(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = s"Parties not known on ledger: ${parties
            .mkString("[", ",", "]")}") {
      override def resources: Seq[(ErrorResource, String)] =
        parties.map((ErrorResource.Party, _)).toSeq
    }
  }

  @Explanation("A submitting party is not authorized to act through the participant.")
  @Resolution("Contact the participant operator or re-submit with an authorized party.")
  object SubmitterCannotActViaParticipant
      extends ErrorCode(
        id = "SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT",
        ErrorCategory.InsufficientPermission,
      ) {
    final case class RejectWithSubmitterAndParticipantId(
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

    final case class Reject(
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
      final case class Reject(override val cause: String, keyO: Option[GlobalKey] = None)(implicit
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
      final case class Reject(override val cause: String, keyO: Option[GlobalKey] = None)(implicit
          loggingContext: ContextualizedErrorLogger
      ) extends DamlErrorWithDefiniteAnswer(cause = cause) {
        override def resources: Seq[(ErrorResource, String)] =
          super.resources ++ keyO.map(key => ErrorResource.ContractKey -> key.toString).toList
      }
    }
  }
}
