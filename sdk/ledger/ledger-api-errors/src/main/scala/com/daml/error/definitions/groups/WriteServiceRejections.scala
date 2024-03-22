// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  ErrorResource,
  Explanation,
  Resolution,
}

@Explanation(
  "Generic submission rejection errors returned by the backing ledger's write service."
)
object WriteServiceRejections extends LedgerApiErrors.WriteServiceRejections {

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

}
