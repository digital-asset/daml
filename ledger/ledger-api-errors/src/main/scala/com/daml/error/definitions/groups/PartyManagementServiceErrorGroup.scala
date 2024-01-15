// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.{
  ContextualizedErrorLogger,
  DamlError,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  ErrorResource,
  Explanation,
  Resolution,
}

object PartyManagementServiceErrorGroup extends AdminServices.PartyManagementServiceErrorGroup {

  @Explanation("There was an attempt to update a party using an invalid update request.")
  @Resolution(
    """|Inspect the error details for specific information on what made the request invalid.
       |Retry with an adjusted update request."""
  )
  object InvalidUpdatePartyDetailsRequest
      extends ErrorCode(
        id = "INVALID_PARTY_DETAILS_UPDATE_REQUEST",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    case class Reject(party: String, reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = s"Update operation for party '$party' failed due to: $reason"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

  @Explanation(
    """|Concurrent updates to a party can be controlled by supplying an update request with a resource version (this is optional).
                  |A party's resource version can be obtained by reading the party on the Ledger API.
                  |There was attempt to update a party using a stale resource version, indicating that a different process had updated the party earlier."""
  )
  @Resolution(
    """|Read this party again to obtain its most recent state and
                  |in particular its most recent resource version. Use the obtained information to build and send a new update request."""
  )
  object ConcurrentPartyDetailsUpdateDetected
      extends ErrorCode(
        id = "CONCURRENT_PARTY_DETAILS_UPDATE_DETECTED",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    case class Reject(party: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause =
            s"Update operation for party '$party' failed due to a concurrent update to the same party"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

  @Explanation("The party referred to by the request was not found.")
  @Resolution(
    "Check that you are connecting to the right participant node and that the party is spelled correctly."
  )
  object PartyNotFound
      extends ErrorCode(
        id = "PARTY_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    case class Reject(operation: String, party: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"Party: '$party' was not found when $operation"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

}
