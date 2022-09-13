// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.{
  ContextualizedErrorLogger,
  ErrorCategory,
  ErrorCode,
  ErrorResource,
  Explanation,
  Resolution,
}
import com.daml.error.definitions.{DamlError, DamlErrorWithDefiniteAnswer}

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

  @Explanation("""|A party can have only a limited amount of annotations.
                  |There was an attempt to allocate or update a party with too many annotations.""")
  @Resolution(
    "Retry with a smaller number of annotations or delete some of the already existing annotations of this party."
  )
  object MaxPartyAnnotationsSizeExceeded
      extends ErrorCode(
        id = "MAX_PARTY_DETAILS_ANNOTATIONS_SIZE_EXCEEDED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Reject(party: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = s"Annotations size for party '$party' has been exceeded"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

  @Explanation(
    """|Concurrent updates to a party can be controlled by optionally supplying an update request with a resource version.
                  |A party's resource version can be obtained by reading the party on the Ledger API.
                  |There was attempt to update a party using a stale resource version indicating a different processes had updated the party earlier."""
  )
  @Resolution(
    """|Restart the party update procedure by reading this party details again to obtain its most recent state and
                 |in particular its most recent resource version."""
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
            s"Update operation for party '$party' failed due a concurrent update to the same party"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

  @Explanation("The party referred to by the request was not found.")
  @Resolution(
    "Check that you are connecting to the right participant node and the party is spelled correctly."
  )
  object PartyNotFound
      extends ErrorCode(
        id = "PARTY_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    case class Reject(operation: String, party: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"$operation failed for unknown party '$party'"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

  @Explanation(
    """|Each on-ledger party known to this participant node can a have a participant local metadata assigned to it.
               |The participant local information about a party referred to by this request was not found while it should have been found."""
  )
  @Resolution(
    "This error can indicate a problem with the server's storage or implementation."
  )
  object InternalPartyRecordNotFound
      extends ErrorCode(
        id = "INTERNAL_PARTY_RECORD_NOT_FOUND",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    case class Reject(operation: String, party: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"$operation failed as party record identified by party '$party' was not found"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

  @Explanation(
    """|Each on-ledger party known to this participant node can a have a participant local metadata assigned to it.
                  |The participant local information about a party referred to by this request was found while it should have been not found."""
  )
  @Resolution(
    "This error can indicate a problem with the server's storage or implementation."
  )
  object InternalPartyRecordAlreadyExists
      extends ErrorCode(
        id = "INTERNAL_PARTY_RECORD_ALREADY_EXISTS",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    case class Reject(operation: String, party: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"$operation failed as party record identified by party '$party' already exists"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

}
