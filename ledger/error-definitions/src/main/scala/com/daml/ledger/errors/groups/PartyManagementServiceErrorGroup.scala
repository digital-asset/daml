// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.errors.groups

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
    """|A party can have at most 256kb worth of annotations in total measured in number of bytes in UTF-8 encoding.
                  |There was an attempt to allocate or update a party such that this limit would have been exceeded."""
  )
  @Resolution(
    "Retry with fewer annotations or delete some of the party's existing annotations."
  )
  object MaxPartyAnnotationsSizeExceeded
      extends ErrorCode(
        id = "MAX_PARTY_DETAILS_ANNOTATIONS_SIZE_EXCEEDED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Reject(party: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = s"Maximum annotations size for party '$party' has been exceeded"
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

  @Explanation(
    """|Each on-ledger party known to this participant node can have a participant's local metadata assigned to it.
               |The local information about a party referred to by this request was not found when it should have been found."""
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
          cause = s"Party record for party: '$party' was not found when $operation"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

  @Explanation(
    """|Each on-ledger party known to this participant node can have a participant's local metadata assigned to it.
                  |The local information about a party referred to by this request was found when it should have been not found."""
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
          cause = s"Party record for party: '$party' already exists when $operation"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

}
