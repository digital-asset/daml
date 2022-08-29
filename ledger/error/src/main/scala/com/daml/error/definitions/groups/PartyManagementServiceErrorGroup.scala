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

  object ConcurrentParticipantPartyUpdateDetected
      extends ErrorCode(
        id = "CONCURRENT_PARTICIPANT_PARTY_UPDATE_DETECTED",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    case class Reject(party: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause =
            s"Update operation for party '${party}' failed due a concurrent update to the same party"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.User -> party
      )
    }
  }

  @Explanation("The party referred to by the request was not found.")
  @Resolution(
    "Check that you are connecting to the right participant node and the party is spelled correctly."
  )
  object ParticipantPartyNotFound
      extends ErrorCode(
        id = "PARTICIPANT_PARTY_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    case class Reject(operation: String, party: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"${operation} failed for unknown user \"${party}\""
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }
  @Explanation("There already exists a party with the same party name.")
  @Resolution(
    "Check that you are connecting to the right participant node and the party is spelled correctly."
  )
  object ParticipantPartyAlreadyExists
      extends ErrorCode(
        id = "PARTICIPANT_PARTY_ALREADY_EXISTS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    case class Reject(operation: String, party: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"${operation} failed, as party \"${party}\" already exists"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.Party -> party
      )
    }
  }

}
