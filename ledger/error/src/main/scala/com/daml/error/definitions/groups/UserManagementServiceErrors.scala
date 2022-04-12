// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.definitions.DamlErrorWithDefiniteAnswer
import com.daml.error.{
  ContextualizedErrorLogger,
  ErrorCategory,
  ErrorCode,
  ErrorResource,
  Explanation,
  Resolution,
}

object UserManagementServiceErrors extends AdminServices.UserManagementServiceErrorGroup {

  @Explanation("The user referred to by the request was not found.")
  @Resolution(
    "Check that you are connecting to the right participant node and the user-id is spelled correctly, if yes, create the user."
  )
  object UserNotFound
      extends ErrorCode(
        id = "USER_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    case class Reject(operation: String, userId: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"${operation} failed for unknown user \"${userId}\""
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.User -> userId
      )
    }
  }
  @Explanation("There already exists a user with the same user-id.")
  @Resolution(
    "Check that you are connecting to the right participant node and the user-id is spelled correctly, or use the user that already exists."
  )
  object UserAlreadyExists
      extends ErrorCode(
        id = "USER_ALREADY_EXISTS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    case class Reject(operation: String, userId: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"${operation} failed, as user \"${userId}\" already exists"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.User -> userId
      )
    }
  }

  @Explanation(
    """|A user can have only a limited number of user rights.
       |There was an attempt to create a user with too many rights or grant too many rights to a user."""
  )
  @Resolution(
    """|Retry with a smaller number of rights or delete some of the already existing rights of this user.
       |Contact the participant operator if the limit is too low."""
  )
  object TooManyUserRights
      extends ErrorCode(
        id = "TOO_MANY_USER_RIGHTS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Reject(operation: String, userId: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"${operation} failed, as user \"${userId}\" would have too many rights."
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.User -> userId
      )
    }
  }

}
