// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions.groups

import com.daml.error.definitions.{DamlError, DamlErrorWithDefiniteAnswer}
import com.daml.error.{
  ContextualizedErrorLogger,
  ErrorCategory,
  ErrorCode,
  ErrorResource,
  Explanation,
  Resolution,
}

object UserManagementServiceErrors extends AdminServices.UserManagementServiceErrorGroup {

  object InvalidUserUpdate
      extends ErrorCode(
        id = "INVALID_USER_UPDATE",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    case class Reject(userId: String, reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = s"Update operation for user id '${userId}' failed due to: $reason"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.User -> userId
      )
    }
  }

  object MaxAnnotationsSizeExceeded
      extends ErrorCode(
        id = "MAX_ANNOTATIONS_SIZE_EXCEEDED",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    case class Reject(userId: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause = s"Annotations size for $userId has been exceeded"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.User -> userId
      )
    }
  }

  object ConcurrentUserUpdateDetected
      extends ErrorCode(
        id = "CONCURRENT_USER_UPDATE_DETECTED",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    case class Reject(userId: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlError(
          cause =
            s"Update operation for user '${userId}' failed due a concurrent update to the same user"
        ) {
      override def resources: Seq[(ErrorResource, String)] = Seq(
        ErrorResource.User -> userId
      )
    }
  }

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

//  // TODO pbatko: Define an error than can be applied to both users and parties
//  @Explanation(
//    """|A user can have only a limited number of user rights.
//       |There was an attempt to create a user with too many rights or grant too many rights to a user."""
//  )
//  @Resolution(
//    """|Retry with a smaller number of rights or delete some of the already existing rights of this user.
//       |Contact the participant operator if the limit is too low."""
//  )
//  object TooManyAnnotations
//    extends ErrorCode(
//      id = "TOO_MANY_ANNOTATIONS",
//      ErrorCategory.InvalidGivenCurrentSystemStateOther,
//    ) {
//    case class Reject(operation: String, userId: String)(implicit
//                                                         loggingContext: ContextualizedErrorLogger
//    ) extends DamlErrorWithDefiniteAnswer(
//      cause = s"${operation} failed, as user \"${userId}\" would have too many annotations."
//    ) {
//      override def resources: Seq[(ErrorResource, String)] = Seq(
//        ErrorResource.User -> userId
//      )
//    }
//  }

}
