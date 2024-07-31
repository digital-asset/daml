// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.error.*
import com.digitalasset.canton.error.CantonErrorGroups

import scala.concurrent.duration.*

@Explanation("Authentication and authorization errors.")
object AuthorizationChecksErrors extends CantonErrorGroups.AuthorizationChecksErrorGroup {

  @Explanation("""The stream was aborted because the authenticated user's rights changed,
                 |and the user might thus no longer be authorized to this stream.
                 |""")
  @Resolution(
    "The application should automatically retry fetching the stream. It will either succeed, or fail with an explicit denial of authentication or permission."
  )
  object StaleUserManagementBasedStreamClaims
      extends ErrorCode(
        id = "STALE_STREAM_AUTHORIZATION",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Reject()(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer("Stale stream authorization. Retry quickly.") {
      override def retryable: Option[ErrorCategoryRetry] = Some(
        ErrorCategoryRetry(duration = 0.seconds)
      )
    }

  }

  @Explanation(
    """This rejection is given if the submitted command does not contain a JWT token on a participant enforcing JWT authentication."""
  )
  @Resolution(
    "Ask your participant operator or IDP provider to give you an appropriate JWT token."
  )
  object Unauthenticated
      extends ErrorCode(
        id = "UNAUTHENTICATED",
        ErrorCategory.AuthInterceptorInvalidAuthenticationCredentials,
      ) {
    final case class MissingJwtToken()(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = "The command is missing a (valid) JWT token"
        )

    final case class UserBasedAuthenticationIsDisabled()(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = "User based authentication is disabled."
        )
  }

  @Explanation(
    "This rejection is given when a valid JWT token used for some period of time expires."
  )
  @Resolution("Ask your participant operator or IDP provider to give you an appropriate JWT token.")
  object AccessTokenExpired
      extends ErrorCode(
        id = "ACCESS_TOKEN_EXPIRED",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Reject(override val cause: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = "JWT token has expired"
        )
  }

  @Explanation("An internal system authorization error occurred.")
  @Resolution("Contact the participant operator.")
  object InternalAuthorizationError
      extends ErrorCode(
        id = "INTERNAL_AUTHORIZATION_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Reject(message: String, throwable: Throwable)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = message,
          throwableO = Some(throwable),
        )
  }

  @Explanation(
    """This rejection is given if the supplied authorization token is not sufficient for the intended command.
      |The exact reason is logged on the participant, but not given to the user for security reasons."""
  )
  @Resolution(
    "Inspect your command and your token or ask your participant operator for an explanation why this command failed."
  )
  object PermissionDenied
      extends ErrorCode(id = "PERMISSION_DENIED", ErrorCategory.InsufficientPermission) {
    final case class Reject(override val cause: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The provided authorization token is not sufficient to authorize the intended command: $cause"
        )
  }

  @Explanation(
    """This error is emitted when a submitted ledger API command contains an invalid token."""
  )
  @Resolution("Inspect the reason given and correct your application.")
  object InvalidToken
      extends ErrorCode(id = "INVALID_TOKEN", ErrorCategory.InvalidIndependentOfSystemState) {
    final case class MissingUserId(reason: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"The submitted request is missing a user-id: $reason"
        )

    final case class InvalidField(fieldName: String, message: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The submitted token has a field with invalid value: Invalid field $fieldName: $message"
        )
  }
}
