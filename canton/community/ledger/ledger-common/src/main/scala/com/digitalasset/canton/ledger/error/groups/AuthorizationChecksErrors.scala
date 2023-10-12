// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error.groups

import com.daml.error.{
  ContextualizedErrorLogger,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCategoryRetry,
  ErrorCode,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.ledger.error.ParticipantErrorGroup.LedgerApiErrorGroup.AuthorizationChecksErrorGroup

import scala.concurrent.duration.*

@Explanation("Authentication and authorization errors.")
object AuthorizationChecksErrors extends AuthorizationChecksErrorGroup {

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
    "Ask your participant operator to provide you with an appropriate JWT token."
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
}
