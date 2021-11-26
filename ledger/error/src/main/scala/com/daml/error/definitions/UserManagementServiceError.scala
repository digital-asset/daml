// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error._

trait UserManagementServiceError extends BaseError

abstract class LoggingUserManagementServiceError(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
)(implicit override val code: ErrorCode)
    extends BaseError.Impl(cause, throwableO)
    with UserManagementServiceError {
  final override def logOnCreation: Boolean = true
}

@Explanation(
  "FIXME."
)
object UserManagementServiceError extends LedgerApiErrors.UserManagementServiceErrorGroup {
  @Explanation("FIXME.")
  object Reading extends ErrorGroup { // FIXME name <-

    @Explanation(
      """FIXME."""
    )
    @Resolution("FIXME.")
    object UserNotFound
        extends ErrorCode(
          id = "USER_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {

      case class Reject(
          override val cause: String, // FIXME consider removing the cause
          userId: String,
      )(implicit val loggingContext: ContextualizedErrorLogger)
          extends LoggingUserManagementServiceError(
            cause = cause
          ) {
        override def resources: Seq[(ErrorResource, String)] = Seq(
          ErrorResource.User -> userId
        )
      }
    }

  }
}
