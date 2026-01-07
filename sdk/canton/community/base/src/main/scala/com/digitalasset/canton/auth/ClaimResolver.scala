// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.logging.{ErrorLoggingContext, LoggingContextWithTrace}

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait ClaimResolver {
  def apply(claimSet: ClaimSet)(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[ClaimSet]
}

case object RequiringAdminClaimResolver extends ClaimResolver {
  override def apply(
      claimSet: ClaimSet
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[ClaimSet] =
    Future.fromTry(
      claimSet match {
        case ClaimSet.Unauthenticated =>
          Failure(AuthorizationChecksErrors.Unauthenticated.MissingJwtToken().asGrpcError)
        case _: ClaimSet.AuthenticatedUser =>
          Failure(
            AuthorizationChecksErrors.Unauthenticated
              .UserBasedAuthenticationIsDisabled()
              .asGrpcError
          )
        case claimSet: ClaimSet.Claims =>
          if (claimSet.claims.contains(ClaimAdmin)) Success(claimSet)
          else
            Failure(
              AuthorizationChecksErrors.PermissionDenied.Reject("lacking admin claim").asGrpcError
            )
      }
    )
}
