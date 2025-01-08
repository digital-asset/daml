// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.logging.ErrorLoggingContext

import scala.util.{Failure, Success, Try}

trait StatelessAuthorizer {
  def apply(claimSet: ClaimSet)(implicit errorLoggingContext: ErrorLoggingContext): Try[ClaimSet]
}

case object AdminAuthorizer extends StatelessAuthorizer {
  override def apply(
      claimSet: ClaimSet
  )(implicit errorLoggingContext: ErrorLoggingContext): Try[ClaimSet] =
    claimSet match {
      case ClaimSet.Unauthenticated =>
        Failure(AuthorizationChecksErrors.Unauthenticated.MissingJwtToken().asGrpcError)
      case _: ClaimSet.AuthenticatedUser =>
        Failure(
          AuthorizationChecksErrors.Unauthenticated.UserBasedAuthenticationIsDisabled().asGrpcError
        )
      case claimSet: ClaimSet.Claims =>
        if (claimSet.claims.contains(ClaimAdmin)) Success(claimSet)
        else
          Failure(
            AuthorizationChecksErrors.PermissionDenied.Reject("lacking admin claim").asGrpcError
          )
    }
}
