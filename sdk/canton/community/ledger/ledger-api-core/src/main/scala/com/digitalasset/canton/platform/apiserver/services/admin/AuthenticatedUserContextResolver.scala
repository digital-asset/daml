// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.digitalasset.canton.auth.ClaimSet.Claims
import com.digitalasset.canton.auth.{AuthInterceptor, ClaimAdmin, ClaimIdentityProviderAdmin}
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.logging.ErrorLoggingContext

import scala.concurrent.Future

trait AuthenticatedUserContextResolver {
  import AuthenticatedUserContextResolver.*
  def resolveAuthenticatedUserContext(implicit
      errorLogger: ErrorLoggingContext
  ): Future[AuthenticatedUserContext] =
    AuthInterceptor
      .extractClaimSetFromContext()
      .fold(
        fa = error =>
          Future.failed(
            LedgerApiErrors.InternalError
              .Generic("Could not extract a claim set from the context", throwableO = Some(error))
              .asGrpcError
          ),
        fb = {
          case claims: Claims =>
            Future.successful(AuthenticatedUserContext(claims))
          case claimsSet =>
            Future.failed(
              LedgerApiErrors.InternalError
                .Generic(
                  s"Unexpected claims when trying to resolve the authenticated user: $claimsSet"
                )
                .asGrpcError
            )
        },
      )
}

object AuthenticatedUserContextResolver {
  final case class AuthenticatedUserContext(claims: Claims) {
    def userId: Option[String] = if (claims.resolvedFromUser) claims.userId else None
    def isParticipantAdmin: Boolean = claims.claims.contains(ClaimAdmin)
    def isIdpAdmin: Boolean = claims.claims.contains(ClaimIdentityProviderAdmin)
    def isRegularUser: Boolean = claims.resolvedFromUser && !isParticipantAdmin && !isIdpAdmin
  }
}
