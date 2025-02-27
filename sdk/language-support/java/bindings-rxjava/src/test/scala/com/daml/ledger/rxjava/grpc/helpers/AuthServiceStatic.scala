// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.digitalasset.canton.auth.AuthService.AUTHORIZATION_KEY
import com.digitalasset.canton.auth.{AuthService, ClaimSet}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Metadata

import scala.concurrent.Future

/** An AuthService that matches the value of the `Authorization` HTTP header against
  * a static map of header values to [[ClaimSet.Claims]].
  *
  * Note: This AuthService is meant to be used for testing purposes only.
  */
final class AuthServiceStatic(claims: PartialFunction[String, ClaimSet]) extends AuthService {
  override def decodeMetadata(headers: Metadata)(implicit
      traceContext: TraceContext
  ): Future[ClaimSet] = {
    if (headers.containsKey(AUTHORIZATION_KEY)) {
      val authorizationValue = headers.get(AUTHORIZATION_KEY).stripPrefix("Bearer ")
      Future.successful(
        claims.lift(authorizationValue).getOrElse(ClaimSet.Unauthenticated)
      )
    } else {
      Future.successful(ClaimSet.Unauthenticated)
    }
  }
}

object AuthServiceStatic {
  def apply(claims: PartialFunction[String, ClaimSet]) = new AuthServiceStatic(claims)
}
