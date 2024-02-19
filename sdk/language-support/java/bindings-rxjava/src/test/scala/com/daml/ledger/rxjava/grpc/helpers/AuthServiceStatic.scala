// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.digitalasset.canton.ledger.api.auth.AuthService.AUTHORIZATION_KEY
import com.digitalasset.canton.ledger.api.auth.{AuthService, ClaimSet}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Metadata

/** An AuthService that matches the value of the `Authorization` HTTP header against
  * a static map of header values to [[ClaimSet.Claims]].
  *
  * Note: This AuthService is meant to be used for testing purposes only.
  */
final class AuthServiceStatic(claims: PartialFunction[String, ClaimSet]) extends AuthService {
  override def decodeMetadata(headers: Metadata)(implicit
      traceContext: TraceContext
  ): CompletionStage[ClaimSet] = {
    if (headers.containsKey(AUTHORIZATION_KEY)) {
      val authorizationValue = headers.get(AUTHORIZATION_KEY).stripPrefix("Bearer ")
      CompletableFuture.completedFuture(
        claims.lift(authorizationValue).getOrElse(ClaimSet.Unauthenticated)
      )
    } else {
      CompletableFuture.completedFuture(ClaimSet.Unauthenticated)
    }
  }
}

object AuthServiceStatic {
  def apply(claims: PartialFunction[String, ClaimSet]) = new AuthServiceStatic(claims)
}
