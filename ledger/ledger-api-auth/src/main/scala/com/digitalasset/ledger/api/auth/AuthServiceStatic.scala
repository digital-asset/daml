// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import java.util.concurrent.{CompletableFuture, CompletionStage}

import io.grpc.Metadata

/** An AuthService that matches the value of the `Authorization` HTTP header against
  * a static map of header values to [[Claims]].
  *
  * Note: This AuthService is meant to be used for testing purposes only.
  */
final class AuthServiceStatic(claims: PartialFunction[String, Claims]) extends AuthService {
  override def decodeMetadata(headers: Metadata): CompletionStage[Claims] = {
    if (headers.containsKey(AUTHORIZATION_KEY)) {
      val authorizationValue = headers.get(AUTHORIZATION_KEY).stripPrefix("Bearer ")
      CompletableFuture.completedFuture(claims.lift(authorizationValue).getOrElse(Claims.empty))
    } else {
      CompletableFuture.completedFuture(Claims.empty)
    }
  }
}

object AuthServiceStatic {
  def apply(claims: PartialFunction[String, Claims]) = new AuthServiceStatic(claims)
}
