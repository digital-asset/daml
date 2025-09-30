// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool

import com.daml.grpc.AuthCallCredentials
import com.daml.jwt.{
  AuthServiceJWTCodec,
  DecodedJwt,
  JwtSigner,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import io.grpc.stub.AbstractStub

object AuthorizationHelper {
  def maybeAuthedService[T <: AbstractStub[T]](userTokenO: Option[String])(service: T): T =
    userTokenO.fold(service)(token => AuthCallCredentials.authorizingStub(service, token))
}

class AuthorizationHelper(val authorizationTokenSecret: String) {

  /** @return
    *   user token signed with HMAC256
    */
  def tokenFor(userId: String): String = {
    val payload = StandardJWTPayload(
      issuer = None,
      participantId = None,
      userId = userId,
      exp = None,
      format = StandardJWTTokenFormat.Scope,
      audiences = List.empty,
      scope = None,
    )
    JwtSigner.HMAC256
      .sign(
        jwt = DecodedJwt(
          header = """{"alg": "HS256", "typ": "JWT"}""",
          payload = AuthServiceJWTCodec.compactPrint(payload),
        ),
        secret = authorizationTokenSecret,
      )
      .getOrElse(sys.error("Failed to generate token"))
      .value
  }

}
