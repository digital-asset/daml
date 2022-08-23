// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.client.LedgerCallCredentials
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, StandardJWTPayload, StandardJWTTokenFormat}
import io.grpc.stub.AbstractStub

object AuthorizationHelper {
  def maybeAuthedService[T <: AbstractStub[T]](userTokenO: Option[String])(service: T): T = {
    userTokenO.fold(service)(token => LedgerCallCredentials.authenticatingStub(service, token))
  }
}

class AuthorizationHelper(val authorizationTokenSecret: String) {

  /** @return user token signed with HMAC256
    */
  def tokenFor(userId: String): String = {
    val payload = StandardJWTPayload(
      participantId = None,
      userId = userId,
      exp = None,
      format = StandardJWTTokenFormat.Scope,
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
