// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

object JwtDecoder extends WithExecuteUnsafe {
  def decode(jwt: Jwt): Either[Error, DecodedJwt[String]] =
    executeUnsafe(com.auth0.jwt.JWT.decode(jwt.value), Symbol("JwtDecoder.decode"))
      // TODO (i26199)- possible place to add expiration time check
      .map(a => DecodedJwt(header = a.getHeader, payload = a.getPayload))
      .flatMap(base64Decode)

  private def base64Decode(jwt: DecodedJwt[String]): Either[Error, DecodedJwt[String]] =
    jwt
      .transform(Base64.decode)
      .left
      .map(_.within(Symbol("JwtDecoder.base64Decode")))
}
