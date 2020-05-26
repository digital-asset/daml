// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import scalaz.{Show, \/}
import scalaz.syntax.show._
import scalaz.syntax.traverse._

object JwtDecoder {
  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"JwtDecoder.Error: ${e.what}, ${e.message}")
  }

  def decode(jwt: domain.Jwt): Error \/ domain.DecodedJwt[String] = {
    \/.fromTryCatchNonFatal(com.auth0.jwt.JWT.decode(jwt.value))
      .bimap(
        e => Error('decode, e.getMessage),
        a => domain.DecodedJwt(header = a.getHeader, payload = a.getPayload)
      )
      .flatMap(base64Decode)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def base64Decode(jwt: domain.DecodedJwt[String]): Error \/ domain.DecodedJwt[String] =
    jwt.traverse(Base64.decode).leftMap(e => Error('base64Decode, e.shows))
}
