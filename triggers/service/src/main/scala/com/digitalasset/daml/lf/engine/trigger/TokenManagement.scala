// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.HttpRequest

import com.daml.ledger.api.auth.{AuthServiceJWTCodec}
import com.daml.jwt.domain.Jwt
import com.daml.jwt.{JwtDecoder}
import com.daml.jwt.domain.{DecodedJwt}

import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.{\/, -\/}

case class Unauthorized(message: String) extends Error(message)
case class JwtPayload(ledgerId: String, applicationId: String, party: String)

object TokenManagement {

  def decodeJwt(jwt: Jwt): Unauthorized \/ DecodedJwt[String] = {
    JwtDecoder.decode(jwt).leftMap(e => Unauthorized(e.shows))
  }

  def findJwt(req: HttpRequest): Unauthorized \/ Jwt = {
    req.headers
      .collectFirst {
        case Authorization(OAuth2BearerToken(token)) => Jwt(token)
      }
      .toRightDisjunction(Unauthorized("missing Authorization header with OAuth 2.0 Bearer Token"))
  }

  def decodeAndParsePayload(jwt: Jwt, decodeJwt: Jwt => Unauthorized \/ DecodedJwt[String])
    : Unauthorized \/ (jwt.type, JwtPayload) =
    for {
      a <- decodeJwt(jwt)
      p <- parsePayload(a)
    } yield (jwt, p)

  def parsePayload(jwt: DecodedJwt[String]): Unauthorized \/ JwtPayload = {
    // AuthServiceJWTCodec is the JWT reader used by the sandbox and
    // some DAML-on-X ledgers. Most JWT fields are optional for the
    // sandbox, but not for the trigger service (exactly as the case
    // as for the http-json serive).
    AuthServiceJWTCodec
      .readFromString(jwt.payload)
      .fold(
        e => -\/(Unauthorized(e.getMessage)),
        payload =>
          for {
            ledgerId <- payload.ledgerId.toRightDisjunction(
              Unauthorized("ledgerId missing in access token"))
            applicationId <- payload.applicationId.toRightDisjunction(
              Unauthorized("applicationId missing in access token"))
            party <- payload.party.toRightDisjunction(
              Unauthorized("party missing or not unique in access token"))
          } yield JwtPayload(ledgerId, applicationId, party)
      )
  }
}
