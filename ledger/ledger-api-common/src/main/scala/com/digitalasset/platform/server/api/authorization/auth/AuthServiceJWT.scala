// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.authorization.auth

import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.daml.ledger.participant.state.v1.{AuthService, Claim, ClaimActAsParty, ClaimAdmin, ClaimPublic, Claims}
import com.digitalasset.daml.lf.data.Ref
import io.grpc.Metadata
import org.slf4j.{Logger, LoggerFactory}
import spray.json._

import scala.collection.mutable.ListBuffer
import scala.util.Try

/** The JWT token payload used in [[AuthServiceJWT]]
  *
  * TODO(RA): Do we need to wrap everything in a "com.daml.ledger.api" field,
  * to prevent name collisions with other JWT fields?
  *
  * For forward/backward compatibility reasons, all fields are optional.
  */
case class AuthServiceJWTPayload(
  ledgerId: Option[String],
  applicationId: Option[String],
  admin: Option[Boolean],
  actAsParty: Option[List[String]]
)

object AuthServiceJWTProtocol extends DefaultJsonProtocol {
  implicit val jwtPayloadFormat = jsonFormat4(AuthServiceJWTPayload.apply)
}

/** An AuthService that reads a JWT token from a `Authorization: Bearer` HTTP header.
  * The token is expected to use the format as defined in [[AuthServiceJWTPayload]]:
  */
class AuthServiceJWT() extends AuthService {

  protected val logger: Logger = LoggerFactory.getLogger(AuthServiceJWT.getClass)

  override def decodeMetadata(headers: Metadata): CompletionStage[Claims] = {
    val tokenE = for {
      headerValue <- Option.apply(headers.get(AuthServiceJWT.AUTHORIZATION_KEY))
        .toRight(new RuntimeException("Authorization header not found"))
      token <- AuthServiceJWT.decodeAndParse(headerValue)
    } yield token

    tokenE.fold(
      error => {
        logger.warn("Authorization error:", error)
        CompletableFuture.completedFuture(Claims.empty)
      },
      token => CompletableFuture.completedFuture(AuthServiceJWT.payloadToClaims(token))
    )
  }
}

object AuthServiceJWT {
  val AUTHORIZATION_KEY: Metadata.Key[String] =
    Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)

  def apply() = new AuthServiceJWT()

  def parsePayload(jwtPayload: String): Either[Throwable, AuthServiceJWTPayload] = {
    import AuthServiceJWTProtocol._
    Try(JsonParser(jwtPayload).convertTo[AuthServiceJWTPayload]).toEither
  }

  def decodeAndParse(headerValue: String): Either[Throwable, AuthServiceJWTPayload] = {
    val bearerTokenRegex = "[Bb]earer: (.*)".r
    for {
      token <- bearerTokenRegex.findFirstIn(headerValue)
        .toRight(new RuntimeException("Authorization header does not use Bearer format"))
      decoded <- com.digitalasset.jwt.JwtDecoder.decode(com.digitalasset.jwt.domain.Jwt(token))
        .toEither
        .left.map(e => new RuntimeException("Could not decode JWT token: " + e.message))
      parsed <- parsePayload(decoded.payload)
        .left.map(e => new RuntimeException("Could not parse JWT token: " + e))
    } yield parsed
  }

  def payloadToClaims(payload: AuthServiceJWTPayload): Claims = {
    val claims = ListBuffer[Claim]()

    // Any valid token authorizes the user to use public services
    claims.append(ClaimPublic)

    if (payload.admin.getOrElse(false))
      claims.append(ClaimAdmin)

    payload.actAsParty
      .getOrElse(List.empty)
      .foreach(party => claims.append(ClaimActAsParty(Ref.Party.assertFromString(party))))
    Claims(claims.toList)
  }
}