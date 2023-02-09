// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import com.daml.jwt.{Error, JwtFromBearerHeader, JwtVerifier, JwtVerifierBase}
import com.daml.ledger.api.auth.AuthService.AUTHORIZATION_KEY

import java.util.concurrent.{CompletableFuture, CompletionStage}
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain.IdentityProviderId
import io.grpc.Metadata
import org.slf4j.{Logger, LoggerFactory}
import spray.json._

import scala.collection.mutable.ListBuffer
import scala.util.Try

/** An AuthService that reads a JWT token from a `Authorization: Bearer` HTTP header.
  * The token is expected to use the format as defined in [[AuthServiceJWTPayload]]:
  */
class AuthServiceJWT(verifier: JwtVerifierBase, expectsAudienceBasedTokens: Boolean)
    extends AuthService {

  protected val logger: Logger = LoggerFactory.getLogger(AuthServiceJWT.getClass)

  override def decodeMetadata(headers: Metadata): CompletionStage[ClaimSet] =
    CompletableFuture.completedFuture {
      getAuthorizationHeader(headers) match {
        case None => ClaimSet.Unauthenticated
        case Some(header) => parseHeader(header)
      }
    }

  private[this] def getAuthorizationHeader(headers: Metadata): Option[String] =
    Option.apply(headers.get(AUTHORIZATION_KEY))

  private[this] def parseHeader(header: String): ClaimSet =
    parseJWTPayload(header).fold(
      error => {
        logger.warn("Authorization error: " + error.message)
        ClaimSet.Unauthenticated
      },
      token => payloadToClaims(token),
    )

  private[this] def parsePayload(jwtPayload: String): Either[Error, AuthServiceJWTPayload] =
    (if (expectsAudienceBasedTokens) {
       Try(parseAudienceBasedPayload(jwtPayload))
     } else {
       Try(parseOldPayload(jwtPayload))
     }).toEither.left.map(t =>
      Error(Symbol("parsePayload"), "Could not parse JWT token: " + t.getMessage)
    )

  private[this] def parseOldPayload(jwtPayload: String): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.JsonImplicits._
    JsonParser(jwtPayload).convertTo[AuthServiceJWTPayload]
  }

  private[this] def parseAudienceBasedPayload(
      jwtPayload: String
  ): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.AudienceBasedTokenJsonImplicits._
    JsonParser(jwtPayload).convertTo[AuthServiceJWTPayload]
  }

  private[this] def parseJWTPayload(header: String): Either[Error, AuthServiceJWTPayload] =
    for {
      token <- JwtFromBearerHeader(header)
      decoded <- verifier
        .verify(com.daml.jwt.domain.Jwt(token))
        .toEither
        .left
        .map(e => Error(Symbol("parseJWTPayload"), "Could not verify JWT token: " + e.message))
      parsed <- parsePayload(decoded.payload)
    } yield parsed

  private[this] def payloadToClaims: AuthServiceJWTPayload => ClaimSet = {
    case payload: CustomDamlJWTPayload =>
      val claims = ListBuffer[Claim]()

      // Any valid token authorizes the user to use public services
      claims.append(ClaimPublic)

      if (payload.admin)
        claims.append(ClaimAdmin)

      payload.actAs
        .foreach(party => claims.append(ClaimActAsParty(Ref.Party.assertFromString(party))))

      payload.readAs
        .foreach(party => claims.append(ClaimReadAsParty(Ref.Party.assertFromString(party))))

      ClaimSet.Claims(
        claims = claims.toList,
        ledgerId = payload.ledgerId,
        participantId = payload.participantId,
        applicationId = payload.applicationId,
        expiration = payload.exp,
        resolvedFromUser = false,
        identityProviderId = IdentityProviderId.Default,
        targetAudiences = Some(List.empty),
      )

    case payload: StandardJWTPayload =>
      ClaimSet.AuthenticatedUser(
        identityProviderId = IdentityProviderId.Default,
        participantId = payload.participantId,
        userId = payload.userId,
        expiration = payload.exp,
        targetAudiences = payload.audiences,
      )
  }
}

object AuthServiceJWT {
  def apply(
      verifier: com.auth0.jwt.interfaces.JWTVerifier,
      expectsAudienceBasedTokens: Boolean,
  ): AuthServiceJWT =
    new AuthServiceJWT(new JwtVerifier(verifier), expectsAudienceBasedTokens)

  def apply(verifier: JwtVerifierBase, expectsAudienceBasedTokens: Boolean): AuthServiceJWT =
    new AuthServiceJWT(verifier, expectsAudienceBasedTokens)
}
