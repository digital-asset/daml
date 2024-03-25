// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.jwt.{Error, JwtFromBearerHeader, JwtVerifier, JwtVerifierBase}
import com.digitalasset.canton.ledger.api.auth.AuthService.AUTHORIZATION_KEY
import com.digitalasset.canton.ledger.api.domain.IdentityProviderId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Metadata
import spray.json.*

import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.util.Try

/** An AuthService that reads a JWT token from a `Authorization: Bearer` HTTP header.
  * The token is expected to use the format as defined in [[AuthServiceJWTPayload]]:
  */
class AuthServiceJWT(
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    val loggerFactory: NamedLoggerFactory,
) extends AuthService
    with NamedLogging {

  override def decodeMetadata(
      headers: Metadata
  )(implicit traceContext: TraceContext): CompletionStage[ClaimSet] =
    CompletableFuture.completedFuture {
      getAuthorizationHeader(headers) match {
        case None => ClaimSet.Unauthenticated
        case Some(header) => parseHeader(header)
      }
    }

  private[this] def getAuthorizationHeader(headers: Metadata): Option[String] =
    Option.apply(headers.get(AUTHORIZATION_KEY))

  private[this] def parseHeader(header: String)(implicit traceContext: TraceContext): ClaimSet =
    parseJWTPayload(header).fold(
      error => {
        logger.warn("Authorization error: " + error.message)
        ClaimSet.Unauthenticated
      },
      token => payloadToClaims(token),
    )

  private[this] def parsePayload(jwtPayload: String): Either[Error, AuthServiceJWTPayload] = {
    val parsed = targetAudience match {
      case Some(_) => Try(parseAudienceBasedPayload(jwtPayload))
      case None if targetScope.isDefined => Try(parseScopeBasedPayload(jwtPayload))
      case _ => Try(parseAuthServicePayload(jwtPayload))
    }

    parsed.toEither.left
      .map(t => Error(Symbol("parsePayload"), "Could not parse JWT token: " + t.getMessage))
      .flatMap(checkAudienceAndScope)
  }

  private def checkAudienceAndScope(
      payload: AuthServiceJWTPayload
  ): Either[Error, AuthServiceJWTPayload] =
    (payload, targetAudience, targetScope) match {
      case (payload: StandardJWTPayload, Some(audience), _) =>
        if (payload.audiences.contains(audience))
          Right(payload)
        else
          Left(Error(Symbol("checkAudienceAndScope"), "Audience doesn't match the target value"))
      case (payload: StandardJWTPayload, None, Some(scope)) =>
        if (payload.scope.toList.flatMap(_.split(' ')).contains(scope))
          Right(payload)
        else
          Left(Error(Symbol("checkAudienceAndScope"), "Scope doesn't match the target value"))
      case (payload, None, None) =>
        Right(payload)
      case _ =>
        Left(Error(Symbol("checkAudienceAndScope"), "Could not check the audience"))
    }

  private[this] def parseAuthServicePayload(jwtPayload: String): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.JsonImplicits.*
    JsonParser(jwtPayload).convertTo[AuthServiceJWTPayload]
  }

  private[this] def parseAudienceBasedPayload(
      jwtPayload: String
  ): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.AudienceBasedTokenJsonImplicits.*
    JsonParser(jwtPayload).convertTo[AuthServiceJWTPayload]
  }

  private[this] def parseScopeBasedPayload(
      jwtPayload: String
  ): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.ScopeBasedTokenJsonImplicits.*
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
    case payload: StandardJWTPayload =>
      ClaimSet.AuthenticatedUser(
        identityProviderId = IdentityProviderId.Default,
        participantId = payload.participantId,
        userId = payload.userId,
        expiration = payload.exp,
      )
  }
}

object AuthServiceJWT {
  def apply(
      verifier: com.auth0.jwt.interfaces.JWTVerifier,
      targetAudience: Option[String],
      targetScope: Option[String],
      loggerFactory: NamedLoggerFactory,
  ): AuthServiceJWT =
    new AuthServiceJWT(new JwtVerifier(verifier), targetAudience, targetScope, loggerFactory)

  def apply(
      verifier: JwtVerifierBase,
      targetAudience: Option[String],
      targetScope: Option[String],
      loggerFactory: NamedLoggerFactory,
  ): AuthServiceJWT =
    new AuthServiceJWT(verifier, targetAudience, targetScope, loggerFactory)
}
