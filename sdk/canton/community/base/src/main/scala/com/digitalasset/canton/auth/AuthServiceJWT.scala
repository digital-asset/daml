// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  Error,
  JwtFromBearerHeader,
  JwtVerifierBase,
  StandardJWTPayload,
}
import com.digitalasset.canton.auth.AuthService.AUTHORIZATION_KEY
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Metadata
import spray.json.*

import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.util.Try

sealed trait AccessLevel extends Product with Serializable

object AccessLevel {
  case object Admin extends AccessLevel
  case object Wildcard extends AccessLevel
}

/** An AuthService that reads a JWT token from a `Authorization: Bearer` HTTP header.
  * The token is expected to use the format as defined in [[com.daml.jwt.AuthServiceJWTPayload]]:
  */
abstract class AuthServiceJWTBase(
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
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
      payloadToClaims,
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
        .verify(com.daml.jwt.Jwt(token))
        .toEither
        .left
        .map(e => Error(Symbol("parseJWTPayload"), "Could not verify JWT token: " + e.message))
      parsed <- parsePayload(decoded.payload)
    } yield parsed

  protected[this] def payloadToClaims: AuthServiceJWTPayload => ClaimSet
}

class AuthServiceJWT(
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    val loggerFactory: NamedLoggerFactory,
) extends AuthServiceJWTBase(verifier, targetAudience, targetScope) {
  protected[this] def payloadToClaims: AuthServiceJWTPayload => ClaimSet = {
    case payload: StandardJWTPayload =>
      ClaimSet.AuthenticatedUser(
        identityProviderId = None,
        participantId = payload.participantId,
        userId = payload.userId,
        expiration = payload.exp,
      )
  }
}

class AuthServicePrivilegedJWT private[auth] (
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    accessLevel: AccessLevel,
    val loggerFactory: NamedLoggerFactory,
) extends AuthServiceJWTBase(
      verifier = verifier,
      targetAudience = targetAudience,
      targetScope = targetScope,
    ) {

  private def claims = accessLevel match {
    case AccessLevel.Admin => ClaimSet.Claims.Admin.claims
    case AccessLevel.Wildcard => ClaimSet.Claims.Wildcard.claims
  }
  protected[this] def payloadToClaims: AuthServiceJWTPayload => ClaimSet = {
    case payload: StandardJWTPayload =>
      ClaimSet.Claims(
        claims = claims,
        identityProviderId = None,
        participantId = payload.participantId,
        applicationId = Option.when(payload.userId.nonEmpty)(payload.userId),
        expiration = payload.exp,
        resolvedFromUser = false,
      )
  }
}

object AuthServiceJWT {
  def apply(
      verifier: JwtVerifierBase,
      targetAudience: Option[String],
      targetScope: Option[String],
      privileged: Boolean,
      accessLevel: AccessLevel,
      loggerFactory: NamedLoggerFactory,
  ): AuthServiceJWTBase =
    (privileged, targetScope, targetAudience) match {
      case (true, Some(scope), _) =>
        new AuthServicePrivilegedJWT(verifier, None, Some(scope), accessLevel, loggerFactory)
      case (true, None, Some(audience)) =>
        new AuthServicePrivilegedJWT(verifier, Some(audience), None, accessLevel, loggerFactory)
      case (true, None, None) =>
        throw new IllegalArgumentException(
          "Missing targetScope or targetAudience in the definition of a privileged JWT AuthService"
        )
      case (false, _, _) =>
        new AuthServiceJWT(verifier, targetAudience, targetScope, loggerFactory)
    }
}
