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
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{CantonConfigValidator, UniformCantonConfigValidation}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Metadata
import spray.json.*

import scala.concurrent.Future
import scala.util.Try

sealed trait AccessLevel extends Product with Serializable with UniformCantonConfigValidation

object AccessLevel {
  implicit val accessLevelCantonConfigValidator: CantonConfigValidator[AccessLevel] =
    CantonConfigValidatorDerivation[AccessLevel]

  case object Admin extends AccessLevel
  case object Wildcard extends AccessLevel
}

final case class AuthorizedUser(
    userId: String,
    allowedServices: Seq[String],
) extends UniformCantonConfigValidation

object AuthorizedUser {
  implicit val accessLevelCantonConfigValidator: CantonConfigValidator[AuthorizedUser] =
    CantonConfigValidatorDerivation[AuthorizedUser]
}

/** An AuthService that reads a JWT token from a `Authorization: Bearer` HTTP header. The token is
  * expected to use the format as defined in [[com.daml.jwt.AuthServiceJWTPayload]]:
  */
abstract class AuthServiceJWTBase(
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
) extends AuthService
    with NamedLogging {

  override def decodeMetadata(
      headers: Metadata,
      serviceName: String,
  )(implicit traceContext: TraceContext): Future[ClaimSet] =
    Future.successful {
      getAuthorizationHeader(headers) match {
        case None => ClaimSet.Unauthenticated
        case Some(header) => parseHeader(header, serviceName)
      }
    }

  private[this] def getAuthorizationHeader(headers: Metadata): Option[String] =
    Option.apply(headers.get(AUTHORIZATION_KEY))

  private[this] def parseHeader(header: String, serviceName: String)(implicit
      traceContext: TraceContext
  ): ClaimSet =
    parseJWTPayload(header).fold(
      error => {
        logger.warn("Authorization error: " + error.message)
        ClaimSet.Unauthenticated
      },
      payloadToClaims(serviceName),
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

  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet
}

class AuthServiceJWT(
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    val loggerFactory: NamedLoggerFactory,
) extends AuthServiceJWTBase(verifier, targetAudience, targetScope) {
  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet = {
    case payload: StandardJWTPayload =>
      ClaimSet.AuthenticatedUser(
        identityProviderId = None,
        participantId = payload.participantId,
        userId = payload.userId,
        expiration = payload.exp,
      )
  }
}

class UserConfigAuthService private[auth] (
    verifier: JwtVerifierBase,
    targetAudience: Option[String],
    targetScope: Option[String],
    users: Seq[AuthorizedUser],
    val loggerFactory: NamedLoggerFactory,
) extends AuthServiceJWTBase(verifier, targetAudience, targetScope) {
  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet = {
    case payload: StandardJWTPayload =>
      users
        .find(_.userId == payload.userId)
        .flatMap(_.allowedServices.find(_ == serviceName))
        .fold[ClaimSet](ClaimSet.Unauthenticated)(_ => ClaimSet.Claims.Admin)
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
  protected[this] def payloadToClaims(serviceName: String): AuthServiceJWTPayload => ClaimSet = {
    case payload: StandardJWTPayload =>
      ClaimSet.Claims(
        claims = claims,
        identityProviderId = None,
        participantId = payload.participantId,
        userId = Option.when(payload.userId.nonEmpty)(payload.userId),
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
      users: Seq[AuthorizedUser],
  ): AuthServiceJWTBase =
    (privileged, targetScope, targetAudience, users) match {
      case (_, _, _, authorizedUsers) if authorizedUsers.nonEmpty =>
        new UserConfigAuthService(
          verifier,
          targetAudience,
          targetScope,
          authorizedUsers,
          loggerFactory,
        )
      case (true, Some(scope), _, _) =>
        new AuthServicePrivilegedJWT(verifier, None, Some(scope), accessLevel, loggerFactory)
      case (true, None, Some(audience), _) =>
        new AuthServicePrivilegedJWT(verifier, Some(audience), None, accessLevel, loggerFactory)
      case (true, None, None, _) =>
        throw new IllegalArgumentException(
          "Missing targetScope or targetAudience in the definition of a privileged JWT AuthService"
        )
      case (false, _, _, _) =>
        new AuthServiceJWT(verifier, targetAudience, targetScope, loggerFactory)
    }
}
