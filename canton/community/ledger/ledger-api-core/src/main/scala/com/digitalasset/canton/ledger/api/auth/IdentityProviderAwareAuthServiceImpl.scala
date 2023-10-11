// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.auth0.jwt.JWT
import com.daml.jwt.domain.DecodedJwt
import com.daml.jwt.{Error as JwtError, JwtFromBearerHeader, JwtVerifier}
import com.digitalasset.canton.ledger.api.auth.interceptor.IdentityProviderAwareAuthService
import com.digitalasset.canton.ledger.api.domain.IdentityProviderId
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import io.grpc.Metadata
import spray.json.*

import scala.concurrent.{ExecutionContext, Future}

class IdentityProviderAwareAuthServiceImpl(
    identityProviderConfigLoader: IdentityProviderConfigLoader,
    jwtVerifierLoader: JwtVerifierLoader,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends IdentityProviderAwareAuthService
    with NamedLogging {

  def decodeMetadata(
      headers: Metadata
  )(implicit loggingContext: LoggingContextWithTrace): Future[ClaimSet] =
    getAuthorizationHeader(headers) match {
      case None => Future.successful(ClaimSet.Unauthenticated)
      case Some(header) =>
        parseJWTPayload(header).recover { case error =>
          // While we failed to authorize the token using IDP, it could still be possible
          // to be valid by other means of authorizations, i.e. using default auth service
          logger.warn("Failed to authorize the token: " + error.getMessage)
          ClaimSet.Unauthenticated
        }
    }

  private def getAuthorizationHeader(headers: Metadata): Option[String] =
    Option(headers.get(AuthService.AUTHORIZATION_KEY))

  private def parseJWTPayload(
      header: String
  )(implicit loggingContext: LoggingContextWithTrace): Future[ClaimSet] =
    for {
      token <- toFuture(JwtFromBearerHeader(header))
      decodedJWT <- Future(JWT.decode(token))
      claims <- extractClaims(
        token,
        Option(decodedJWT.getIssuer),
        Option(decodedJWT.getKeyId),
      )
    } yield claims

  def extractClaims(
      token: String,
      issuer: Option[String],
      keyId: Option[String],
  )(implicit loggingContext: LoggingContextWithTrace): Future[ClaimSet] = {
    issuer match {
      case None => Future.successful(ClaimSet.Unauthenticated)
      case Some(issuer) =>
        for {
          identityProviderConfig <- identityProviderConfigLoader
            .getIdentityProviderConfig(issuer)
          verifier <- jwtVerifierLoader.loadJwtVerifier(
            jwksUrl = identityProviderConfig.jwksUrl,
            keyId,
          )
          decodedJwt <- verifyToken(token, verifier)
          payload <- Future(
            parse(decodedJwt.payload, targetAudience = identityProviderConfig.audience)
          )
          _ <- checkAudience(payload, identityProviderConfig.audience)
          jwtPayload <- parsePayload(payload)
        } yield toAuthenticatedUser(jwtPayload, identityProviderConfig.identityProviderId)
    }
  }

  private def checkAudience(
      payload: AuthServiceJWTPayload,
      targetAudience: Option[String],
  ): Future[Unit] =
    (payload, targetAudience) match {
      case (payload: StandardJWTPayload, Some(audience)) if payload.audiences.contains(audience) =>
        Future.unit
      case (_, None) =>
        Future.unit
      case _ =>
        Future.failed(new Exception(s"JWT token has an audience which is not recognized"))
    }

  private def verifyToken(token: String, verifier: JwtVerifier): Future[DecodedJwt[String]] =
    toFuture(verifier.verify(com.daml.jwt.domain.Jwt(token)).toEither)

  private def toFuture[T](e: Either[JwtError, T]): Future[T] =
    e.fold(err => Future.failed(new Exception(err.message)), Future.successful)

  private def parsePayload(
      jwtPayload: AuthServiceJWTPayload
  ): Future[StandardJWTPayload] =
    jwtPayload match {
      case _: CustomDamlJWTPayload =>
        Future.failed(new Exception("Unexpected token payload format"))
      case payload: StandardJWTPayload =>
        Future.successful(payload)
    }

  private def parse(jwtPayload: String, targetAudience: Option[String]): AuthServiceJWTPayload =
    if (targetAudience.isDefined)
      parseAudienceBasedPayload(jwtPayload)
    else
      parseAuthServicePayload(jwtPayload)

  private def parseAuthServicePayload(jwtPayload: String): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.JsonImplicits.*
    JsonParser(jwtPayload).convertTo[AuthServiceJWTPayload]
  }

  private[this] def parseAudienceBasedPayload(
      jwtPayload: String
  ): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.AudienceBasedTokenJsonImplicits.*
    JsonParser(jwtPayload).convertTo[AuthServiceJWTPayload]
  }

  private def toAuthenticatedUser(payload: StandardJWTPayload, id: IdentityProviderId.Id) =
    ClaimSet.AuthenticatedUser(
      identityProviderId = id,
      participantId = payload.participantId,
      userId = payload.userId,
      expiration = payload.exp,
    )
}
