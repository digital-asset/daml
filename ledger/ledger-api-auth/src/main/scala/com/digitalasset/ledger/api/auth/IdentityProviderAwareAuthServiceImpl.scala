// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.api.auth

import com.auth0.jwt.JWT
import com.daml.jwt.{JwtFromBearerHeader, JwtVerifier}
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.domain.IdentityProviderId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc.Metadata
import spray.json._
import com.daml.jwt.{Error => JwtError}
import com.daml.ledger.api.auth.interceptor.IdentityProviderAwareAuthService

import scala.concurrent.{ExecutionContext, Future}

class IdentityProviderAwareAuthServiceImpl(
    identityProviderConfigLoader: IdentityProviderConfigLoader,
    jwtVerifierLoader: JwtVerifierLoader,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends IdentityProviderAwareAuthService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  def decodeMetadata(headers: Metadata): Future[ClaimSet] =
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
  ): Future[ClaimSet] =
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
  ): Future[ClaimSet] = {
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
          payload <- Future(parse(decodedJwt.payload))
          jwtPayload <- parsePayload(payload)
        } yield toAuthenticatedUser(jwtPayload, identityProviderConfig.identityProviderId)
    }
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

  private def parse(jwtPayload: String): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.JsonImplicits._
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
