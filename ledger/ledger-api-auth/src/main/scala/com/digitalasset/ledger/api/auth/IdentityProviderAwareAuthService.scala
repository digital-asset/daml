// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.api.auth

import com.auth0.jwt.JWT
import com.daml.jwt.JwtVerifier
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.domain.IdentityProviderId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc.Metadata
import spray.json._

import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.FutureOps

class IdentityProviderAwareAuthService(
    defaultAuthService: AuthService,
    configLoader: ConfigLoader,
    jwtVerifierLoader: JwtVerifierLoader,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends AuthService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def decodeMetadata(headers: Metadata): CompletionStage[ClaimSet] = {
    (getAuthorizationHeader(headers) match {
      case None => Future.successful(ClaimSet.Unauthenticated)
      case Some(header) =>
        parseJWTPayload(header).recover { case error =>
          logger.warn("Authorization error: " + error.getMessage)
          ClaimSet.Unauthenticated
        }
    }).asJava
      .thenCompose(defaultAuth(headers))
  }

  def defaultAuth(
      headers: Metadata
  )(prevClaims: ClaimSet): CompletionStage[ClaimSet] =
    if (prevClaims != ClaimSet.Unauthenticated)
      CompletableFuture.completedFuture(prevClaims)
    else {
      defaultAuthService.decodeMetadata(headers)
    }

  private def getAuthorizationHeader(headers: Metadata): Option[String] =
    Option(headers.get(AUTHORIZATION_KEY))

  private def parseJWTPayload(
      header: String
  ): Future[ClaimSet] =
    for {
      token <- toFuture(JwtVerifier.fromHeader(header))
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
          identityProviderConfig <- configLoader
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

  private def toFuture[T](e: Either[JwtVerifier.Error, T]): Future[T] =
    e.fold(err => Future.failed(new Exception(err.message)), Future.successful)

  private def parsePayload(
      jwtPayload: AuthServiceJWTPayload
  ): Future[StandardJWTPayload] =
    jwtPayload match {
      case _: CustomDamlJWTPayload =>
        Future.failed(new Exception("Unexpected token format"))
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
