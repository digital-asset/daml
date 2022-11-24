// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.auth0.jwt.JWT
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.jwt.JwtVerifier
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth._
import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.localstore.api.IdentityProviderConfigStore
import com.daml.platform.localstore.api.IdentityProviderConfigStore.Result
import io.grpc.Metadata
import scalaz.\/
import spray.json._

import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.FutureOps

//TODO DPP-1299 Move to a package
class IdentityProviderAwareAuthService(
    defaultAuthService: AuthService,
    identityProviderConfigStore: IdentityProviderConfigStore,
    jwtVerifierLoader: JwtVerifierLoader,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends AuthService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  private implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

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

  private def handleResult(
      result: Result[IdentityProviderConfig]
  ): Future[IdentityProviderConfig] = result match {
    case Right(value) => Future.successful(value)
    case Left(error) =>
      Future.failed(LedgerApiErrors.InternalError.Generic(error.toString).asGrpcError)
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
      token <- toFuture(\/.fromEither(JwtVerifier.fromHeader(header)))
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
          identityProviderConfig <- identityProviderConfigStore
            .getIdentityProviderConfig(issuer)
            .flatMap(handleResult)
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
    toFuture(verifier.verify(com.daml.jwt.domain.Jwt(token)))

  private def toFuture[T](e: \/[JwtVerifier.Error, T]): Future[T] =
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
