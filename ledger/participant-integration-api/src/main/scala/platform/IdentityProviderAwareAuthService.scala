// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.auth.{AuthService, ClaimSet, IdentityProviderAuthService}
import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.localstore.api.IdentityProviderConfigStore
import com.daml.platform.localstore.api.IdentityProviderConfigStore.Result
import io.grpc.Metadata

import scala.jdk.FutureConverters.FutureOps
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.concurrent.{ExecutionContext, Future}

//TODO DPP-1299 Move to a package
class IdentityProviderAwareAuthService(
    defaultAuthService: AuthService,
    identityProviderAuthService: IdentityProviderAuthService,
    identityProviderConfigStore: IdentityProviderConfigStore,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends AuthService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  private implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private def toEntry(
      identityProviderConfig: IdentityProviderConfig
  ): IdentityProviderAuthService.Entry =
    IdentityProviderAuthService.Entry(
      identityProviderConfig.identityProviderId,
      identityProviderConfig.jwksUrl,
      identityProviderConfig.issuer,
    )

  private def handleResult(
      result: Result[Seq[IdentityProviderConfig]]
  ): Future[Seq[IdentityProviderConfig]] = result match {
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

  private def iterateOverIdentityProviders(
      headers: Metadata
  )(entries: Seq[IdentityProviderAuthService.Entry]): CompletionStage[ClaimSet] =
    identityProviderAuthService.decodeMetadata(getAuthorizationHeader(headers), entries)

  override def decodeMetadata(headers: Metadata): CompletionStage[ClaimSet] =
    identityProviderConfigStore
      .listIdentityProviderConfigs() // todo cache the list
      .flatMap(handleResult)
      .map(_.filterNot(_.isDeactivated))
      .map(_.map(toEntry))
      .asJava
      .thenCompose(iterateOverIdentityProviders(headers))
      .thenCompose(defaultAuth(headers))
}
