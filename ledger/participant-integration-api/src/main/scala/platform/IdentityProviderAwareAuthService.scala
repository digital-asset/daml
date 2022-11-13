// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.jwt.JwtTimestampLeeway
import com.daml.ledger.api.auth.{AuthService, ClaimSet, IdentityProviderAuthService}
import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.localstore.api.IdentityProviderStore
import com.daml.platform.localstore.api.IdentityProviderStore.Result
import io.grpc.Metadata

import scala.jdk.FutureConverters.FutureOps
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.concurrent.{ExecutionContext, Future}

//TODO DPP-1299 Move to a package
class IdentityProviderAwareAuthService(
    defaultAuthService: AuthService,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    identityProviderStore: IdentityProviderStore,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends AuthService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  private implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val deny = CompletableFuture.completedFuture(ClaimSet.Unauthenticated: ClaimSet)

  private def toEntry(
      identityProviderConfig: IdentityProviderConfig
  ): IdentityProviderAwareAuthService.IdpEntry = {
    val authService = new IdentityProviderAuthService(
      identityProviderConfig.jwksURL,
      identityProviderConfig.issuer,
      IdentityProviderAuthService.Config(
        cache = IdentityProviderAuthService.CacheConfig(),
        http = IdentityProviderAuthService.HttpConfig(),
        jwtTimestampLeeway = jwtTimestampLeeway,
      ),
    )
    IdentityProviderAwareAuthService.IdpEntry(
      identityProviderConfig.identityProviderId,
      authService,
    )
  }

  private def claimCheck(
      prevClaims: ClaimSet,
      entry: IdentityProviderAwareAuthService.IdpEntry,
      headers: Metadata,
  ): CompletionStage[ClaimSet] =
    if (prevClaims != ClaimSet.Unauthenticated)
      CompletableFuture.completedFuture(prevClaims)
    else
      entry.service.decodeMetadata(headers)

  private def handleResult(
      result: Result[Seq[IdentityProviderConfig]]
  ): Future[Seq[IdentityProviderConfig]] = result match {
    case Right(value) => Future.successful(value)
    case Left(error) =>
      Future.failed(LedgerApiErrors.InternalError.Generic(error.toString).asGrpcError)
  }

  private def iterateOverIdentityProviders(
      headers: Metadata
  )(entries: Seq[IdentityProviderAwareAuthService.IdpEntry]): CompletableFuture[ClaimSet] =
    entries
      .foldLeft(deny) { case (acc, elem) =>
        acc.thenCompose(prevClaims => claimCheck(prevClaims, elem, headers))
      }
      .thenCompose { prevClaims =>
        if (prevClaims != ClaimSet.Unauthenticated)
          CompletableFuture.completedFuture(prevClaims)
        else {
          defaultAuthService.decodeMetadata(headers)
        }
      }

  override def decodeMetadata(headers: Metadata): CompletionStage[ClaimSet] =
    identityProviderStore
      .listIdentityProviderConfigs() // todo cache the list
      .flatMap(handleResult)
      .map(_.map(toEntry))
      .asJava
      .thenCompose(iterateOverIdentityProviders(headers))
}

object IdentityProviderAwareAuthService {
  case class IdpEntry(id: Ref.IdentityProviderId.Id, service: AuthService)
}
