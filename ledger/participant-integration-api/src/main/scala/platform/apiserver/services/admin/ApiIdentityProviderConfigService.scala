// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.daml.ledger.api.v1.admin.{identity_provider_config_service => proto}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.admin.ApiIdentityProviderConfigService.toProto
import com.daml.platform.apiserver.update
import com.daml.platform.apiserver.update.IdentityProviderConfigUpdateMapper
import com.daml.platform.localstore.api
import com.daml.platform.localstore.api.{IdentityProviderConfigStore, IdentityProviderConfigUpdate}
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

class ApiIdentityProviderConfigService(
    identityProviderConfigStore: IdentityProviderConfigStore
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends proto.IdentityProviderConfigServiceGrpc.IdentityProviderConfigService
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  import com.daml.platform.server.api.validation.FieldValidations._

  private def withValidation[A, B](validatedResult: Either[StatusRuntimeException, A])(
      f: A => Future[B]
  ): Future[B] =
    validatedResult.fold(Future.failed, Future.successful).flatMap(f)

  override def createIdentityProviderConfig(
      request: proto.CreateIdentityProviderConfigRequest
  ): Future[proto.CreateIdentityProviderConfigResponse] =
    withValidation {
      for {
        config <- requirePresence(request.identityProviderConfig, "identity_provider_config")
        identityProviderId <- requireIdentityProviderId(
          config.identityProviderId,
          "identity_provider_id",
        )
        jwksUrl <- requireJwksUrl(config.jwksUrl, "jwks_url")
        issuer <- requireNonEmptyString(config.issuer, "issuer")
      } yield IdentityProviderConfig(
        identityProviderId,
        config.isDeactivated,
        jwksUrl,
        issuer,
      )
    } { config =>
      identityProviderConfigStore
        .createIdentityProviderConfig(config)
        .flatMap(handleResult("creating identity provider config"))
        .map(config => proto.CreateIdentityProviderConfigResponse(Some(toProto(config))))
    }

  override def getIdentityProviderConfig(
      request: proto.GetIdentityProviderConfigRequest
  ): Future[proto.GetIdentityProviderConfigResponse] =
    withValidation(
      requireIdentityProviderId(request.identityProviderId, "identity_provider_id")
    )(identityProviderId =>
      identityProviderConfigStore
        .getIdentityProviderConfig(identityProviderId)
        .flatMap(handleResult("getting identity provider config"))
        .map(cfg => proto.GetIdentityProviderConfigResponse(Some(toProto(cfg))))
    )

  override def updateIdentityProviderConfig(
      request: proto.UpdateIdentityProviderConfigRequest
  ): Future[proto.UpdateIdentityProviderConfigResponse] =
    withValidation {
      for {
        config <- requirePresence(request.identityProviderConfig, "identity_provider_config")
        identityProviderId <- requireIdentityProviderId(
          config.identityProviderId,
          "identity_provider_id",
        )
        jwksUrl <- optionalString(config.jwksUrl)(requireJwksUrl(_, "jwks_url"))
        issuer <- optionalString(config.issuer)(requireNonEmptyString(_, "issuer"))
        updateMask <- requirePresence(
          request.updateMask,
          "update_mask",
        )
      } yield (
        IdentityProviderConfigUpdate(
          identityProviderId,
          Some(config.isDeactivated),
          jwksUrl,
          issuer,
        ),
        updateMask,
      )
    } { case (identityProviderConfig, updateMask) =>
      for {
        identityProviderConfigUpdate: IdentityProviderConfigUpdate <- handleUpdatePathResult(
          identityProviderId = identityProviderConfig.identityProviderId,
          IdentityProviderConfigUpdateMapper.toUpdate(
            domainObject = identityProviderConfig,
            updateMask = updateMask,
          ),
        )
        updateResult <- identityProviderConfigStore.updateIdentityProviderConfig(
          identityProviderConfigUpdate
        )
        updatedIdentityProviderConfig <- handleResult("deleting identity provider config")(
          updateResult
        )
      } yield proto.UpdateIdentityProviderConfigResponse(
        Some(toProto(updatedIdentityProviderConfig))
      )
    }

  override def listIdentityProviderConfigs(
      request: proto.ListIdentityProviderConfigsRequest
  ): Future[proto.ListIdentityProviderConfigsResponse] =
    identityProviderConfigStore
      .listIdentityProviderConfigs()
      .flatMap(handleResult("listing identity provider configs"))
      .map(result => proto.ListIdentityProviderConfigsResponse(result.map(toProto).toSeq))

  override def deleteIdentityProviderConfig(
      request: proto.DeleteIdentityProviderConfigRequest
  ): Future[proto.DeleteIdentityProviderConfigResponse] = {
    withValidation(
      requireIdentityProviderId(request.identityProviderId, "identity_provider_id")
    )(identityProviderId =>
      identityProviderConfigStore
        .deleteIdentityProviderConfig(identityProviderId)
        .flatMap(handleResult("deleting identity provider config"))
        .map { _ =>
          proto.DeleteIdentityProviderConfigResponse()
        }
    )
  }

  private def handleResult[T](operation: String)(
      result: api.IdentityProviderConfigStore.Result[T]
  ): Future[T] = result match {
    case Left(IdentityProviderConfigStore.IdentityProviderConfigNotFound(id)) =>
      Future.failed(
        LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigNotFound
          .Reject(operation, id.value)
          .asGrpcError
      )
    case Left(IdentityProviderConfigStore.IdentityProviderConfigExists(id)) =>
      Future.failed(
        LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigAlreadyExists
          .Reject(operation, id.value)
          .asGrpcError
      )
    case Left(IdentityProviderConfigStore.IdentityProviderConfigWithIssuerExists(issuer)) =>
      Future.failed(
        LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigIssuerAlreadyExists
          .Reject(operation, issuer)
          .asGrpcError
      )
    case Left(IdentityProviderConfigStore.TooManyIdentityProviderConfigs()) =>
      Future.failed(
        LedgerApiErrors.Admin.IdentityProviderConfig.TooManyIdentityProviderConfigs
          .Reject(operation)
          .asGrpcError
      )
    case Left(IdentityProviderConfigStore.IdentityProviderConfigByIssuerNotFound(issuer)) =>
      Future.failed(
        LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigByIssuerNotFound
          .Reject(operation, issuer)
          .asGrpcError
      )
    case scala.util.Right(t) =>
      Future.successful(t)
  }

  private def handleUpdatePathResult[T](
      identityProviderId: IdentityProviderId.Id,
      result: update.Result[T],
  ): Future[T] =
    result match {
      case Left(e: update.UpdatePathError) =>
        Future.failed(
          LedgerApiErrors.Admin.IdentityProviderConfig.InvalidUpdateIdentityProviderConfigRequest
            .Reject(identityProviderId.value, reason = e.getReason)
            .asGrpcError
        )
      case Right(t) =>
        Future.successful(t)
    }

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    proto.IdentityProviderConfigServiceGrpc.bindService(this, executionContext)
}

object ApiIdentityProviderConfigService {
  private def toProto(
      identityProviderConfig: IdentityProviderConfig
  ): proto.IdentityProviderConfig =
    proto.IdentityProviderConfig(
      identityProviderId = identityProviderConfig.identityProviderId.toRequestString,
      isDeactivated = identityProviderConfig.isDeactivated,
      jwksUrl = identityProviderConfig.jwksUrl.value,
      issuer = identityProviderConfig.issuer,
    )

}
