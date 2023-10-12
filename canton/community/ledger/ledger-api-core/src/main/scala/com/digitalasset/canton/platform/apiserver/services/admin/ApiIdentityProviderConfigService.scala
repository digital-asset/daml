// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v1.admin.identity_provider_config_service as proto
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.error.groups.IdentityProviderConfigServiceErrors
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiIdentityProviderConfigService.toProto
import com.digitalasset.canton.platform.apiserver.update
import com.digitalasset.canton.platform.apiserver.update.IdentityProviderConfigUpdateMapper
import com.digitalasset.canton.platform.localstore.api
import com.digitalasset.canton.platform.localstore.api.{
  IdentityProviderConfigStore,
  IdentityProviderConfigUpdate,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

class ApiIdentityProviderConfigService(
    identityProviderConfigStore: IdentityProviderConfigStore,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends proto.IdentityProviderConfigServiceGrpc.IdentityProviderConfigService
    with GrpcApiService
    with NamedLogging {

  import com.digitalasset.canton.ledger.api.validation.FieldValidator.*

  private def withValidation[A, B](validatedResult: Either[StatusRuntimeException, A])(
      f: A => Future[B]
  ): Future[B] =
    validatedResult.fold(Future.failed, Future.successful).flatMap(f)

  override def createIdentityProviderConfig(
      request: proto.CreateIdentityProviderConfigRequest
  ): Future[proto.CreateIdentityProviderConfigResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)

    logger.info("Creating identity provider config.")
    withValidation {
      for {
        config <- requirePresence(request.identityProviderConfig, "identity_provider_config")
        identityProviderId <- requireIdentityProviderId(
          config.identityProviderId,
          "identity_provider_id",
        )
        jwksUrl <- requireJwksUrl(config.jwksUrl, "jwks_url")
        issuer <- requireNonEmptyString(config.issuer, "issuer")
        audience <- optionalString(config.audience)(Right(_))
      } yield IdentityProviderConfig(
        identityProviderId,
        config.isDeactivated,
        jwksUrl,
        issuer,
        audience,
      )
    } { config =>
      identityProviderConfigStore
        .createIdentityProviderConfig(config)
        .flatMap(handleResult("creating identity provider config"))
        .map(config => proto.CreateIdentityProviderConfigResponse(Some(toProto(config))))
    }
  }

  override def getIdentityProviderConfig(
      request: proto.GetIdentityProviderConfigRequest
  ): Future[proto.GetIdentityProviderConfigResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)

    withValidation(
      requireIdentityProviderId(request.identityProviderId, "identity_provider_id")
    )(identityProviderId =>
      identityProviderConfigStore
        .getIdentityProviderConfig(identityProviderId)
        .flatMap(handleResult("getting identity provider config"))
        .map(cfg => proto.GetIdentityProviderConfigResponse(Some(toProto(cfg))))
    )
  }
  override def updateIdentityProviderConfig(
      request: proto.UpdateIdentityProviderConfigRequest
  ): Future[proto.UpdateIdentityProviderConfigResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)

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
        audience <- optionalString(config.audience)(Right(_))
      } yield (
        IdentityProviderConfigUpdate(
          identityProviderId,
          Some(config.isDeactivated),
          jwksUrl,
          issuer,
          Some(audience),
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
        updatedIdentityProviderConfig <- handleResult("updating identity provider config")(
          updateResult
        )
      } yield proto.UpdateIdentityProviderConfigResponse(
        Some(toProto(updatedIdentityProviderConfig))
      )
    }
  }

  override def listIdentityProviderConfigs(
      request: proto.ListIdentityProviderConfigsRequest
  ): Future[proto.ListIdentityProviderConfigsResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)

    identityProviderConfigStore
      .listIdentityProviderConfigs()
      .flatMap(handleResult("listing identity provider configs"))
      .map(result => proto.ListIdentityProviderConfigsResponse(result.map(toProto).toSeq))
  }
  override def deleteIdentityProviderConfig(
      request: proto.DeleteIdentityProviderConfigRequest
  ): Future[proto.DeleteIdentityProviderConfigResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)

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
  )(implicit traceContext: TraceContext): Future[T] = result match {
    case Left(IdentityProviderConfigStore.IdentityProviderConfigNotFound(id)) =>
      Future.failed(
        IdentityProviderConfigServiceErrors.IdentityProviderConfigNotFound
          .Reject(operation, id.value)
          .asGrpcError
      )
    case Left(IdentityProviderConfigStore.IdentityProviderConfigExists(id)) =>
      Future.failed(
        IdentityProviderConfigServiceErrors.IdentityProviderConfigAlreadyExists
          .Reject(operation, id.value)
          .asGrpcError
      )
    case Left(IdentityProviderConfigStore.IdentityProviderConfigWithIssuerExists(issuer)) =>
      Future.failed(
        IdentityProviderConfigServiceErrors.IdentityProviderConfigIssuerAlreadyExists
          .Reject(operation, issuer)
          .asGrpcError
      )
    case Left(IdentityProviderConfigStore.TooManyIdentityProviderConfigs()) =>
      Future.failed(
        IdentityProviderConfigServiceErrors.TooManyIdentityProviderConfigs
          .Reject(operation)
          .asGrpcError
      )
    case Left(IdentityProviderConfigStore.IdentityProviderConfigByIssuerNotFound(issuer)) =>
      Future.failed(
        IdentityProviderConfigServiceErrors.IdentityProviderConfigByIssuerNotFound
          .Reject(operation, issuer)
          .asGrpcError
      )
    case scala.util.Right(t) =>
      Future.successful(t)
  }

  private def handleUpdatePathResult[T](
      identityProviderId: IdentityProviderId.Id,
      result: update.Result[T],
  )(implicit traceContext: TraceContext): Future[T] =
    result match {
      case Left(e: update.UpdatePathError) =>
        Future.failed(
          IdentityProviderConfigServiceErrors.InvalidUpdateIdentityProviderConfigRequest
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
      audience = identityProviderConfig.audience.getOrElse(""),
    )
}
