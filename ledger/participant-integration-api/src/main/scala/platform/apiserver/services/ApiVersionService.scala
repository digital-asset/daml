// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCodesVersionSwitcher,
}
import com.daml.ledger.api.v1.experimental_features.{
  ExperimentalFeatures,
  ExperimentalOptionalLedgerId,
  ExperimentalSelfServiceErrorCodes,
  ExperimentalStaticTime,
}
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc.VersionService
import com.daml.ledger.api.v1.version_service.{
  FeaturesDescriptor,
  GetLedgerApiVersionRequest,
  GetLedgerApiVersionResponse,
  UserManagementFeature,
  VersionServiceGrpc,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.LedgerFeatures
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.usermanagement.UserManagementConfig
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.Try
import scala.util.control.NonFatal

private[apiserver] final class ApiVersionService private (
    enableSelfServiceErrorCodes: Boolean,
    ledgerFeatures: LedgerFeatures,
    enableUserManagement: Boolean,
)(implicit
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) extends VersionService
    with GrpcApiService {

  private val errorCodesVersionSwitcher = new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes)
  private val logger = ContextualizedLogger.get(this.getClass)
  private val errorFactories = ErrorFactories(errorCodesVersionSwitcher)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val versionFile: String = "ledger-api/VERSION"
  private lazy val apiVersion: Try[String] = readVersion(versionFile)

  private val featuresDescriptor =
    FeaturesDescriptor.of(
      userManagement = Some(
        UserManagementFeature(
          supported = enableUserManagement,
          maxRightsPerUser = if (enableUserManagement) UserManagementConfig.MaxRightsPerUser else 0,
        )
      ),
      experimental = Some(
        ExperimentalFeatures.of(
          selfServiceErrorCodes =
            Option.when(enableSelfServiceErrorCodes)(ExperimentalSelfServiceErrorCodes()),
          staticTime = Some(ExperimentalStaticTime(supported = ledgerFeatures.staticTime)),
          commandDeduplication = Some(ledgerFeatures.commandDeduplicationFeatures),
          optionalLedgerId = Some(ExperimentalOptionalLedgerId()),
          contractIds = Some(ledgerFeatures.contractIdFeatures),
        )
      ),
    )

  override def getLedgerApiVersion(
      request: GetLedgerApiVersionRequest
  ): Future[GetLedgerApiVersionResponse] =
    Future
      .fromTry(apiVersion)
      .map(apiVersionResponse)
      .andThen(logger.logErrorsOnCall[GetLedgerApiVersionResponse])
      .recoverWith { case NonFatal(_) =>
        internalError
      }

  private def apiVersionResponse(version: String) =
    GetLedgerApiVersionResponse(version, Some(featuresDescriptor))

  private lazy val internalError: Future[Nothing] =
    Future.failed(
      errorFactories.versionServiceInternalError(message = "Cannot read Ledger API version")
    )

  private def readVersion(versionFileName: String): Try[String] =
    Try {
      Source
        .fromResource(versionFileName)
        .getLines()
        .toList
        .head
    }

  override def bindService(): ServerServiceDefinition =
    VersionServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

}

private[apiserver] object ApiVersionService {
  def create(
      enableSelfServiceErrorCodes: Boolean,
      ledgerFeatures: LedgerFeatures,
      enableUserManagement: Boolean,
  )(implicit loggingContext: LoggingContext, ec: ExecutionContext): ApiVersionService =
    new ApiVersionService(
      enableSelfServiceErrorCodes,
      ledgerFeatures,
      enableUserManagement = enableUserManagement,
    )
}
