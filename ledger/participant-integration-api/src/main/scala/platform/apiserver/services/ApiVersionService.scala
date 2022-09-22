// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
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
import com.daml.platform.usermanagement.UserManagementConfig
import io.grpc.ServerServiceDefinition

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.Try
import scala.util.control.NonFatal

private[apiserver] final class ApiVersionService private (
    ledgerFeatures: LedgerFeatures,
    userManagementConfig: UserManagementConfig,
)(implicit
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) extends VersionService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val versionFile: String = "ledger-api/VERSION"
  private val apiVersion: Try[String] = readVersion(versionFile)

  private val featuresDescriptor =
    FeaturesDescriptor.of(
      userManagement = Some(
        if (userManagementConfig.enabled) {
          UserManagementFeature(
            supported = true,
            maxRightsPerUser = UserManagementConfig.MaxRightsPerUser,
            maxUsersPageSize = userManagementConfig.maxUsersPageSize,
          )
        } else {
          UserManagementFeature(
            supported = false,
            maxRightsPerUser = 0,
            maxUsersPageSize = 0,
          )
        }
      ),
      experimental = Some(
        ExperimentalFeatures.of(
          selfServiceErrorCodes = Some(ExperimentalSelfServiceErrorCodes()): @nowarn(
            "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.experimental_features\\..*"
          ),
          staticTime = Some(ExperimentalStaticTime(supported = ledgerFeatures.staticTime)),
          commandDeduplication = Some(ledgerFeatures.commandDeduplicationFeatures),
          optionalLedgerId = Some(ExperimentalOptionalLedgerId()),
          contractIds = Some(ledgerFeatures.contractIdFeatures),
          committerEventLog = Some(ledgerFeatures.committerEventLog),
          explicitDisclosure = Some(ledgerFeatures.explicitDisclosure),
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
      LedgerApiErrors.InternalError
        .VersionService(message = "Cannot read Ledger API version")
        .asGrpcError
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
      ledgerFeatures: LedgerFeatures,
      userManagementConfig: UserManagementConfig,
  )(implicit loggingContext: LoggingContext, ec: ExecutionContext): ApiVersionService =
    new ApiVersionService(
      ledgerFeatures,
      userManagementConfig = userManagementConfig,
    )
}
