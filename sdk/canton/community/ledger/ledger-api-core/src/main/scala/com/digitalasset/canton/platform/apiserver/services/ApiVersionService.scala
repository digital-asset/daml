// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v2.experimental_features.*
import com.daml.ledger.api.v2.version_service.VersionServiceGrpc.VersionService
import com.daml.ledger.api.v2.version_service.{
  FeaturesDescriptor,
  GetLedgerApiVersionResponse,
  UserManagementFeature,
  *,
}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.apiserver.LedgerFeatures
import com.digitalasset.canton.platform.config.UserManagementServiceConfig
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.Try
import scala.util.control.NonFatal

private[apiserver] final class ApiVersionService(
    ledgerFeatures: LedgerFeatures,
    userManagementServiceConfig: UserManagementServiceConfig,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends VersionService
    with GrpcApiService
    with NamedLogging {

  private val versionFile: String = "VERSION"
  private val apiVersion: Try[String] = readVersion(versionFile)

  private val featuresDescriptor =
    FeaturesDescriptor.of(
      userManagement = Some(
        if (userManagementServiceConfig.enabled) {
          UserManagementFeature(
            supported = true,
            maxRightsPerUser = userManagementServiceConfig.maxRightsPerUser,
            maxUsersPageSize = userManagementServiceConfig.maxUsersPageSize,
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
          staticTime = Some(ExperimentalStaticTime(supported = ledgerFeatures.staticTime))
        )
      ),
    )

  override def getLedgerApiVersion(
      request: GetLedgerApiVersionRequest
  ): Future[GetLedgerApiVersionResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    Future
      .fromTry(apiVersion)
      .map(apiVersionResponse)
      .andThen(logger.logErrorsOnCall[GetLedgerApiVersionResponse])
      .recoverWith { case NonFatal(_) =>
        Future.failed(
          LedgerApiErrors.InternalError
            .VersionService(message = "Cannot read Ledger API version")
            .asGrpcError
        )
      }
  }
  private def apiVersionResponse(version: String) =
    GetLedgerApiVersionResponse(version, Some(featuresDescriptor))

  private def readVersion(versionFileName: String): Try[String] =
    Try {
      Source
        .fromResource(versionFileName)
        .getLines()
        .toList
        .headOption
        .getOrElse(throw new IllegalStateException("Empty version file"))
    }

  override def bindService(): ServerServiceDefinition =
    VersionServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

}
