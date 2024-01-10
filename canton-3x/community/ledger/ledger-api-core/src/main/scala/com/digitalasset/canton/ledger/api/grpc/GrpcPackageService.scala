// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.daml.ledger.api.v1.package_service.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.{LedgerId, optionalLedgerId}
import com.digitalasset.canton.ledger.api.validation.FieldValidator
import com.digitalasset.canton.ledger.api.{ProxyCloseable, ValidationLogger}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import io.grpc.ServerServiceDefinition

import scala.Function.const
import scala.concurrent.{ExecutionContext, Future}

class GrpcPackageService(
    protected val service: PackageService with AutoCloseable,
    val ledgerId: LedgerId,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends PackageService
    with ProxyCloseable
    with GrpcApiService
    with NamedLogging {

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)

    FieldValidator
      .matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
      .map(const(request))
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        service.listPackages,
      )
  }
  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)

    FieldValidator
      .matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
      .map(const(request))
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        service.getPackage,
      )
  }
  override def getPackageStatus(
      request: GetPackageStatusRequest
  ): Future[GetPackageStatusResponse] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)

    FieldValidator
      .matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
      .map(const(request))
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        service.getPackageStatus,
      )
  }
  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
