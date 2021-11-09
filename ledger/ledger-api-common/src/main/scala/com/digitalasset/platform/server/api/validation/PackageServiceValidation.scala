// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCodesVersionSwitcher,
}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.daml.ledger.api.v1.package_service._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.{ProxyCloseable, ValidationLogger}
import io.grpc.ServerServiceDefinition

import scala.Function.const
import scala.concurrent.{ExecutionContext, Future}

class PackageServiceValidation(
    protected val service: PackageService with AutoCloseable,
    val ledgerId: LedgerId,
    errorCodesVersionSwitcher: ErrorCodesVersionSwitcher,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends PackageService
    with ProxyCloseable
    with GrpcApiService {

  private val fieldValidations = FieldValidations(ErrorFactories(errorCodesVersionSwitcher))
  protected implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
    fieldValidations
      .matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      .map(const(request))
      .fold(
        t =>
          Future.failed(
            ValidationLogger
              .logFailure(errorCodesVersionSwitcher.enableSelfServiceErrorCodes)(request, t)
          ),
        service.listPackages,
      )

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    fieldValidations
      .matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      .map(const(request))
      .fold(
        t =>
          Future.failed(
            ValidationLogger
              .logFailure(errorCodesVersionSwitcher.enableSelfServiceErrorCodes)(request, t)
          ),
        service.getPackage,
      )

  override def getPackageStatus(
      request: GetPackageStatusRequest
  ): Future[GetPackageStatusResponse] =
    fieldValidations
      .matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      .map(const(request))
      .fold(
        t =>
          Future.failed(
            ValidationLogger
              .logFailure(errorCodesVersionSwitcher.enableSelfServiceErrorCodes)(request, t)
          ),
        service.getPackageStatus,
      )
  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
