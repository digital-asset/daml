// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.daml.ledger.api.v1.package_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.Function.const
import scala.concurrent.Future

class PackageServiceValidation(
    protected val service: PackageService with AutoCloseable,
    val ledgerId: LedgerId)
    extends PackageService
    with ProxyCloseable
    with GrpcApiService
    with FieldValidations {

  protected val logger: Logger = LoggerFactory.getLogger(PackageService.getClass)

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
    matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      .map(const(request))
      .fold(
        Future.failed,
        service.listPackages
      )

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      .map(const(request))
      .fold(
        Future.failed,
        service.getPackage
      )

  override def getPackageStatus(
      request: GetPackageStatusRequest): Future[GetPackageStatusResponse] =
    matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      .map(const(request))
      .fold(
        Future.failed,
        service.getPackageStatus
      )
  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
