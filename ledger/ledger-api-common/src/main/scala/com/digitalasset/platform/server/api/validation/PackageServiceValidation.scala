// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.validation

import com.digitalasset.daml.lf.data.Ref.LedgerId
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.package_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.Function.const
import scala.concurrent.Future

class PackageServiceValidation(
    protected val service: PackageService with AutoCloseable,
    val ledgerId: LedgerId
) extends PackageService
    with ProxyCloseable
    with GrpcApiService
    with FieldValidations {

  protected val logger: Logger = LoggerFactory.getLogger(PackageService.getClass)

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
    matchLedgerId(ledgerId)(request.ledgerId)
      .map(const(request))
      .fold(
        Future.failed,
        service.listPackages
      )

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    matchLedgerId(ledgerId)(request.ledgerId)
      .map(const(request))
      .fold(
        Future.failed,
        service.getPackage
      )

  override def getPackageStatus(
      request: GetPackageStatusRequest): Future[GetPackageStatusResponse] =
    matchLedgerId(ledgerId)(request.ledgerId)
      .map(const(request))
      .fold(
        Future.failed,
        service.getPackageStatus
      )
  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
