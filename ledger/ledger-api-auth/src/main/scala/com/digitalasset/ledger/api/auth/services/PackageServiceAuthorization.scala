// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.services

import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.package_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class PackageServiceAuthorization(
    protected val service: PackageService with AutoCloseable,
    protected val authService: AuthService)
    extends PackageService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(PackageService.getClass)

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
    ApiServiceAuthorization
      .requirePublicClaims()
      .fold(Future.failed(_), _ => service.listPackages(request))

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    ApiServiceAuthorization
      .requirePublicClaims()
      .fold(Future.failed(_), _ => service.getPackage(request))

  override def getPackageStatus(
      request: GetPackageStatusRequest): Future[GetPackageStatusResponse] =
    ApiServiceAuthorization
      .requirePublicClaims()
      .fold(Future.failed(_), _ => service.getPackageStatus(request))

  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
