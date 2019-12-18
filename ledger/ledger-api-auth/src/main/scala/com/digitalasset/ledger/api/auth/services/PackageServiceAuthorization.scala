// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.services

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.auth.Authorizer
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.package_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition

import scala.concurrent.Future

final class PackageServiceAuthorization(
    protected val service: PackageService with AutoCloseable,
    private val authorizer: Authorizer)
    extends PackageService
    with ProxyCloseable
    with GrpcApiService {

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
    authorizer.requirePublicClaims(service.listPackages)(request)

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    authorizer.requirePublicClaims(service.getPackage)(request)

  override def getPackageStatus(
      request: GetPackageStatusRequest): Future[GetPackageStatusResponse] =
    authorizer.requirePublicClaims(service.getPackageStatus)(request)

  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
