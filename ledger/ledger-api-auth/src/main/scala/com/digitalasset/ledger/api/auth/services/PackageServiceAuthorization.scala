// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.daml.ledger.api.v1.package_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
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
