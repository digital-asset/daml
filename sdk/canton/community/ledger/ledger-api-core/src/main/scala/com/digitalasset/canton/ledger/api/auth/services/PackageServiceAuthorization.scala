// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.package_service.*
import com.daml.ledger.api.v2.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class PackageServiceAuthorization(
    protected val service: PackageService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends PackageService
    with ProxyCloseable
    with GrpcApiService {

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
    authorizer.rpc(service.listPackages)(RequiredClaim.Public())(request)

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    authorizer.rpc(service.getPackage)(RequiredClaim.Public())(request)

  override def getPackageStatus(
      request: GetPackageStatusRequest
  ): Future[GetPackageStatusResponse] =
    authorizer.rpc(service.getPackageStatus)(RequiredClaim.Public())(request)

  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, executionContext)
}
