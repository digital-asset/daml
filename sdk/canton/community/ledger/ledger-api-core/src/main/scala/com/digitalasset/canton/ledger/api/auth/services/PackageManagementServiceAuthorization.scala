// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.ledger.api.v2.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class PackageManagementServiceAuthorization(
    protected val service: PackageManagementService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends PackageManagementService
    with ProxyCloseable
    with GrpcApiService {

  override def listKnownPackages(
      request: ListKnownPackagesRequest
  ): Future[ListKnownPackagesResponse] =
    authorizer.rpc(service.listKnownPackages)(RequiredClaim.Admin())(request)

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] =
    authorizer.rpc(service.uploadDarFile)(RequiredClaim.Admin())(request)

  override def validateDarFile(request: ValidateDarFileRequest): Future[ValidateDarFileResponse] =
    authorizer.rpc(service.validateDarFile)(RequiredClaim.Admin())(request)

  override def bindService(): ServerServiceDefinition =
    PackageManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
