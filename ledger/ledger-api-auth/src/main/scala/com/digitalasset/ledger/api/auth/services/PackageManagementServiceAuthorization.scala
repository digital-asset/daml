// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.package_management_service._
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[daml] final class PackageManagementServiceAuthorization(
    protected val service: PackageManagementService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends PackageManagementService
    with ProxyCloseable
    with GrpcApiService {

  override def listKnownPackages(
      request: ListKnownPackagesRequest
  ): Future[ListKnownPackagesResponse] =
    authorizer.requireAdminClaims(service.listKnownPackages)(request)

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] =
    authorizer.requireAdminClaims(service.uploadDarFile)(request)

  override def bindService(): ServerServiceDefinition =
    PackageManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
