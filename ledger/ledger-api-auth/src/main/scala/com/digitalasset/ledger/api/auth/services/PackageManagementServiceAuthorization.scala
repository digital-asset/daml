// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.package_management_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition

import scala.concurrent.Future

final class PackageManagementServiceAuthorization(
    protected val service: PackageManagementService with AutoCloseable,
    private val authorizer: Authorizer)
    extends PackageManagementService
    with ProxyCloseable
    with GrpcApiService {

  override def listKnownPackages(
      request: ListKnownPackagesRequest): Future[ListKnownPackagesResponse] =
    authorizer.requireAdminClaims(service.listKnownPackages)(request)

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] =
    authorizer.requireAdminClaims(service.uploadDarFile)(request)

  override def bindService(): ServerServiceDefinition =
    PackageManagementServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
