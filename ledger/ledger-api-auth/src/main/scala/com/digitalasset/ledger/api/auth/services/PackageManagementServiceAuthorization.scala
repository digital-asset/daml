// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.services

import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.ledger.api.v1.admin.package_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class PackageManagementServiceAuthorization(
    protected val service: PackageManagementService with AutoCloseable,
    protected val authService: AuthService)
    extends PackageManagementService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(PackageManagementService.getClass)

  override def listKnownPackages(
      request: ListKnownPackagesRequest): Future[ListKnownPackagesResponse] =
    ApiServiceAuthorization
      .requireAdminClaims()
      .fold(Future.failed(_), _ => service.listKnownPackages(request))

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] =
    ApiServiceAuthorization
      .requireAdminClaims()
      .fold(Future.failed(_), _ => service.uploadDarFile(request))

  override def bindService(): ServerServiceDefinition =
    PackageManagementServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
