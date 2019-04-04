// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.package_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.sandbox.services.pkg.PackageServiceBackend
import com.digitalasset.platform.server.api.validation.PackageServiceValidation
import com.digitalasset.platform.common.util.DirectExecutionContext
import io.grpc.{BindableService, ServerServiceDefinition, Status}

import scala.concurrent.{ExecutionContext, Future}

class SandboxPackageService private (backend: PackageServiceBackend)
    extends PackageService
    with GrpcApiService {
  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = ()

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] = {
    Future.successful {
      ListPackagesResponse(backend.installedPackages.toSeq)
    }
  }

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] = {
    backend
      .getPackage(request.packageId)
      .fold(Future.failed[GetPackageResponse](Status.NOT_FOUND.asRuntimeException()))(
        Future.successful(_))
  }

  override def getPackageStatus(
      request: GetPackageStatusRequest): Future[GetPackageStatusResponse] = {
    Future.successful {
      GetPackageStatusResponse {
        if (backend.installedPackages.contains(request.packageId)) {
          PackageStatus.REGISTERED
        } else {
          PackageStatus.UNKNOWN
        }
      }
    }

  }
}

object SandboxPackageService {
  def apply(backend: PackageServiceBackend, ledgerId: String)(implicit ec: ExecutionContext)
    : PackageService with BindableService with PackageServiceLogging =
    new PackageServiceValidation(new SandboxPackageService(backend), ledgerId) with BindableService
    with PackageServiceLogging {
      override def bindService(): ServerServiceDefinition =
        PackageServiceGrpc.bindService(this, DirectExecutionContext)
    }
}
