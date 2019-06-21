// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.admin

import com.daml.ledger.participant.state.index.v2.IndexPackagesService
import com.daml.ledger.participant.state.v2.{
  UploadDarRejectionReason,
  UploadDarResult,
  WritePackagesService
}
import com.digitalasset.ledger.api.v1.admin.package_management_service._
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.google.protobuf.timestamp.Timestamp
import com.digitalasset.platform.common.util.{DirectExecutionContext => DE}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.compat.java8.FutureConverters
import scala.concurrent.Future

class ApiPackageManagementService(
    packagesIndex: IndexPackagesService,
    packagesWrite: WritePackagesService)
    extends PackageManagementService
    with GrpcApiService {

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PackageManagementServiceGrpc.bindService(this, DE)

  override def listKnownPackages(
      request: ListKnownPackagesRequest): Future[ListKnownPackagesResponse] = {
    packagesIndex
      .listLfPackages()
      .map { pkgs =>
        ListKnownPackagesResponse(pkgs.toSeq.map {
          case (pkgId, details) =>
            PackageDetails(
              pkgId.toString,
              details.size,
              Some(Timestamp(details.knownSince.getEpochSecond, details.knownSince.getNano)),
              details.sourceDescription)
        })
      }(DE)
  }

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
    FutureConverters
      .toScala(packagesWrite.uploadDar("", request.darFile.toByteArray))
      .flatMap {
        case UploadDarResult.Ok =>
          Future.successful(UploadDarFileResponse())
        case UploadDarResult.Rejected(reason @ UploadDarRejectionReason.InvalidPackage(_)) =>
          Future.failed(ErrorFactories.invalidArgument(reason.description))
        case UploadDarResult.Rejected(reason @ UploadDarRejectionReason.ParticipantNotAuthorized) =>
          Future.failed(ErrorFactories.permissionDenied(reason.description))
      }(DE)
  }
}

object ApiPackageManagementService {
  def createApiService(
      readBackend: IndexPackagesService,
      writeBackend: WritePackagesService): GrpcApiService with BindableService = {
    new ApiPackageManagementService(readBackend, writeBackend) with BindableService
  }
}
