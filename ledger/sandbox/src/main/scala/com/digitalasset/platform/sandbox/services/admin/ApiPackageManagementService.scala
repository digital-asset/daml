// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.admin

import java.io.{File, FileOutputStream}
import java.util.zip.ZipFile

import com.daml.ledger.participant.state.index.v2.IndexPackagesService
import com.daml.ledger.participant.state.v2.{UploadPackagesResult, WritePackagesService}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.TryOps.Bracket.bracket
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.v1.admin.package_management_service._
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.google.protobuf.timestamp.Timestamp
import com.digitalasset.platform.common.util.{DirectExecutionContext => DE}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.util.Try

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
              details.sourceDescription.getOrElse(""))
        })
      }(DE)
  }

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
    val resultT = for {
      file <- Try(File.createTempFile("uploadDarFile", ".dar"))
      _ <- bracket(Try(new FileOutputStream(file)))(fos => Try(fos.close())).flatMap { fos =>
        Try(fos.write(request.darFile.toByteArray))
      }
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }.readArchive(new ZipFile(file))
    } yield {
      packagesWrite.uploadPackages(dar.all, None)
    }
    resultT.fold(
      err => Future.failed(ErrorFactories.invalidArgument(err.getMessage)),
      res =>
        FutureConverters
          .toScala(res)
          .flatMap {
            case UploadPackagesResult.Ok =>
              Future.successful(UploadDarFileResponse())
            case r @ UploadPackagesResult.InvalidPackage(_) =>
              Future.failed(ErrorFactories.invalidArgument(r.description))
            case r @ UploadPackagesResult.ParticipantNotAuthorized =>
              Future.failed(ErrorFactories.permissionDenied(r.description))
          }(DE)
    )
  }
}

object ApiPackageManagementService {
  def createApiService(
      readBackend: IndexPackagesService,
      writeBackend: WritePackagesService): GrpcApiService with BindableService = {
    new ApiPackageManagementService(readBackend, writeBackend) with BindableService
  }
}
