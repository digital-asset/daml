// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream

import com.daml.ledger.participant.state.index.v1.{PackagesService => IndexPackageService}
import com.daml.ledger.participant.state.v1.{UploadPackagesResult, WritePackagesService}
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.ledger.api.v1.admin.package_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.validation.ErrorFactories
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class DamlOnXPackageManagementService(
    writeService: WritePackagesService,
    indexService: IndexPackageService)
    extends PackageManagementService
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(PackageManagementService.getClass)
  implicit val ec: ExecutionContext = DirectExecutionContext

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PackageManagementServiceGrpc.bindService(this, DirectExecutionContext)

  override def listKnownPackages(
      request: ListKnownPackagesRequest): Future[ListKnownPackagesResponse] = {

    indexService.listPackageDetails().map { pkgs =>
      ListKnownPackagesResponse(pkgs.toSeq.map {
        case (id, details) =>
          PackageDetails(
            id,
            details.size,
            Some(fromInstant(details.knownSince.toInstant)),
            details.sourceDescription.getOrElse(""))
      })
    }
  }

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
    val resultT = for {
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
        .readArchive(
          "package-upload",
          new ZipInputStream(new ByteArrayInputStream(request.darFile.toByteArray)))
    } yield {
      writeService.uploadPackages(dar.all, None)
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
            case r @ UploadPackagesResult.NotSupported =>
              Future.failed(ErrorFactories.unimplemented(r.description))
        }
    )
  }
}

object DamlOnXPackageManagementService {
  def apply(writeService: WritePackagesService, indexService: IndexPackageService): GrpcApiService =
    new DamlOnXPackageManagementService(writeService, indexService)
    with PackageManagementServiceLogging
}
