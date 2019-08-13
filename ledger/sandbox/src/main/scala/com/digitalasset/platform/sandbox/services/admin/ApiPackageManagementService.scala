// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.admin

import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream

import akka.actor.Scheduler
import akka.stream.ActorMaterializer
import com.daml.ledger.participant.state.index.v2.IndexPackagesService
import com.daml.ledger.participant.state.v1.{UploadPackagesResult, WritePackagesService}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.ledger.api.v1.admin.package_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.{DirectExecutionContext => DE}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.google.protobuf.timestamp.Timestamp
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Try

class ApiPackageManagementService(
    packagesIndex: IndexPackagesService,
    packagesWrite: WritePackagesService,
    scheduler: Scheduler)
    extends PackageManagementService
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(PackageManagementService.getClass)

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
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
        .readArchive(
          "package-upload",
          new ZipInputStream(new ByteArrayInputStream(request.darFile.toByteArray)))
    } yield {
      (packagesWrite.uploadPackages(dar.all, None), dar.all.map(_.getHash))
    }
    resultT.fold(
      err => Future.failed(ErrorFactories.invalidArgument(err.getMessage)),
      res =>
        FutureConverters
          .toScala(res._1)
          .flatMap {
            case UploadPackagesResult.Ok =>
              Future.successful(UploadDarFileResponse())
            case r @ UploadPackagesResult.InvalidPackage(_) =>
              Future.failed(ErrorFactories.invalidArgument(r.description))
            case r @ UploadPackagesResult.ParticipantNotAuthorized =>
              Future.failed(ErrorFactories.permissionDenied(r.description))
            case r @ UploadPackagesResult.NotSupported =>
              Future.failed(ErrorFactories.unimplemented(r.description))
          }(DE)
          .flatMap(pollUntilPersisted(res._2, _))(DE)
    )
  }

  /**
    * Wraps a call [[PollingUtils.pollUntilPersisted]] so that it can be chained on the package upload with a `flatMap`.
    *
    * Checks invariants and forwards the original result after all packages are found to be persisted.
    *
    * @param ids The IDs of the uploaded packages
    * @return The result of the party allocation received originally, wrapped in a [[Future]]
    */
  private def pollUntilPersisted(
      ids: List[String],
      result: UploadDarFileResponse): Future[UploadDarFileResponse] = {
    val newPackages = ids.toSet
    val description = s"packages ${ids.mkString(", ")}"

    PollingUtils
      .pollUntilPersisted(packagesIndex.listLfPackages _)(
        x => newPackages.subsetOf(x.keySet.toSet),
        description,
        50.milliseconds,
        500.milliseconds,
        d => d * 2,
        scheduler)
      .map { numberOfAttempts =>
        logger.debug(
          s"All ${ids.length} packages available, read after $numberOfAttempts attempt(s)")
        result
      }(DE)
  }
}

object ApiPackageManagementService {
  def createApiService(readBackend: IndexPackagesService, writeBackend: WritePackagesService)(
      implicit mat: ActorMaterializer): GrpcApiService =
    new ApiPackageManagementService(readBackend, writeBackend, mat.system.scheduler)
    with PackageManagementServiceLogging
}
