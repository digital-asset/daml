// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.admin

import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.zip.ZipInputStream

import akka.actor.Scheduler
import akka.stream.ActorMaterializer
import com.daml.ledger.participant.state.index.v2.IndexPackagesService
import com.daml.ledger.participant.state.v1.{SubmissionId, SubmissionResult, WritePackagesService}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.ledger.api.v1.admin.package_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.common.util.{DirectExecutionContext => DE}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.google.protobuf.timestamp.Timestamp
import io.grpc.ServerServiceDefinition
import org.slf4j.Logger

import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Try

class ApiPackageManagementService(
    packagesIndex: IndexPackagesService,
    packagesWrite: WritePackagesService,
    scheduler: Scheduler,
    loggerFactory: NamedLoggerFactory)
    extends PackageManagementService
    with GrpcApiService {

  protected val logger: Logger = loggerFactory.getLogger(PackageManagementService.getClass)

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
    val submissionId = UUID.randomUUID().toString
    val resultT = for {
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
        .readArchive(
          "package-upload",
          new ZipInputStream(new ByteArrayInputStream(request.darFile.toByteArray)))
    } yield {
      (packagesWrite.uploadPackages(dar.all, None, submissionId), dar.all.map(_.getHash))
    }
    resultT.fold(
      err => Future.failed(ErrorFactories.invalidArgument(err.getMessage)),
      res =>
        FutureConverters
          .toScala(res._1)
          .flatMap {
            case SubmissionResult.Acknowledged =>
              pollForPackageUploadResult(Ref.LedgerString.assertFromString(submissionId)).flatMap {
                case domain.PackageUploadEntry.Accepted(_, _) =>
                  Future.successful(UploadDarFileResponse())
                case domain.PackageUploadEntry.Rejected(_, _, reason) =>
                  Future.failed(ErrorFactories.invalidArgument(reason))
              }(DE)
            case r @ SubmissionResult.Overloaded =>
              Future.failed(ErrorFactories.resourceExhausted(r.description))
            case r @ SubmissionResult.InternalError(_) =>
              Future.failed(ErrorFactories.internal(r.reason))
            case r @ SubmissionResult.NotSupported =>
              Future.failed(ErrorFactories.unimplemented(r.description))
          }(DE)
    )
  }

  private def pollForPackageUploadResult(
      submissionId: SubmissionId): Future[domain.PackageUploadEntry] = {
    PollingUtils
      .pollSingleUntilPersisted(() => packagesIndex.lookupPackageUploadEntry(submissionId))(
        s"submissionId $submissionId",
        50.milliseconds,
        500.milliseconds,
        d => d * 2,
        scheduler,
        loggerFactory)
      .map {
        case (numberOfAttempts, result) =>
          logger.debug(
            s"submissionId $submissionId available, read after $numberOfAttempts attempt(s)")
          result
      }(DE)
  }

}

object ApiPackageManagementService {
  def createApiService(
      readBackend: IndexPackagesService,
      writeBackend: WritePackagesService,
      loggerFactory: NamedLoggerFactory)(implicit mat: ActorMaterializer)
    : PackageManagementServiceGrpc.PackageManagementService with GrpcApiService =
    new ApiPackageManagementService(readBackend, writeBackend, mat.system.scheduler, loggerFactory)
    with PackageManagementServiceLogging
}
