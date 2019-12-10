// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.admin

import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream
import java.util.UUID

import akka.actor.Scheduler
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.{SubmissionId, SubmissionResult, WritePackagesService}
import com.daml.ledger.participant.state.index.v2.{
  IndexPackagesService, IndexTransactionsService
}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.ledger.api.v1.admin.package_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.common.util.{DirectExecutionContext => DE}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.google.protobuf.timestamp.Timestamp
import com.digitalasset.ledger.api.domain.{LedgerOffset, PackageEntry}
import com.digitalasset.api.util.{TimeProvider, TimestampConversion}

import io.grpc.ServerServiceDefinition
import org.slf4j.Logger
import com.digitalasset.daml.lf.data.Time

import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

 class ApiPackageManagementService(
    packagesIndex: IndexPackagesService,
    transactionsService: IndexTransactionsService,
    packagesWrite: WritePackagesService,
    timeProvider: TimeProvider,
    materializer: Materializer,
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
    val submissionId =
      if (request.submissionId.isEmpty)
        SubmissionId.assertFromString(UUID.randomUUID().toString)
      else
        SubmissionId.assertFromString(request.submissionId)

    // TODO(JM): Implement computation of maximum record time from
    // the current configuration. I am leaving this to another PR as
    // we need this change in other services as well and this PR is
    // large enough. For now, we'll default to 1 minute.
    val timeToLive = 60.seconds
    val maxRecordTime =
      Time.Timestamp.assertFromInstant(
        request
          .maximumRecordTime
          .map(TimestampConversion.toInstant)
          .getOrElse(timeProvider.getCurrentTime.plusNanos(timeToLive.toNanos))
      )

    val resultT = for {
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
        .readArchive(
          "package-upload",
          new ZipInputStream(new ByteArrayInputStream(request.darFile.toByteArray)))
    } yield {
      packagesWrite.uploadPackages(submissionId, maxRecordTime, dar.all, None)
    }

    resultT.fold(
      err => Future.failed(ErrorFactories.invalidArgument(err.getMessage)),
      res =>
      transactionsService
        .currentLedgerEnd()
        .flatMap { ledgerEndBeforeRequest =>
          FutureConverters
            .toScala(res)
            .flatMap {
              case SubmissionResult.Acknowledged =>
                pollUntilPersisted(submissionId, timeToLive, ledgerEndBeforeRequest).flatMap {
                  case _: PackageEntry.PackageUploadAccepted =>
                    Future.successful(UploadDarFileResponse())
                  case PackageEntry.PackageUploadRejected(_, _, reason) =>
                    Future.failed(ErrorFactories.invalidArgument(reason))
                }(DE)
              case r @ SubmissionResult.Overloaded =>
                Future.failed(ErrorFactories.resourceExhausted(r.description))
              case r @ SubmissionResult.InternalError(_) =>
                Future.failed(ErrorFactories.internal(r.reason))
              case r @ SubmissionResult.NotSupported =>
                Future.failed(ErrorFactories.unimplemented(r.description))
            }(DE)
        }(DE)
    )
  }

  private def pollUntilPersisted(
      submissionId: SubmissionId,
      timeToLive: FiniteDuration,
      offset: LedgerOffset.Absolute): Future[PackageEntry] = {
    packagesIndex
      .packageEntries(offset)
      .collect {
        case entry @ PackageEntry.PackageUploadAccepted(`submissionId`, _) => entry
        case entry @ PackageEntry.PackageUploadRejected(`submissionId`, _, _) => entry
      }
      .completionTimeout(timeToLive)
      .runWith(Sink.head)(materializer)
    }
}

object ApiPackageManagementService {
  def createApiService(
      readBackend: IndexPackagesService,
      transactionsService: IndexTransactionsService,
      writeBackend: WritePackagesService,
      timeProvider: TimeProvider,
      loggerFactory: NamedLoggerFactory)(implicit mat: ActorMaterializer)
    : PackageManagementServiceGrpc.PackageManagementService with GrpcApiService =
    new ApiPackageManagementService(readBackend, transactionsService, writeBackend, timeProvider, mat, mat.system.scheduler, loggerFactory)
    with PackageManagementServiceLogging
}
