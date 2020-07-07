// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.zip.ZipInputStream

import akka.actor.Scheduler
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.index.v2.{IndexPackagesService, IndexTransactionsService}
import com.daml.ledger.participant.state.v1.{SubmissionId, SubmissionResult, WritePackagesService}
import com.daml.api.util.TimeProvider
import com.daml.lf.archive.DarReader
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.dec.{DirectExecutionContext => DE}
import com.daml.ledger.api.domain.{LedgerOffset, PackageEntry}
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.package_management_service._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.ErrorFactories
import com.google.protobuf.timestamp.Timestamp
import io.grpc.ServerServiceDefinition

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

final class ApiPackageManagementService private (
    packagesIndex: IndexPackagesService,
    transactionsService: IndexTransactionsService,
    packagesWrite: WritePackagesService,
    timeProvider: TimeProvider,
    materializer: Materializer,
    scheduler: Scheduler)(implicit logCtx: LoggingContext)
    extends PackageManagementService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

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
      .andThen(logger.logErrorsOnCall[ListKnownPackagesResponse])(DE)
  }

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
    val submissionId =
      if (request.submissionId.isEmpty)
        SubmissionId.assertFromString(UUID.randomUUID().toString)
      else
        SubmissionId.assertFromString(request.submissionId)

    // Amount of time we wait for the ledger to commit the request before we
    // give up on polling for the result.
    // TODO(JM): This constant should be replaced by user-provided maximum record time
    // which should be wired through the stack and verified during validation, just like
    // with transactions. I'm leaving this for another PR.
    val timeToLive = 30.seconds

    implicit val ec: ExecutionContext = DE
    val uploadDarFileResponse = for {
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
        .readArchive(
          "package-upload",
          new ZipInputStream(new ByteArrayInputStream(request.darFile.toByteArray)))
        .fold(
          err => Future.failed(ErrorFactories.invalidArgument(err.getMessage)),
          Future.successful
        )
      ledgerEndBeforeRequest <- transactionsService.currentLedgerEnd()
      submissionResult <- FutureConverters.toScala(
        packagesWrite.uploadPackages(submissionId, dar.all, None)
      )
      response <- submissionResult match {
        case SubmissionResult.Acknowledged =>
          pollUntilPersisted(submissionId, timeToLive, ledgerEndBeforeRequest).flatMap {
            case _: PackageEntry.PackageUploadAccepted =>
              Future.successful(UploadDarFileResponse())
            case PackageEntry.PackageUploadRejected(_, _, reason) =>
              Future.failed(ErrorFactories.invalidArgument(reason))
          }
        case r @ SubmissionResult.Overloaded =>
          Future.failed(ErrorFactories.resourceExhausted(r.description))
        case r @ SubmissionResult.InternalError(_) =>
          Future.failed(ErrorFactories.internal(r.reason))
        case r @ SubmissionResult.NotSupported =>
          Future.failed(ErrorFactories.unimplemented(r.description))
      }
    } yield response
    uploadDarFileResponse.andThen(logger.logErrorsOnCall[UploadDarFileResponse])
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
      timeProvider: TimeProvider)(implicit mat: Materializer, logCtx: LoggingContext)
    : PackageManagementServiceGrpc.PackageManagementService with GrpcApiService =
    new ApiPackageManagementService(
      readBackend,
      transactionsService,
      writeBackend,
      timeProvider,
      mat,
      mat.system.scheduler)
}
