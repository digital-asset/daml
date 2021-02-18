// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.time.Duration
import java.util.UUID
import java.util.zip.ZipInputStream

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.domain.{LedgerOffset, PackageEntry}
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.package_management_service._
import com.daml.ledger.participant.state.index.v2.{
  IndexPackagesService,
  IndexTransactionsService,
  LedgerEndService,
}
import com.daml.ledger.participant.state.v1.{SubmissionId, SubmissionResult, WritePackagesService}
import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.lf.engine.Engine
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.services.logging
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.admin.ApiPackageManagementService._
import com.daml.platform.server.api.validation.ErrorFactories
import com.google.protobuf.timestamp.Timestamp
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[apiserver] final class ApiPackageManagementService private (
    packagesIndex: IndexPackagesService,
    transactionsService: IndexTransactionsService,
    packagesWrite: WritePackagesService,
    managementServiceTimeout: Duration,
    engine: Engine,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends PackageManagementService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val synchronousResponse = new SynchronousResponse(
    new SynchronousResponseStrategy(
      transactionsService,
      packagesIndex,
      packagesWrite,
    ),
    timeToLive = managementServiceTimeout,
  )

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PackageManagementServiceGrpc.bindService(this, executionContext)

  override def listKnownPackages(
      request: ListKnownPackagesRequest
  ): Future[ListKnownPackagesResponse] = {
    logger.info("Listing known packages")
    packagesIndex
      .listLfPackages()
      .map { pkgs =>
        ListKnownPackagesResponse(pkgs.toSeq.map { case (pkgId, details) =>
          PackageDetails(
            pkgId.toString,
            details.size,
            Some(Timestamp(details.knownSince.getEpochSecond, details.knownSince.getNano)),
            details.sourceDescription.getOrElse(""),
          )
        })
      }
      .andThen(logger.logErrorsOnCall[ListKnownPackagesResponse])
  }

  private[this] val darReader = DarReader[Archive] { case (_, x) => Try(Archive.parseFrom(x)) }

  private def decodeAndValidate(stream: ZipInputStream): Try[Dar[Archive]] =
    for {
      dar <- darReader.readArchive("package-upload", stream)
      packages <- Try(dar.all.iterator.map(Decode.decodeArchive).toMap)
      _ <- engine
        .validatePackages(packages.keySet, packages)
        .left
        .map(e => new IllegalArgumentException(e.msg))
        .toTry
    } yield dar

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] =
    withEnrichedLoggingContext(logging.submissionId(request.submissionId)) {
      implicit loggingContext =>
        logger.info("Uploading DAR file")
        val submissionId =
          if (request.submissionId.isEmpty)
            SubmissionId.assertFromString(UUID.randomUUID().toString)
          else
            SubmissionId.assertFromString(request.submissionId)

        val darInputStream = new ZipInputStream(request.darFile.newInput())

        val response = for {
          dar <- decodeAndValidate(darInputStream).fold(
            err => Future.failed(ErrorFactories.invalidArgument(err.getMessage)),
            Future.successful,
          )
          _ <- synchronousResponse.submitAndWait(submissionId, dar)(executionContext, materializer)
        } yield {
          for (archive <- dar.all) {
            logger.info(s"Package ${archive.getHash} successfully uploaded")
          }
          UploadDarFileResponse()
        }

        response.andThen(logger.logErrorsOnCall[UploadDarFileResponse])
    }

}

private[apiserver] object ApiPackageManagementService {

  def createApiService(
      readBackend: IndexPackagesService,
      transactionsService: IndexTransactionsService,
      writeBackend: WritePackagesService,
      managementServiceTimeout: Duration,
      engine: Engine,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): PackageManagementServiceGrpc.PackageManagementService with GrpcApiService =
    new ApiPackageManagementService(
      readBackend,
      transactionsService,
      writeBackend,
      managementServiceTimeout,
      engine,
    )

  private final class SynchronousResponseStrategy(
      ledgerEndService: LedgerEndService,
      packagesIndex: IndexPackagesService,
      packagesWrite: WritePackagesService,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
      extends SynchronousResponse.Strategy[
        Dar[Archive],
        PackageEntry,
        PackageEntry.PackageUploadAccepted,
      ] {

    override def currentLedgerEnd(): Future[Option[LedgerOffset.Absolute]] =
      ledgerEndService.currentLedgerEnd().map(Some(_))

    override def submit(submissionId: SubmissionId, dar: Dar[Archive]): Future[SubmissionResult] =
      packagesWrite.uploadPackages(submissionId, dar.all, None).toScala

    override def entries(offset: Option[LedgerOffset.Absolute]): Source[PackageEntry, _] =
      packagesIndex.packageEntries(offset)

    override def accept(
        submissionId: SubmissionId
    ): PartialFunction[PackageEntry, PackageEntry.PackageUploadAccepted] = {
      case entry @ PackageEntry.PackageUploadAccepted(`submissionId`, _) => entry
    }

    override def reject(
        submissionId: SubmissionId
    ): PartialFunction[PackageEntry, StatusRuntimeException] = {
      case PackageEntry.PackageUploadRejected(`submissionId`, _, reason) =>
        ErrorFactories.invalidArgument(reason)
    }
  }

}
