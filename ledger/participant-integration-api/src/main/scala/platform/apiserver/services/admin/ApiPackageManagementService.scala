// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.api.util.TimestampConversion
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.definitions.PackageServiceError.Validation
import com.daml.error.definitions.{DamlError, LedgerApiErrors}
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.domain.{LedgerOffset, PackageEntry}
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.package_management_service._
import com.daml.ledger.participant.state.index.v2.{
  IndexPackagesService,
  IndexTransactionsService,
  LedgerEndService,
}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.archive.{Dar, DarParser, Decode, GenDarReader}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.admin.ApiPackageManagementService._
import com.daml.platform.apiserver.services.logging
import com.daml.telemetry.{DefaultTelemetry, TelemetryContext}
import com.google.protobuf.ByteString
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

import scala.util.Using
import java.util.zip.ZipInputStream
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Try

private[apiserver] final class ApiPackageManagementService private (
    packagesIndex: IndexPackagesService,
    transactionsService: IndexTransactionsService,
    packagesWrite: state.WritePackagesService,
    managementServiceTimeout: FiniteDuration,
    engine: Engine,
    darReader: GenDarReader[Archive],
    submissionIdGenerator: String => Ref.SubmissionId,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends PackageManagementService
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

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
            Some(TimestampConversion.fromLf(details.knownSince)),
            details.sourceDescription.getOrElse(""),
          )
        })
      }
      .andThen(logger.logErrorsOnCall[ListKnownPackagesResponse])
  }

  private def decodeAndValidate(
      darFile: ByteString
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Dar[Archive]] = Future.delegate {
    // Triggering computation in `executionContext` as caller thread (from netty)
    // should not be busy with heavy computation
    val result = for {
      darArchive <- Using(new ZipInputStream(darFile.newInput())) { stream =>
        darReader.readArchive("package-upload", stream)
      }
      dar <- darArchive.handleError(Validation.handleLfArchiveError)
      packages <- dar.all
        .traverse(Decode.decodeArchive(_))
        .handleError(Validation.handleLfArchiveError)
      _ <- engine
        .validatePackages(packages.toMap)
        .handleError(Validation.handleLfEnginePackageError)
    } yield dar
    Future.fromTry(result)
  }

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
    val submissionId = submissionIdGenerator(request.submissionId)
    withEnrichedLoggingContext(logging.submissionId(submissionId)) { implicit loggingContext =>
      logger.info("Uploading DAR file")

      implicit val telemetryContext: TelemetryContext =
        DefaultTelemetry.contextFromGrpcThreadLocalContext()

      implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
        new DamlContextualizedErrorLogger(
          logger,
          loggingContext,
          Some(submissionId),
        )

      val response = for {
        dar <- decodeAndValidate(request.darFile)
        _ <- synchronousResponse.submitAndWait(submissionId, dar)
      } yield {
        for (archive <- dar.all) {
          logger.info(s"Package ${archive.getHash} successfully uploaded")
        }
        UploadDarFileResponse()
      }
      response.andThen(logger.logErrorsOnCall[UploadDarFileResponse])
    }
  }

  private implicit class ErrorValidations[E, R](result: Either[E, R]) {
    def handleError(toSelfServiceErrorCode: E => DamlError): Try[R] =
      result.left.map { err =>
        toSelfServiceErrorCode(err).asGrpcError
      }.toTry
  }
}

private[apiserver] object ApiPackageManagementService {

  def createApiService(
      readBackend: IndexPackagesService,
      transactionsService: IndexTransactionsService,
      writeBackend: state.WritePackagesService,
      managementServiceTimeout: FiniteDuration,
      engine: Engine,
      darReader: GenDarReader[Archive] = DarParser,
      submissionIdGenerator: String => Ref.SubmissionId = augmentSubmissionId,
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
      darReader,
      submissionIdGenerator,
    )

  private final class SynchronousResponseStrategy(
      ledgerEndService: LedgerEndService,
      packagesIndex: IndexPackagesService,
      packagesWrite: state.WritePackagesService,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
      extends SynchronousResponse.Strategy[
        Dar[Archive],
        PackageEntry,
        PackageEntry.PackageUploadAccepted,
      ] {
    private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

    override def currentLedgerEnd(): Future[Option[LedgerOffset.Absolute]] =
      ledgerEndService.currentLedgerEnd().map(Some(_))

    override def submit(submissionId: Ref.SubmissionId, dar: Dar[Archive])(implicit
        telemetryContext: TelemetryContext,
        loggingContext: LoggingContext,
    ): Future[state.SubmissionResult] =
      packagesWrite.uploadPackages(submissionId, dar.all, None).asScala

    override def entries(offset: Option[LedgerOffset.Absolute]): Source[PackageEntry, _] =
      packagesIndex.packageEntries(offset)

    override def accept(
        submissionId: Ref.SubmissionId
    ): PartialFunction[PackageEntry, PackageEntry.PackageUploadAccepted] = {
      case entry @ PackageEntry.PackageUploadAccepted(`submissionId`, _) => entry
    }

    override def reject(
        submissionId: Ref.SubmissionId
    ): PartialFunction[PackageEntry, StatusRuntimeException] = {
      case PackageEntry.PackageUploadRejected(`submissionId`, _, reason) =>
        LedgerApiErrors.Admin.PackageUploadRejected
          .Reject(reason)(
            new DamlContextualizedErrorLogger(logger, loggingContext, Some(submissionId))
          )
          .asGrpcError
    }
  }
}
