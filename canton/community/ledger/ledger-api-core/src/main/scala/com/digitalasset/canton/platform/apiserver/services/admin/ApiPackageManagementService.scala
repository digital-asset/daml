// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.lf2.archive.daml_lf_dev.DamlLf.Archive
import com.daml.error.DamlError
import com.daml.ledger.api.v2.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.lf.archive.{Dar, DarParser, Decode, GenDarReader}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.{PackageEntry, ParticipantOffset}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.error.PackageServiceErrors.Validation
import com.digitalasset.canton.ledger.error.groups.AdminServiceErrors
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  IndexPackagesService,
  IndexTransactionsService,
}
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  LedgerErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPackageManagementService.*
import com.digitalasset.canton.platform.apiserver.services.logging
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadataStore
import com.google.protobuf.ByteString
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import scalaz.std.either.*
import scalaz.syntax.traverse.*

import java.util.zip.ZipInputStream
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Try, Using}

private[apiserver] final class ApiPackageManagementService private (
    packagesIndex: IndexPackagesService,
    transactionsService: IndexTransactionsService,
    packagesWrite: state.WritePackagesService,
    packageUpgradeValidator: PackageUpgradeValidator,
    managementServiceTimeout: FiniteDuration,
    engine: Engine,
    darReader: GenDarReader[Archive],
    submissionIdGenerator: String => Ref.SubmissionId,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
    disableUpgradeValidation: Boolean,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
) extends PackageManagementService
    with GrpcApiService
    with NamedLogging {

  private implicit val loggingContext: LoggingContext =
    createLoggingContext(loggerFactory)(identity)

  private val synchronousResponse = new SynchronousResponse(
    new SynchronousResponseStrategy(
      packagesIndex,
      packagesWrite,
      loggerFactory,
    ),
    loggerFactory,
  )

  override def close(): Unit = synchronousResponse.close()

  override def bindService(): ServerServiceDefinition =
    PackageManagementServiceGrpc.bindService(this, executionContext)

  override def listKnownPackages(
      request: ListKnownPackagesRequest
  ): Future[ListKnownPackagesResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    logger.info("Listing known packages.")
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
      loggingContext: LoggingContextWithTrace
  ): Future[Dar[Archive]] = Future.delegate {
    // Triggering computation in `executionContext` as caller thread (from netty)
    // should not be busy with heavy computation
    for {
      (dar, decodedDar) <- Future.fromTry(
        for {
          darArchive <- Using(new ZipInputStream(darFile.newInput())) { stream =>
            darReader.readArchive("package-upload", stream)
          }
          dar <- darArchive.handleError(Validation.handleLfArchiveError)
          decodedDar <- dar
            .traverse(Decode.decodeArchive(_))
            .handleError(Validation.handleLfArchiveError)
          _ <- engine
            .validatePackages(decodedDar.all.toMap)
            .handleError(Validation.handleLfEnginePackageError)
        } yield (dar, decodedDar)
      )
      _ <-
        if (disableUpgradeValidation) {
          logger.info(s"Skipping upgrade validation for package ${decodedDar.main._1}.")
          Future { () }
        } else
          packageUpgradeValidator.validateUpgrade(decodedDar.main)
    } yield dar
  }

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
    val submissionId = submissionIdGenerator(request.submissionId)
    LoggingContextWithTrace.withEnrichedLoggingContext(telemetry)(
      logging.submissionId(submissionId)
    ) { implicit loggingContext: LoggingContextWithTrace =>
      logger.info(s"Uploading DAR file, ${loggingContext.serializeFiltered("submissionId")}.")

      val response = for {
        dar <- decodeAndValidate(request.darFile)
        ledgerEndbeforeRequest <- transactionsService.currentLedgerEnd().map(Some(_))
        _ <- synchronousResponse.submitAndWait(
          submissionId,
          dar,
          ledgerEndbeforeRequest,
          managementServiceTimeout,
        )
      } yield {
        for (archive <- dar.all) {
          logger.info(s"Package ${archive.getHash} successfully uploaded.")
        }
        UploadDarFileResponse()
      }
      response.andThen(logger.logErrorsOnCall[UploadDarFileResponse])
    }
  }
}

private[apiserver] object ApiPackageManagementService {

  def createApiService(
      readBackend: IndexPackagesService,
      transactionsService: IndexTransactionsService,
      packageMetadataStore: PackageMetadataStore,
      writeBackend: state.WritePackagesService,
      managementServiceTimeout: FiniteDuration,
      engine: Engine,
      darReader: GenDarReader[Archive] = DarParser,
      submissionIdGenerator: String => Ref.SubmissionId = augmentSubmissionId,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
      disableUpgradeValidation: Boolean,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): PackageManagementServiceGrpc.PackageManagementService & GrpcApiService = {
    val packageUpgradeValidator =
      new PackageUpgradeValidator(
        getPackageMap = _ => Right(packageMetadataStore.getSnapshot.getUpgradablePackageMap),
        getLfArchive = implicit loggingContextWithTrace => pkgId => readBackend.getLfArchive(pkgId),
        loggerFactory = loggerFactory,
      )
    new ApiPackageManagementService(
      readBackend,
      transactionsService,
      writeBackend,
      packageUpgradeValidator,
      managementServiceTimeout,
      engine,
      darReader,
      submissionIdGenerator,
      telemetry,
      loggerFactory,
      disableUpgradeValidation,
    )
  }

  private final class SynchronousResponseStrategy(
      packagesIndex: IndexPackagesService,
      packagesWrite: state.WritePackagesService,
      val loggerFactory: NamedLoggerFactory,
  ) extends SynchronousResponse.Strategy[
        Dar[Archive],
        PackageEntry,
        PackageEntry.PackageUploadAccepted,
      ]
      with NamedLogging {

    override def submit(submissionId: Ref.SubmissionId, dar: Dar[Archive])(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[state.SubmissionResult] =
      packagesWrite.uploadPackages(submissionId, dar.all, None).asScala

    override def entries(offset: Option[ParticipantOffset.Absolute])(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[PackageEntry, ?] =
      packagesIndex.packageEntries(offset)

    override def accept(
        submissionId: Ref.SubmissionId
    ): PartialFunction[PackageEntry, PackageEntry.PackageUploadAccepted] = {
      case entry @ PackageEntry.PackageUploadAccepted(`submissionId`, _) => entry
    }

    override def reject(
        submissionId: Ref.SubmissionId
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): PartialFunction[PackageEntry, StatusRuntimeException] = {
      case PackageEntry.PackageUploadRejected(`submissionId`, _, reason) =>
        AdminServiceErrors.PackageUploadRejected
          .Reject(reason)(
            LedgerErrorLoggingContext(
              logger,
              loggingContext.toPropertiesMap,
              loggingContext.traceContext,
              submissionId,
            )
          )
          .asGrpcError
    }
  }

  implicit class ErrorValidations[E, R](result: Either[E, R]) {
    def handleError(toSelfServiceErrorCode: E => DamlError): Try[R] =
      result.left.map { err =>
        toSelfServiceErrorCode(err).asGrpcError
      }.toTry
  }
}
