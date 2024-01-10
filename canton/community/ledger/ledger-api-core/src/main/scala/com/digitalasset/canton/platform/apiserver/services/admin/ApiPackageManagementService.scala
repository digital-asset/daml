// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.{ContextualizedErrorLogger, DamlError}
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.package_management_service.*
import com.daml.lf.archive.{Dar, DarParser, Decode, GenDarReader}
import com.daml.lf.upgrades.{Typecheck, UpgradeError}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import com.daml.logging.LoggingContext
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.{LedgerOffset, PackageEntry}
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
import com.google.protobuf.ByteString
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import scalaz.std.either.*
import scalaz.std.list.*
import scalaz.std.option.*
import scalaz.syntax.traverse.*

import java.util.zip.ZipInputStream
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Try, Success, Failure, Using}

private[apiserver] final class ApiPackageManagementService private (
    packagesIndex: IndexPackagesService,
    transactionsService: IndexTransactionsService,
    packagesWrite: state.WritePackagesService,
    managementServiceTimeout: FiniteDuration,
    engine: Engine,
    darReader: GenDarReader[Archive],
    submissionIdGenerator: String => Ref.SubmissionId,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
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
      contextualizedErrorLogger: ContextualizedErrorLogger,
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
      _ <- validateUpgrade(decodedDar)
    } yield dar
  }

  private def validateUpgrade(upgradingPackage: Dar[(Ref.PackageId, Ast.Package)])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    val upgradingPackageId = upgradingPackage.main._1
    logger.info(s"Uploading DAR file for $upgradingPackageId in submission ID ${loggingContext.serializeFiltered("submissionId")}.")
    upgradingPackage.main._2.metadata.flatMap(_.upgradedPackageId) match {
      case Some(upgradedPackageId) =>
        for {
          upgradedArchiveMb <- packagesIndex.getLfArchive(upgradedPackageId)
          upgradedPackageMb <- Future.fromTry {
            upgradedArchiveMb.traverse(Decode.decodeArchive(_))
              .handleError(Validation.handleLfArchiveError)
          }
          _ <- {
            logger.info(s"Package $upgradingPackageId upgrades package id $upgradedPackageId")
            val upgradeCheckResult = Typecheck.typecheckUpgrades(upgradingPackage.main, upgradedPackageMb)
            upgradeCheckResult match {
              case Success(()) => {
                logger.info(s"Typechecking upgrades for $upgradingPackageId succeeded.")
                Future(())
              }
              case Failure(err: UpgradeError) => {
                logger.info(s"Typechecking upgrades for $upgradingPackageId failed with following message: ${err.message}")
                Future.failed(Validation.handleUpgradeError(upgradingPackageId, upgradedPackageId, err).asGrpcError)
              }
              case Failure(err: Throwable) => {
                logger.info(s"Typechecking upgrades failed with unknown error.")
                Future.failed(err)
              }
            }
          }
        } yield ()
      case None => {
        logger.info(s"Package $upgradingPackageId does not upgrade anything.")
        Future(())
      }
    }
  }

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
    val submissionId = submissionIdGenerator(request.submissionId)
    LoggingContextWithTrace.withEnrichedLoggingContext(telemetry)(
      logging.submissionId(submissionId)
    ) { implicit loggingContext: LoggingContextWithTrace =>
      logger.info(s"Uploading DAR file, ${loggingContext.serializeFiltered("submissionId")}.")

      // a new ErrorLoggingContext (that is overriding the default one derived from NamedLogging) is required to contain the loggingContext entries
      implicit val errorLoggingContext: LedgerErrorLoggingContext =
        LedgerErrorLoggingContext(
          logger,
          loggerFactory.properties ++ loggingContext.toPropertiesMap,
          loggingContext.traceContext,
          submissionId,
        )

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
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): PackageManagementServiceGrpc.PackageManagementService & GrpcApiService =
    new ApiPackageManagementService(
      readBackend,
      transactionsService,
      writeBackend,
      managementServiceTimeout,
      engine,
      darReader,
      submissionIdGenerator,
      telemetry,
      loggerFactory,
    )

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

    override def entries(offset: Option[LedgerOffset.Absolute])(implicit
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
}
