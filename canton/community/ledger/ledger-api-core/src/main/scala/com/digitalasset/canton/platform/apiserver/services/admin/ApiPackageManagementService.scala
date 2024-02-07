// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.{ContextualizedErrorLogger, DamlError}
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.package_management_service.*
import com.daml.lf.archive.{Dar, DarParser, Decode, GenDarReader}
import com.daml.lf.validation.{TypecheckUpgrades, UpgradeError, Upgrading}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import com.daml.logging.LoggingContext
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.{LedgerOffset, PackageEntry}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.error.PackageServiceErrors.{InternalError, Validation}
import com.digitalasset.canton.ledger.error.groups.AdminServiceErrors
import com.digitalasset.canton.ledger.participant.state.index.v2.{IndexPackagesService, IndexTransactionsService}
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
import scalaz.std.list.*
import scalaz.std.option.*
import scalaz.std.scalaFuture.futureInstance
import scalaz.syntax.traverse.*

import java.util.zip.ZipInputStream
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Try, Success, Failure, Using}
import scala.util.control.NonFatal

private[apiserver] final class ApiPackageManagementService private (
    packagesIndex: IndexPackagesService,
    transactionsService: IndexTransactionsService,
    packageMetadataStore: PackageMetadataStore,
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

  private def lookupDar(pkgId: Ref.PackageId)(implicit
                                                  loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    for {
      optArchive
        <- packagesIndex.getLfArchive(pkgId)
      optPackage
        <- Future.fromTry {
        optArchive.traverse(Decode.decodeArchive(_))
          .handleError(Validation.handleLfArchiveError)
      }
    } yield optPackage
  }

  private def minimalVersionedDar(dar: (Ref.PackageId, Ast.Package))(implicit
                                                                loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val (_, pkg) = dar
    val pkgName = pkg.metadata.name
    val pkgVersion = pkg.metadata.version
    packageMetadataStore.getSnapshot.getUpgradablePackageMap.collect { case (pkgId, (`pkgName`, pkgVersion)) =>
      (pkgId, pkgVersion)
    }.minByOption { case (_, version) =>
      pkgVersion < version
    }.traverse { case (pId, _) => lookupDar(pId) }
    .map(_.flatten)
  }

  private def maximalVersionedDar(dar: (Ref.PackageId, Ast.Package))(implicit
                                                                loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val (_, pkg) = dar
    val pkgName = pkg.metadata.name
    val pkgVersion = pkg.metadata.version
    packageMetadataStore.getSnapshot.getUpgradablePackageMap.collect { case (pkgId, (`pkgName`, pkgVersion)) =>
      (pkgId, pkgVersion)
    }.maxByOption { case (_, version) =>
      pkgVersion > version
    }.traverse { case (pId, _) => lookupDar(pId) }
    .map(_.flatten)
  }

  def strictTypecheckUpgrades(optDar1: Option[(Ref.PackageId, Ast.Package)], pkgId2: Ref.PackageId, optPkg2: Option[Ast.Package])(implicit
                                                                                                                      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    optDar1 match {
      case None =>
        Future.unit

      case Some((pkgId1, pkg1)) =>
        logger.info(s"Package $pkgId1 claims to upgrade package id $pkgId2")
        Future.fromTry(TypecheckUpgrades.typecheckUpgrades((pkgId1, pkg1), pkgId2, optPkg2)).recoverWith {
          case err: UpgradeError =>
            logger.info(s"Typechecking upgrades for $pkgId1 failed with following message: ${err.getMessage}")
            Future.failed(Validation.handleUpgradeError(pkgId1, pkgId2, err).asGrpcError)
          case NonFatal(err) =>
            logger.info(s"Typechecking upgrades failed with unknown error.")
            Future.failed(InternalError.Unhandled(err).asGrpcError)
        }
    }
  }

  def typecheckUpgrades(optDar1: Option[(Ref.PackageId, Ast.Package)], optDar2: Option[(Ref.PackageId, Ast.Package)])(implicit
                                                                                                                      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    (optDar1, optDar2) match {
      case (None, _) | (_, None) =>
        Future.unit

      case (Some((pkgId1, pkg1)), Some((pkgId2, pkg2))) =>
        strictTypecheckUpgrades(Some((pkgId1, pkg1)), pkgId2, Some(pkg2))
    }
  }

  private def validateUpgrade(upgradingDar: Dar[(Ref.PackageId, Ast.Package)])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    val (upgradingPackageId, upgradingPackage) = upgradingDar.main
    val optUpgradingDar = Some((upgradingPackageId, upgradingPackage))
    logger.info(s"Uploading DAR file for $upgradingPackageId in submission ID ${loggingContext.serializeFiltered("submissionId")}.")
    upgradingPackage.metadata.upgradedPackageId match {
      case Some(upgradedPackageId) =>
        for {
          optUpgradedDar <- lookupDar(upgradedPackageId)
          _ <- strictTypecheckUpgrades(optUpgradingDar, upgradedPackageId, optUpgradedDar.map(_._2))
          optMaximalDar <- maximalVersionedDar(upgradingDar.main)
          _ <- typecheckUpgrades(optMaximalDar, optUpgradingDar)
          optMinimalDar <- minimalVersionedDar(upgradingDar.main)
          _ <- typecheckUpgrades(optUpgradingDar, optMinimalDar)
        } yield ()

      case None =>
        logger.info(s"Package $upgradingPackageId does not upgrade anything.")
        Future(())
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
      packageMetadataStore: PackageMetadataStore,
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
      packageMetadataStore,
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
