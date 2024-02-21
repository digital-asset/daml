// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.DamlError
import com.daml.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v1.admin.package_management_service.*
import com.daml.lf.archive.{Dar, DarParser, Decode, GenDarReader}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import com.daml.lf.validation.{TypecheckUpgrades, UpgradeError, Upgrading}
import com.daml.logging.LoggingContext
import com.daml.logging.entries.LoggingValue.OfString
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.{PackageEntry, ParticipantOffset}
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
import scalaz.std.option.*
import scalaz.std.scalaFuture.futureInstance
import scalaz.syntax.traverse.*

import java.util.zip.ZipInputStream
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success, Try, Using}
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

  private def existingVersionedPackageId(pkg: Ast.Package, packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]): Option[Ref.PackageId] = {
    val pkgName = pkg.metadata.name
    val pkgVersion = pkg.metadata.version
    packageMap.collectFirst { case (pkgId, (`pkgName`, `pkgVersion`)) => pkgId }
  }

  private def minimalVersionedDar(pkg: Ast.Package, packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)])(implicit
                                                                loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val pkgName = pkg.metadata.name
    val pkgVersion = pkg.metadata.version
    packageMap.collect { case (pkgId, (`pkgName`, pkgVersion)) =>
      (pkgId, pkgVersion)
    }
      .filter { case (_, version) => pkgVersion < version }
      .minByOption { case (_, version) => version }
      .traverse { case (pId, _) => lookupDar(pId) }
      .map(_.flatten)
  }

  private def maximalVersionedDar(upgradedPackageId: Ref.PackageId, pkg: Ast.Package, packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)])(implicit
                                                                loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val pkgName = pkg.metadata.name
    val pkgVersion = pkg.metadata.version
    packageMap.collect { case (pkgId, (`pkgName`, pkgVersion)) =>
      (pkgId, pkgVersion)
    }
      .filter { case (pkgId, version) => pkgVersion > version && upgradedPackageId != pkgId }
      .maxByOption { case (_, version) => version }
      .traverse { case (pId, _) => lookupDar(pId) }
      .map(_.flatten)
  }

  private def strictTypecheckUpgrades(phase: TypecheckUpgrades.UploadPhaseCheck, optDar1: Option[(Ref.PackageId, Ast.Package)], pkgId2: Ref.PackageId, optPkg2: Option[Ast.Package])(implicit
                                                                                                                      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    LoggingContextWithTrace.withEnrichedLoggingContext("upgradeTypecheckPhase" -> OfString(phase.toString)) { implicit loggingContext =>
      optDar1 match {
        case None =>
          Future.unit

        case Some((pkgId1, pkg1)) =>
          logger.info(s"Package $pkgId1 claims to upgrade package id $pkgId2")
          Future.fromTry(TypecheckUpgrades.typecheckUpgrades((pkgId1, pkg1), pkgId2, optPkg2)).recoverWith {
            case err: UpgradeError =>
              Future.failed(
                Validation.Upgradeability
                  .Error(pkgId2, pkgId1, err)
                  .asGrpcError
              )
            case NonFatal(err) =>
              Future.failed(
                InternalError
                  .Unhandled(
                    err,
                    Some(
                      s"Typechecking upgrades for $pkgId2 failed with unknown error."
                    ),
                  )
                  .asGrpcError
              )
          }
      }
    }
  }

  private def typecheckUpgrades(typecheckPhase: TypecheckUpgrades.UploadPhaseCheck, optDar1: Option[(Ref.PackageId, Ast.Package)], optDar2: Option[(Ref.PackageId, Ast.Package)])(implicit
                                                                                                                      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    (optDar1, optDar2) match {
      case (None, _) | (_, None) =>
        Future.unit

      case (Some((pkgId1, pkg1)), Some((pkgId2, pkg2))) =>
        strictTypecheckUpgrades(typecheckPhase, Some((pkgId1, pkg1)), pkgId2, Some(pkg2))
    }
  }

  private def validateUpgrade(upgradingDar: Dar[(Ref.PackageId, Ast.Package)])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    val (upgradingPackageId, upgradingPackage) = upgradingDar.main
    val optUpgradingDar = Some((upgradingPackageId, upgradingPackage))
    val packageMap = packageMetadataStore.getSnapshot.getUpgradablePackageMap
    logger.info(s"Uploading DAR file for $upgradingPackageId in submission ID ${loggingContext.serializeFiltered("submissionId")}.")
    existingVersionedPackageId(upgradingPackage, packageMap) match {
      case Some(uploadedPackageId) =>
        if (uploadedPackageId == upgradingPackageId) {
          logger.info(s"Ignoring upload of package $upgradingPackageId as it has been previously uploaded")
          Future.unit
        } else {
          Future.failed(Validation.UpgradeVersion.Error(uploadedPackageId, upgradingPackageId, upgradingPackage.metadata.version).asGrpcError)
        }

      case None =>
        upgradingPackage.metadata.upgradedPackageId match {
          case Some(upgradedPackageId) if packageMap.contains(upgradedPackageId) =>
            for {
              Some((_, upgradedPackage)) <- lookupDar(upgradedPackageId)
              _ <- strictTypecheckUpgrades(TypecheckUpgrades.DarCheck, optUpgradingDar, upgradedPackageId, Some(upgradedPackage))
              optMaximalDar <- maximalVersionedDar(upgradedPackageId, upgradingPackage, packageMap)
              _ <- typecheckUpgrades(TypecheckUpgrades.MaximalDarCheck, optMaximalDar, optUpgradingDar)
              optMinimalDar <- minimalVersionedDar(upgradingPackage, packageMap)
              _ <- typecheckUpgrades(TypecheckUpgrades.MinimalDarCheck, optUpgradingDar, optMinimalDar)
              _ = logger.info(s"Typechecking upgrades for $upgradingPackageId succeeded.")
            } yield ()

          case Some(upgradedPackageId) =>
            Future.failed(Validation.Upgradeability
              .Error(upgradingPackageId, upgradedPackageId, UpgradeError(UpgradeError.CouldNotResolveUpgradedPackageId(Upgrading(upgradingPackageId, upgradedPackageId)))).asGrpcError)

          case None =>
            logger.info(s"Package $upgradingPackageId does not upgrade anything.")
            Future.unit
        }
    }
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
}
