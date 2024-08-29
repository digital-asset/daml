// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits.{catsSyntaxParallelTraverse1, toBifunctorOps, toTraverseOps}
import com.daml.error.{ContextualizedErrorLogger, DamlError}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService.{
  Dar,
  DarDescriptor,
  catchUpstreamErrors,
}
import com.digitalasset.canton.participant.store.memory.MutablePackageMetadataView
import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.{PathUtils, SimpleExecutionQueue}
import com.digitalasset.canton.{LedgerSubmissionId, LfPackageId}
import com.digitalasset.daml.lf.archive.{DamlLf, Dar as LfDar, DarParser, Decode}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.google.protobuf.ByteString

import java.nio.file.Paths
import java.util.zip.ZipInputStream
import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.{Failure, Success, Try}

class PackageUploader(
    clock: Clock,
    engine: Engine,
    enableUpgradeValidation: Boolean,
    futureSupervisor: FutureSupervisor,
    hashOps: HashOps,
    packageDependencyResolver: PackageDependencyResolver,
    packageMetadataView: MutablePackageMetadataView,
    exitOnFatalFailures: Boolean,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {
  private val uploadDarExecutionQueue = new SimpleExecutionQueue(
    "sequential-upload-dar-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )
  private val packagesDarsStore = packageDependencyResolver.damlPackageStore
  private val packageUpgradeValidator = new PackageUpgradeValidator(
    getPackageMap = implicit loggingContextWithTrace =>
      packageMetadataView.getSnapshot.packageIdVersionMap,
    getLfArchive = loggingContextWithTrace =>
      pkgId => packagesDarsStore.getPackage(pkgId)(loggingContextWithTrace.traceContext),
    loggerFactory = loggerFactory,
  )

  def validateDar(
      payload: ByteString,
      darName: String,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, DamlError, Hash] =
    performUnlessClosingEitherUSF("validate DAR") {
      val hash = hashOps.digest(HashPurpose.DarIdentifier, payload)
      val stream = new ZipInputStream(payload.newInput())
      for {
        dar <- catchUpstreamErrors(DarParser.readArchive(darName, stream))
          .thereafter(_ => stream.close())
        mainPackage <- catchUpstreamErrors(Decode.decodeArchive(dar.main))
        dependencies <- dar.dependencies.parTraverse(archive =>
          catchUpstreamErrors(Decode.decodeArchive(archive))
        )
        _ <- validatePackages(mainPackage :: dependencies)
      } yield hash
    }

  def upload(
      darPayload: ByteString,
      fileNameO: Option[String],
      submissionId: LedgerSubmissionId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DamlError, (List[LfPackageId], Hash)] =
    performUnlessClosingEitherUSF("upload DAR") {
      val darNameO =
        fileNameO.map(fn => PathUtils.getFilenameWithoutExtension(Paths.get(fn).getFileName))

      for {
        lengthValidatedNameO <- darNameO.traverse(darName =>
          EitherT
            .fromEither[FutureUnlessShutdown](String255.create(darName))
            .leftMap(PackageServiceErrors.Reading.InvalidDarFileName.Error(_))
        )
        dar <- readDarFromPayload(darPayload, darNameO)
        _ = logger.debug(
          s"Processing package upload of ${dar.all.length} packages${darNameO
              .fold("")(n => s" from $n")} for submissionId $submissionId"
        )
        mainPackage <- catchUpstreamErrors(Decode.decodeArchive(dar.main)).map(dar.main -> _)
        dependencies <- dar.dependencies.parTraverse(archive =>
          catchUpstreamErrors(Decode.decodeArchive(archive)).map(archive -> _)
        )
        allPackages = mainPackage :: dependencies
        hash <- EitherT(
          uploadDarExecutionQueue.executeUS(
            uploadDarSequentialStep(
              darPayload = darPayload,
              packages = allPackages,
              // TODO(#17635): Allow more generic source descriptions or rename source description to DAR name
              lengthValidatedDarName = lengthValidatedNameO,
              submissionId = submissionId,
            ),
            description = "store DAR",
          )
        )
      } yield allPackages.map(_._2._1) -> hash
    }

  // This stage must be run sequentially to exclude the possibility
  // that a package validation against the current package metadata view
  // is happening concurrently with an update of the package metadata view.
  private def uploadDarSequentialStep(
      darPayload: ByteString,
      packages: List[(DamlLf.Archive, (LfPackageId, Ast.Package))],
      lengthValidatedDarName: Option[String255],
      submissionId: LedgerSubmissionId,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Either[DamlError, Hash]] = {
    def persist(
        dar: Dar,
        uploadedAt: CantonTimestamp,
        allPackages: List[(DamlLf.Archive, (LfPackageId, Ast.Package))],
    ): FutureUnlessShutdown[Unit] =
      for {
        _ <- packagesDarsStore.append(
          pkgs = allPackages.map(_._1),
          uploadedAt = uploadedAt,
          sourceDescription = lengthValidatedDarName.getOrElse(String255.empty),
          dar = dar,
        )
        // update our dependency cache
        // we need to do this due to an issue we can hit if we have pre-populated the cache
        // with the information about the package not being present (with a None)
        // now, that the package is loaded, we need to get rid of this None.
        _ = packageDependencyResolver.clearPackagesNotPreviouslyFound()
        _ = logger.debug(
          s"Managed to upload one or more archives for submissionId $submissionId"
        )
        _ = allPackages.foreach { case (_, (pkgId, pkg)) =>
          if (
            pkg.languageVersion >= LanguageVersion.Features.packageUpgrades && !pkg.isUtilityPackage
          ) {
            packageMetadataView.update(PackageMetadata.from(pkgId, pkg))
          }
        }
      } yield ()

    val uploadTime = clock.monotonicTime()
    val hash = hashOps.digest(HashPurpose.DarIdentifier, darPayload)
    val persistedDarName =
      lengthValidatedDarName.getOrElse(String255.tryCreate(s"DAR_${hash.toHexString}"))
    val darDescriptor =
      Dar(
        DarDescriptor(hash, persistedDarName),
        darPayload.toByteArray,
      )
    validatePackages(packages.map(_._2))
      .semiflatMap { _ =>
        val result = persist(darDescriptor, uploadTime, packages)
        handleUploadResult(result, submissionId)
      }
      .map(_ => hash)
      .value
  }

  private def handleUploadResult(
      res: FutureUnlessShutdown[Unit],
      submissionId: LedgerSubmissionId,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    res.transformWith {
      case Success(UnlessShutdown.Outcome(_)) => FutureUnlessShutdown.unit
      case Success(UnlessShutdown.AbortedDueToShutdown) =>
        // Possibly LedgerSyncEvent.PublicPackageUpload was not emitted but
        // the packages and DARs were already stored in the packagesDarsStore.
        // There is nothing we can do about it since the node is shutting down.
        // However, this situation is acceptable since the user can
        // retry uploading the DARs (DAR uploads are idempotent).
        logger.info("Aborting DAR upload due to shutdown.")
        FutureUnlessShutdown.abortedDueToShutdown
      case Failure(e) =>
        logger.warn(
          s"Failed to upload one or more archives in submissionId $submissionId",
          e,
        )
        // If JDBC insertion call failed, we don't know whether the DB was updated or not
        // hence ensure the package metadata view stays in sync by re-initializing it from the DB.
        packageMetadataView.refreshState.transformWith(_ => FutureUnlessShutdown.failed(e))
    }

  private def validatePackages(
      packages: List[(LfPackageId, Ast.Package)]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DamlError, Unit] =
    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        engine
          .validatePackages(packages.toMap)
          .leftMap(
            PackageServiceErrors.Validation.handleLfEnginePackageError(_): DamlError
          )
      )
      _ <-
        if (enableUpgradeValidation) {
          packageUpgradeValidator
            .validateUpgrade(packages)(LoggingContextWithTrace(loggerFactory))
            .mapK(FutureUnlessShutdown.outcomeK)
        } else {
          logger.info(
            s"Skipping upgrade validation for packages ${packages.map(_._1).sorted.mkString(", ")}"
          )
          EitherT.pure[FutureUnlessShutdown, DamlError](())
        }
    } yield ()

  private def readDarFromPayload(darPayload: ByteString, darNameO: Option[String])(implicit
      errorLogger: ContextualizedErrorLogger
  ): EitherT[FutureUnlessShutdown, DamlError, LfDar[DamlLf.Archive]] = {
    val zipInputStream = new ZipInputStream(darPayload.newInput())
    catchUpstreamErrors(
      DarParser.readArchive(darNameO.getOrElse("unknown-file-name"), zipInputStream)
    ).thereafter(_ => zipInputStream.close())
  }

  override protected def onClosed(): Unit = Lifecycle.close(uploadDarExecutionQueue)(logger)
}

object PackageUploader {
  implicit class ErrorValidations[E, R](result: Either[E, R]) {
    def handleError(toSelfServiceErrorCode: E => DamlError): Try[R] =
      result.left.map { err =>
        toSelfServiceErrorCode(err).asGrpcError
      }.toTry
  }
}
