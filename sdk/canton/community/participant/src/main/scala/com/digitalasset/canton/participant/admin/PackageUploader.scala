// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits.{catsSyntaxParallelTraverse1, toBifunctorOps, toTraverseOps}
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.participant.admin.PackageService.{
  Dar,
  DarDescription,
  DarMainPackageId,
  catchUpstreamErrors,
}
import com.digitalasset.canton.participant.store.memory.MutablePackageMetadataView
import com.digitalasset.canton.participant.store.{DamlPackageStore, PackageInfo}
import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SimpleExecutionQueue
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.{LedgerSubmissionId, LfPackageId}
import com.digitalasset.daml.lf.archive.{DamlLf, Dar as LfDar, DarParser, Decode}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.language.Ast
import com.google.protobuf.ByteString

import java.util.zip.ZipInputStream
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class PackageUploader(
    clock: Clock,
    packageStore: DamlPackageStore,
    engine: Engine,
    enableUpgradeValidation: Boolean,
    futureSupervisor: FutureSupervisor,
    packageMetadataView: MutablePackageMetadataView,
    packageUpgradeValidator: PackageUpgradeValidator,
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

  def validateDar(
      payload: ByteString,
      darName: String,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, DarMainPackageId] =
    performUnlessClosingEitherUSF("validate DAR") {
      val stream = new ZipInputStream(payload.newInput())
      for {
        dar <- catchUpstreamErrors(DarParser.readArchive(darName, stream))
          .thereafter(_ => stream.close())
        mainPackage <- catchUpstreamErrors(Decode.decodeArchive(dar.main))
        dependencies <- dar.dependencies.parTraverse(archive =>
          catchUpstreamErrors(Decode.decodeArchive(archive))
        )
        _ <- validatePackages(mainPackage :: dependencies)
      } yield DarMainPackageId.tryCreate(mainPackage._1)
    }

  /** Uploads dar into dar store
    *
    * @return
    *   the package id of the main package (also used to refer to the dar) and the package ids of
    *   the dependencies
    */
  def upload(
      darPayload: ByteString,
      description: Option[String],
      submissionId: LedgerSubmissionId,
      expectedMainPackageId: Option[LfPackageId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, (LfPackageId, List[LfPackageId])] =
    performUnlessClosingEitherUSF("upload DAR") {

      for {
        lengthValidatedDescO <- description.traverse(description =>
          EitherT
            .fromEither[FutureUnlessShutdown](String255.create(description))
            .leftMap(PackageServiceErrors.Reading.InvalidDarFileName.Error(_))
        )
        dar <- readDarFromPayload(darPayload, description)
        _ = logger.debug(
          s"Processing package upload of ${dar.all.length} packages${description
              .fold("")(n => s" from $n")} for submissionId $submissionId"
        )

        mainPackage <- catchUpstreamErrors(Decode.decodeArchive(dar.main)).map(dar.main -> _)
        foundMainPackageId = mainPackage._2._1
        _ <- expectedMainPackageId.traverse(expected =>
          EitherT.cond[FutureUnlessShutdown](
            foundMainPackageId == expected,
            (),
            PackageServiceErrors.Reading.MainPackageInDarDoesNotMatchExpected
              .Reject(foundMainPackageId, expected),
          )
        )
        dependencies <- dar.dependencies.parTraverse(archive =>
          catchUpstreamErrors(Decode.decodeArchive(archive)).map(archive -> _)
        )
        _ <- uploadDarExecutionQueue.executeEUS(
          uploadDarSequentialStep(
            darPayload = darPayload,
            mainPackage = mainPackage,
            dependencies = dependencies,
            description = lengthValidatedDescO,
            submissionId = submissionId,
          ),
          description = "store DAR",
        )
      } yield (foundMainPackageId, dependencies.map(_._2._1))
    }

  // This stage must be run sequentially to exclude the possibility
  // that a package validation against the current package metadata view
  // is happening concurrently with an update of the package metadata view.
  private def uploadDarSequentialStep(
      darPayload: ByteString,
      mainPackage: (DamlLf.Archive, (LfPackageId, Ast.Package)),
      dependencies: List[(DamlLf.Archive, (LfPackageId, Ast.Package))],
      description: Option[String255],
      submissionId: LedgerSubmissionId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, DarMainPackageId] = {
    val allPackages = mainPackage +: dependencies
    def persist(
        dar: Dar,
        packages: List[(PackageInfo, DamlLf.Archive)],
        uploadedAt: CantonTimestamp,
    ): FutureUnlessShutdown[Unit] =
      for {
        _ <- packageStore.append(packages, uploadedAt, dar)
        _ = logger.debug(
          s"Managed to upload one or more archives for submissionId $submissionId"
        )
        _ = allPackages.foreach { case (_, (pkgId, pkg)) =>
          packageMetadataView.update(PackageMetadata.from(pkgId, pkg))
        }
      } yield ()

    val uploadTime = clock.monotonicTime()
    val mainPackageId = DarMainPackageId.tryCreate(mainPackage._2._1)
    val persistedDescription =
      description.getOrElse(String255.tryCreate(s"DAR_${mainPackageId.value}"))

    def parseMetadata(
        pkg: (DamlLf.Archive, (LfPackageId, Ast.Package))
    ): Either[RpcError, PackageInfo] = {
      val (_, (packageId, ast)) = pkg
      PackageInfo
        .fromPackageMetadata(ast.metadata)
        .leftMap(err =>
          PackageServiceErrors.Reading.ParseError
            .Error(s"Failed to parse package metadata of $packageId: $err")
        )

    }
    for {
      mainInfo <- EitherT.fromEither[FutureUnlessShutdown](parseMetadata(mainPackage))
      darDescriptor =
        Dar(
          DarDescription(mainPackageId, persistedDescription, mainInfo.name, mainInfo.version),
          darPayload.toByteArray,
        )
      _ <- validatePackages(allPackages.map(_._2))
      toUpload <- EitherT.fromEither[FutureUnlessShutdown](
        allPackages.traverse(x => parseMetadata(x).map(_ -> x._1))
      )
      _ <- EitherT.right[RpcError](
        handlePersistResult(persist(darDescriptor, toUpload, uploadTime), submissionId)
      )
    } yield mainPackageId
  }

  private def handlePersistResult(
      res: FutureUnlessShutdown[Unit],
      submissionId: LedgerSubmissionId,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    res.transformWith {
      case Failure(e) =>
        logger.warn(
          s"Failed to upload one or more archives in submissionId $submissionId",
          e,
        )
        // If JDBC insertion call failed, we don't know whether the DB was updated or not
        // hence ensure the package metadata view stays in sync by re-initializing it from the DB.
        packageMetadataView.refreshState.transformWith(_ => FutureUnlessShutdown.failed(e))
      case success: Success[UnlessShutdown[Unit]] => FutureUnlessShutdown.lift(success.value)
    }

  private def validatePackages(
      packages: List[(LfPackageId, Ast.Package)]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        engine
          .validatePackages(packages.toMap)
          .leftMap(
            PackageServiceErrors.Validation.handleLfEnginePackageError(_): RpcError
          )
      )
      _ <-
        if (enableUpgradeValidation) {
          val packageMetadataSnapshot = packageMetadataView.getSnapshot
          packageUpgradeValidator
            .validateUpgrade(packages, packageMetadataSnapshot)(
              LoggingContextWithTrace(loggerFactory)
            )
        } else {
          logger.info(
            s"Skipping upgrade validation for packages ${packages.map(_._1).sorted.mkString(", ")}"
          )
          EitherT.pure[FutureUnlessShutdown, RpcError](())
        }
    } yield ()

  private def readDarFromPayload(darPayload: ByteString, description: Option[String])(implicit
      errorLogger: ErrorLoggingContext
  ): EitherT[FutureUnlessShutdown, RpcError, LfDar[DamlLf.Archive]] = {
    val zipInputStream = new ZipInputStream(darPayload.newInput())
    catchUpstreamErrors(
      DarParser.readArchive(description.getOrElse("unknown-file-name"), zipInputStream)
    ).thereafter(_ => zipInputStream.close())
  }

  override protected def onClosed(): Unit = LifeCycle.close(uploadDarExecutionQueue)(logger)
}

object PackageUploader {
  def apply(
      clock: Clock,
      engine: Engine,
      enableUpgradeValidation: Boolean,
      futureSupervisor: FutureSupervisor,
      packageDependencyResolver: PackageDependencyResolver,
      packageMetadataView: MutablePackageMetadataView,
      exitOnFatalFailures: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): PackageUploader = {

    val packageStore = packageDependencyResolver.damlPackageStore
    val packageUpgradeValidator = new PackageUpgradeValidator(
      getLfArchive = loggingContextWithTrace =>
        pkgId => packageStore.getPackage(pkgId)(loggingContextWithTrace.traceContext),
      loggerFactory = loggerFactory,
    )
    new PackageUploader(
      clock,
      packageStore,
      engine,
      enableUpgradeValidation,
      futureSupervisor,
      packageMetadataView,
      packageUpgradeValidator,
      exitOnFatalFailures,
      timeouts,
      loggerFactory,
    )
  }

  implicit class ErrorValidations[E, R](result: Either[E, R]) {
    def handleError(toSelfServiceErrorCode: E => RpcError): Try[R] =
      result.left.map { err =>
        toSelfServiceErrorCode(err).asGrpcError
      }.toTry
  }
}
