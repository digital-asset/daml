// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits.{catsSyntaxParallelTraverse1, toBifunctorOps, toTraverseOps}
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.DamlError
import com.daml.lf.archive.{DarParser, Decode}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String256M}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose}
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService.{
  Dar,
  DarDescriptor,
  catchUpstreamErrors,
}
import com.digitalasset.canton.participant.store.memory.MutablePackageMetadataView
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, ParticipantEventPublisher}
import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.{ThereafterAsyncOps, ThereafterOps}
import com.digitalasset.canton.util.{EitherTUtil, PathUtils, SimpleExecutionQueue}
import com.digitalasset.canton.{LedgerSubmissionId, LfPackageId}
import com.google.protobuf.ByteString

import java.nio.file.Paths
import java.util.zip.ZipInputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class PackageUploader(
    engine: Engine,
    eventPublisher: ParticipantEventPublisher,
    enableUpgradeValidation: Boolean,
    futureSupervisor: FutureSupervisor,
    hashOps: HashOps,
    packageDependencyResolver: PackageDependencyResolver,
    packageMetadataView: MutablePackageMetadataView,
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
  )
  private val packagesDarsStore = packageDependencyResolver.damlPackageStore
  private val packageUpgradeValidator = new PackageUpgradeValidator(
    getPackageMap = loggingContextWithTrace =>
      packageMetadataView
        .getSnapshot(loggingContextWithTrace.traceContext)
        .packageIdVersionMap,
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
          .thereafter { _ => stream.close() }
        mainPackage <- catchUpstreamErrors(Decode.decodeArchive(dar.main))
        dependencies <- dar.dependencies.parTraverse(archive =>
          catchUpstreamErrors(Decode.decodeArchive(archive))
        )
        _ <- validatePackages(mainPackage, dependencies)
      } yield hash
    }

  def upload(
      darPayload: ByteString,
      fileNameO: Option[String],
      sourceDescription: String256M,
      submissionId: LedgerSubmissionId,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, DamlError, List[Ref.PackageId]] = {
    val darNameO =
      fileNameO.map(fn => PathUtils.getFilenameWithoutExtension(Paths.get(fn).getFileName))
    val stream = new ZipInputStream(darPayload.newInput())

    for {
      lengthValidatedNameO <- darNameO.traverse(darName =>
        EitherT
          .fromEither[FutureUnlessShutdown](
            String255.create(darName, Some("DAR file name"))
          )
          .leftMap(PackageServiceErrors.Reading.InvalidDarFileName.Error(_))
      )
      darDescriptorO = lengthValidatedNameO.map(lengthValidatedName =>
        Dar(
          DarDescriptor(
            hashOps.digest(HashPurpose.DarIdentifier, darPayload),
            lengthValidatedName,
          ),
          darPayload.toByteArray,
        )
      )
      dar <- catchUpstreamErrors(
        DarParser.readArchive(darNameO.getOrElse("package-upload"), stream)
      ).thereafter(_ => stream.close())
      _ = logger.debug(
        s"Processing package upload of ${dar.all.length} packages from source $sourceDescription"
      )
      mainPackage <- catchUpstreamErrors(Decode.decodeArchive(dar.main)).map(dar.main -> _)
      dependencies <- dar.dependencies.parTraverse(archive =>
        catchUpstreamErrors(Decode.decodeArchive(archive)).map(archive -> _)
      )
      _ <- EitherT(
        uploadDarExecutionQueue.executeUnderFailuresUS(
          uploadDarSequentialStep(
            darO = darDescriptorO,
            mainPackage = mainPackage,
            dependencies = dependencies,
            sourceDescription = sourceDescription,
            submissionId = submissionId,
          ),
          description = "store DAR",
        )
      )
    } yield mainPackage._2._1 :: dependencies.map(_._2._1)
  }

  // This stage must be run sequentially to exclude the possibility
  // that a package validation against the current package metadata view
  // is happening concurrently with an update of the package metadata view.
  private def uploadDarSequentialStep(
      darO: Option[Dar],
      mainPackage: (DamlLf.Archive, (LfPackageId, Ast.Package)),
      dependencies: List[(DamlLf.Archive, (LfPackageId, Ast.Package))],
      sourceDescription: String256M,
      submissionId: LedgerSubmissionId,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Either[DamlError, Unit]] = {
    def persist(allPackages: List[(DamlLf.Archive, (LfPackageId, Ast.Package))]) =
      for {
        _ <- packagesDarsStore.append(allPackages.map(_._1), sourceDescription, darO)
        // update our dependency cache
        // we need to do this due to an issue we can hit if we have pre-populated the cache
        // with the information about the package not being present (with a None)
        // now, that the package is loaded, we need to get rid of this None.
        _ = packageDependencyResolver.clearPackagesNotPreviouslyFound()
        _ <- eventPublisher.publish(
          LedgerSyncEvent.PublicPackageUpload(
            archives = allPackages.map(_._1),
            sourceDescription = Some(sourceDescription.unwrap),
            recordTime = ParticipantEventPublisher.now.toLf,
            submissionId = Some(submissionId),
          )
        )
        _ = logger.debug(
          s"Managed to upload one or more archives in submissionId $submissionId and sourceDescription $sourceDescription"
        )
        _ = allPackages.foreach { case (_, (pkgId, pkg)) =>
          packageMetadataView.update(PackageMetadata.from(pkgId, pkg))
        }
      } yield ()

    validatePackages(mainPackage._2, dependencies.map(_._2)).semiflatMap { _ =>
      val allPackages = mainPackage :: dependencies
      val result = persist(allPackages)
      handleUploadResult(result, submissionId, sourceDescription)
    }.value
  }

  private def handleUploadResult(
      res: FutureUnlessShutdown[Unit],
      submissionId: LedgerSubmissionId,
      sourceDescription: String256M,
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
          s"Failed to upload one or more archives in submissionId $submissionId and sourceDescription $sourceDescription",
          e,
        )
        eventPublisher
          .publish(
            LedgerSyncEvent.PublicPackageUploadRejected(
              rejectionReason = e.getMessage,
              recordTime = ParticipantEventPublisher.now.toLf,
              submissionId = submissionId,
            )
          )
          .transformWith {
            case Success(UnlessShutdown.AbortedDueToShutdown) =>
              FutureUnlessShutdown.abortedDueToShutdown
            case Success(_) =>
              FutureUnlessShutdown.failed[Unit](e)
            case Failure(exception) =>
              // The LedgerSyncEvent publishing failed as well so log its error
              // and return the original error
              logger.warn(
                s"Publishing PublicPackageUploadRejected failed for submissionId $submissionId and sourceDescription $sourceDescription",
                exception,
              )
              FutureUnlessShutdown.failed[Unit](e)
          }
          .thereafterF {
            case Success(UnlessShutdown.AbortedDueToShutdown) =>
              // Do nothing, node is shutting down
              Future.unit
            case _other =>
              // If JDBC insertion call failed, we don't know whether the DB was updated or not
              // hence ensure the package metadata view stays in sync by re-initializing it from the DB.
              packageMetadataView.refreshState.onShutdown(())
          }
    }

  private def validatePackages(
      mainPackage: (LfPackageId, Ast.Package),
      dependencies: List[(LfPackageId, Ast.Package)],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DamlError, Unit] =
    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        engine
          .validatePackages((mainPackage :: dependencies).toMap)
          .leftMap(
            PackageServiceErrors.Validation.handleLfEnginePackageError(_): DamlError
          )
      )
      _ <- EitherTUtil.ifThenET(enableUpgradeValidation)(
        packageUpgradeValidator
          .validateUpgrade(mainPackage)(LoggingContextWithTrace(loggerFactory))
          .mapK(FutureUnlessShutdown.outcomeK)
      )
    } yield ()

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
