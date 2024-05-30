// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits.{catsSyntaxParallelTraverse1, toBifunctorOps, toTraverseOps}
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.DamlError
import com.daml.lf.archive.{DarParser, Decode}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String256M}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{HashOps, HashPurpose}
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
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, ParticipantEventPublisher}
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.{EitherTUtil, PathUtils, SimpleExecutionQueue}
import com.digitalasset.canton.{
  DiscardOps,
  LedgerSubmissionId,
  LfPackageId,
  LfPackageName,
  LfPackageVersion,
}
import com.google.protobuf.ByteString

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.ZipInputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Stateful class encapsulating the DAR upload flow.
  * It manages an in-memory state used for resolving package-id to (package-name, package-version) pairs
  * needed for package upgrade validations on DAR uploads.
  */
class PackageUploader(
    engine: Engine,
    hashOps: HashOps,
    eventPublisher: ParticipantEventPublisher,
    packageDependencyResolver: PackageDependencyResolver,
    enableUpgradeValidation: Boolean,
    clock: Clock,
    futureSupervisor: FutureSupervisor,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val loggingSubject = "Upgradable Package Resolution View"
  private val upgradablePackageResolutionMapRef
      : AtomicReference[Option[Map[LfPackageId, (LfPackageName, LfPackageVersion)]]] =
    new AtomicReference(None)
  private val uploadDarExecutionQueue = new SimpleExecutionQueue(
    "sequential-upload-dar-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )
  private val packagesDarsStore = packageDependencyResolver.damlPackageStore
  private val packageUpgradeValidator = new PackageUpgradeValidator(
    getPackageMap = getSnapshot(_),
    getLfArchive = traceContext => pkgId => packagesDarsStore.getPackage(pkgId)(traceContext),
    loggerFactory = loggerFactory,
  )

  def validateAndStorePackages(
      darPayload: ByteString,
      fileNameO: Option[String],
      sourceDescriptionO: Option[String],
      submissionId: LedgerSubmissionId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DamlError, List[Ref.PackageId]] = {
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
      sourceDescriptionLenLimit <- EitherT.fromEither[FutureUnlessShutdown](
        String256M
          .create(sourceDescriptionO.getOrElse(""), Some("package source description"))
          .leftMap(PackageServiceErrors.InternalError.Generic.apply)
      )
      sourceDescription = lengthValidatedNameO
        .map(_.asString1GB)
        .getOrElse(sourceDescriptionLenLimit)
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
  // that a package validation against the current package resolution map
  // is happening concurrently with an update of the package resolution map.
  private def uploadDarSequentialStep(
      darO: Option[Dar],
      mainPackage: (DamlLf.Archive, (LfPackageId, Ast.Package)),
      dependencies: List[(DamlLf.Archive, (LfPackageId, Ast.Package))],
      sourceDescription: String256M,
      submissionId: LedgerSubmissionId,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Either[DamlError, Unit]] = {
    def persist(allPackages: List[(DamlLf.Archive, (LfPackageId, Ast.Package))]) = {
      val packagesToStore = allPackages.map { case (archive, (_, astPackage)) =>
        val upgradingPkg = astPackage.languageVersion >= LanguageVersion.Features.packageUpgrades
        // Package-name and package versions are only relevant for LF versions supporting smart contract upgrading
        val pkgNameO = astPackage.metadata.filter(_ => upgradingPkg).map(_.name)
        val pkgVersionO = astPackage.metadata.filter(_ => upgradingPkg).map(_.version)
        (archive, pkgNameO, pkgVersionO)
      }
      for {
        _ <- packagesDarsStore.append(packagesToStore, sourceDescription, darO)
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
        _ = allPackages.foreach { case (_, (pkgId, pkg)) => updateWithPackage(pkgId, pkg) }
      } yield ()
    }

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
              // hence ensure the package resolution map stays in sync by re-initializing it from the DB.
              initialize
          }
    }

  private def validatePackages(
      mainPackage: (PackageId, Package),
      dependencies: List[(PackageId, Package)],
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
        mainPackage._2.metadata match {
          case Some(packageMetadata) =>
            packageUpgradeValidator
              .validateUpgrade(mainPackage, packageMetadata)(
                LoggingContextWithTrace(loggerFactory)
              )
              .mapK(FutureUnlessShutdown.outcomeK)
          case None =>
            logger.info(
              s"Package metadata is not defined for ${mainPackage._1}. Skipping upgrade validation."
            )
            EitherTUtil.unitUS[DamlError]
        }
      )
    } yield ()

  private def updateWithPackage(
      pkgId: LfPackageId,
      pkg: Package,
  )(implicit traceContext: TraceContext): Unit =
    if (enableUpgradeValidation) {
      // Smart contract upgrading works only for LF versions 1.16 and above
      // for which the package metadata must be defined.
      // Hence, if not defined, no need to update the upgrading package resolution map.
      pkg.metadata.foreach(pkgMeta =>
        upgradablePackageResolutionMapRef.updateAndGet {
          case None =>
            logger.error(s"Trying to update an uninitialized $loggingSubject")
            None
          case Some(current) =>
            Some(current + (pkgId -> (pkgMeta.name, pkgMeta.version)))
        }.discard
      )
    }

  def initialize(implicit tc: TraceContext): Future[Unit] =
    if (enableUpgradeValidation) {
      logger.info(s"Initializing $loggingSubject")
      val startedTime = clock.now
      def elapsedDuration(): java.time.Duration = clock.now - startedTime

      packagesDarsStore
        .listPackages()
        .map(_.foldLeft(Map.empty[LfPackageId, (LfPackageName, LfPackageVersion)]) {
          case (acc, PackageDescription(packageId, _, Some(packageName), Some(packageVersion))) =>
            acc + (packageId -> (packageName -> packageVersion))
          case (acc, _) =>
            // Only upgradable packages (i.e. have language version >= 1.16) are stored with name and version populated
            acc
        })
        .map { packageResolutionMap =>
          upgradablePackageResolutionMapRef.set(Some(packageResolutionMap))
          val duration = elapsedDuration()
          logger.info(
            s"$loggingSubject has been initialized (${duration.toNanos / 1000000L} ms)"
          )
        }
        .recoverWith { case NonFatal(e) =>
          // Likely there is a severe system problem if we can't build the package resolution map
          // Set the reference to None to "mark" the system as unhealthy
          // This ensures that requests requiring the ref will fail early
          upgradablePackageResolutionMapRef.set(None)
          logger.error(s"Failed to initialize $loggingSubject", e)
          Future.failed(e)
        }
    } else Future.unit

  private def getSnapshot(implicit
      tc: TraceContext
  ): Map[LfPackageId, (LfPackageName, LfPackageVersion)] =
    upgradablePackageResolutionMapRef
      .get()
      .getOrElse(
        throw PackageServiceErrors.InternalError
          .Generic(s"$loggingSubject is not initialized.")
          .asGrpcError
      )

  override def onClosed(): Unit = Lifecycle.close(uploadDarExecutionQueue)(logger)
}

object PackageUploader {
  def createAndInitialize(
      engine: Engine,
      hashOps: HashOps,
      eventPublisher: ParticipantEventPublisher,
      packageDependencyResolver: PackageDependencyResolver,
      enableUpgradeValidation: Boolean,
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[PackageUploader] = {
    val packageUploader = new PackageUploader(
      engine,
      hashOps,
      eventPublisher,
      packageDependencyResolver,
      enableUpgradeValidation,
      clock,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )
    FutureUnlessShutdown.outcomeF(packageUploader.initialize.map(_ => packageUploader))
  }
  implicit class ErrorValidations[E, R](result: Either[E, R]) {
    def handleError(toSelfServiceErrorCode: E => DamlError): Try[R] =
      result.left.map { err =>
        toSelfServiceErrorCode(err).asGrpcError
      }.toTry
  }
}
