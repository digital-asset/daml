// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits.{toBifunctorOps, toTraverseOps}
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.DamlError
import com.daml.lf.archive
import com.daml.lf.archive.{DarParser, Decode, Error as LfArchiveError}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast.Package
import com.daml.lf.validation.{TypecheckUpgrades, UpgradeError}
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.DarName
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String256M}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.digitalasset.canton.ledger.error.PackageServiceErrors.Validation
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.{
  CannotRemoveOnlyDarForPackage,
  MainPackageInUse,
  PackageRemovalError,
  PackageVetted,
}
import com.digitalasset.canton.participant.admin.PackageService.*
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.DamlPackageStore.readPackageId
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, ParticipantEventPublisher}
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.protocol.{PackageDescription, PackageInfoService}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, PathUtils}
import com.digitalasset.canton.{LedgerSubmissionId, LfPackageId}
import com.google.protobuf.ByteString
import slick.jdbc.GetResult

import java.nio.file.Paths
import java.util.UUID
import java.util.zip.ZipInputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait DarService {
  def appendDarFromByteString(
      payload: ByteString,
      filename: String,
      vetAllPackages: Boolean,
      synchronizeVetting: Boolean,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, DamlError, Hash]
  def getDar(hash: Hash)(implicit traceContext: TraceContext): Future[Option[PackageService.Dar]]
  def listDars(limit: Option[Int])(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageService.DarDescriptor]]
}

class PackageService(
    engine: Engine,
    val dependencyResolver: PackageDependencyResolver,
    eventPublisher: ParticipantEventPublisher,
    hashOps: HashOps,
    packageOps: PackageOps,
    metrics: ParticipantMetrics,
    disableUpgradeValidation: Boolean,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends DarService
    with PackageInfoService
    with NamedLogging
    with FlagCloseable {

  private val packageLoader = new DeduplicatingPackageLoader()
  private val packagesDarsStore = dependencyResolver.damlPackageStore

  def getLfArchive(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[DamlLf.Archive]] =
    packagesDarsStore.getPackage(packageId)

  def listPackages(limit: Option[Int] = None)(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageDescription]] =
    packagesDarsStore.listPackages(limit)

  def getDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageDescription]] =
    packagesDarsStore.getPackageDescription(packageId)

  def getPackage(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[Package]] =
    packageLoader.loadPackage(
      packageId,
      getLfArchive,
      metrics.ledgerApiServer.execution.getLfPackage,
    )

  def removePackage(
      packageId: PackageId,
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    if (force) {
      logger.info(s"Forced removal of package $packageId")
      EitherT.liftF(packagesDarsStore.removePackage(packageId))
    } else {
      val checkUnused =
        packageOps.checkPackageUnused(packageId).mapK(FutureUnlessShutdown.outcomeK)

      val checkNotVetted =
        packageOps
          .isPackageVetted(packageId)
          .flatMap[CantonError, Unit] {
            case true => EitherT.leftT(new PackageVetted(packageId))
            case false => EitherT.rightT(())
          }
          .mapK(FutureUnlessShutdown.outcomeK)

      for {
        _ <- neededForAdminWorkflow(packageId)
        _ <- checkUnused
        _ <- checkNotVetted
        _ = logger.debug(s"Removing package $packageId")
        _ <- EitherT.liftF(packagesDarsStore.removePackage(packageId))
      } yield ()
    }

  def removeDar(darHash: Hash)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    ifDarExists(darHash)(removeDarLf(_, _))(ifNotExistsOperationFailed = "DAR archive removal")

  def vetDar(darHash: Hash, synchronize: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    ifDarExists(darHash) { (_, darLf) =>
      packageOps
        .vetPackages(darLf.all.map(readPackageId), synchronize)
        .leftWiden[CantonError]
    }(ifNotExistsOperationFailed = "DAR archive vetting")

  def unvetDar(darHash: Hash)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    ifDarExists(darHash) { (descriptor, lfArchive) =>
      val packages = lfArchive.all.map(readPackageId)
      val mainPkg = readPackageId(lfArchive.main)
      revokeVettingForDar(mainPkg, packages, descriptor)
    }(ifNotExistsOperationFailed = "DAR archive unvetting")

  private def ifDarExists(darHash: Hash)(
      action: (
          DarDescriptor,
          archive.Dar[DamlLf.Archive],
      ) => EitherT[FutureUnlessShutdown, CantonError, Unit]
  )(ifNotExistsOperationFailed: => String)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    EitherT
      .liftF(packagesDarsStore.getDar(darHash))
      .mapK(FutureUnlessShutdown.outcomeK)
      .flatMap {
        case None =>
          EitherT.leftT(
            CantonPackageServiceError.DarNotFound
              .Reject(
                operation = ifNotExistsOperationFailed,
                darHash = darHash.toHexString,
              ): CantonError
          )
        case Some(dar) =>
          val darLfE = PackageService.darToLf(dar)
          val (descriptor, lfArchive) =
            darLfE.left.map(msg => throw new IllegalStateException(msg)).merge

          action(descriptor, lfArchive)
      }

  private def neededForAdminWorkflow(packageId: PackageId)(implicit
      elc: ErrorLoggingContext
  ): EitherT[FutureUnlessShutdown, PackageRemovalError, Unit] =
    EitherTUtil.condUnitET(
      !AdminWorkflowServices.AdminWorkflowPackages.keySet.contains(packageId),
      new PackageRemovalErrorCode.CannotRemoveAdminWorkflowPackage(packageId),
    )

  private def removeDarLf(
      darDescriptor: DarDescriptor,
      dar: archive.Dar[DamlLf.Archive],
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit] = {
    // Can remove the DAR if:
    // 1. The main package of the dar is unused
    // 2. For all dependencies of the DAR, either:
    //     - They are unused
    //     - Or they are contained in another vetted DAR
    // 3. The main package of the dar is either:
    //     - Already un-vetted
    //     - Or can be automatically un-vetted, by revoking a vetting transaction corresponding to all packages in the DAR

    val packages = dar.all.map(readPackageId)

    val mainPkg = readPackageId(dar.main)
    for {
      _notAdminWf <- neededForAdminWorkflow(mainPkg)

      _mainUnused <- packageOps
        .checkPackageUnused(mainPkg)
        .leftMap(err => new MainPackageInUse(err.pkg, darDescriptor, err.contract, err.domain))
        .mapK(FutureUnlessShutdown.outcomeK)

      packageUsed <- EitherT
        .liftF(packages.parTraverse(p => packageOps.checkPackageUnused(p).value))
        .mapK(FutureUnlessShutdown.outcomeK)

      usedPackages = packageUsed.mapFilter {
        case Left(packageInUse: PackageRemovalErrorCode.PackageInUse) => Some(packageInUse.pkg)
        case Right(()) => None
      }

      _unit <- packagesDarsStore
        .anyPackagePreventsDarRemoval(usedPackages, darDescriptor)
        .toLeft(())
        .leftMap(p => new CannotRemoveOnlyDarForPackage(p, darDescriptor))
        .mapK(FutureUnlessShutdown.outcomeK)

      packagesThatCanBeRemoved <- EitherT
        .liftF(
          packagesDarsStore
            .determinePackagesExclusivelyInDar(packages, darDescriptor)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      _unit <- revokeVettingForDar(
        mainPkg,
        packagesThatCanBeRemoved.toList,
        darDescriptor,
      )

      _unit <-
        EitherT.liftF(packagesDarsStore.removePackage(mainPkg))

      _removed <- {
        logger.info(s"Removing dar ${darDescriptor.hash}")
        EitherT
          .liftF[FutureUnlessShutdown, CantonError, Unit](
            packagesDarsStore.removeDar(darDescriptor.hash)
          )
      }
    } yield ()
  }

  private def revokeVettingForDar(
      mainPkg: PackageId,
      packages: List[PackageId],
      darDescriptor: DarDescriptor,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    packageOps
      .isPackageVetted(mainPkg)
      .mapK(FutureUnlessShutdown.outcomeK)
      .flatMap { isVetted =>
        if (!isVetted)
          EitherT.pure[FutureUnlessShutdown, CantonError](
            logger.info(
              s"Package with id $mainPkg is already unvetted. Doing nothing for the unvet operation"
            )
          )
        else
          packageOps.revokeVettingForPackages(mainPkg, packages, darDescriptor).leftWiden
      }

  /** Stores DAR file from given byte string with the provided filename.
    * All the Daml packages inside the DAR file are also stored.
    * @param payload ByteString containing the data of the DAR file
    * @param filename String the filename of the DAR
    * @return Future with the hash of the DAR file
    */
  def appendDarFromByteString(
      payload: ByteString,
      filename: String,
      vetAllPackages: Boolean,
      synchronizeVetting: Boolean,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, DamlError, Hash] =
    appendDar(
      payload,
      PathUtils.getFilenameWithoutExtension(Paths.get(filename).getFileName),
      vetAllPackages,
      synchronizeVetting,
    )

  private def catchUpstreamErrors[E](
      attempt: Either[LfArchiveError, E]
  )(implicit traceContext: TraceContext): EitherT[Future, DamlError, E] =
    EitherT.fromEither(attempt match {
      case Right(value) => Right(value)
      case Left(LfArchiveError.InvalidDar(entries, cause)) =>
        Left(PackageServiceErrors.Reading.InvalidDar.Error(entries.entries.keys.toSeq, cause))
      case Left(LfArchiveError.InvalidZipEntry(name, entries)) =>
        Left(
          PackageServiceErrors.Reading.InvalidZipEntry.Error(name, entries.entries.keys.toSeq)
        )
      case Left(LfArchiveError.InvalidLegacyDar(entries)) =>
        Left(PackageServiceErrors.Reading.InvalidLegacyDar.Error(entries.entries.keys.toSeq))
      case Left(LfArchiveError.ZipBomb) =>
        Left(PackageServiceErrors.Reading.ZipBomb.Error(LfArchiveError.ZipBomb.getMessage))
      case Left(e: LfArchiveError) =>
        Left(PackageServiceErrors.Reading.ParseError.Error(e.msg))
      case Left(e) =>
        Left(PackageServiceErrors.InternalError.Unhandled(e))
    })

  private def appendDar(
      payload: ByteString,
      darName: String,
      vetAllPackages: Boolean,
      synchronizeVetting: Boolean,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, DamlError, Hash] = {
    val hash = hashOps.digest(HashPurpose.DarIdentifier, payload)
    val stream = new ZipInputStream(payload.newInput())
    val ret: EitherT[FutureUnlessShutdown, DamlError, Hash] = for {
      lengthValidatedName <- EitherT
        .fromEither[FutureUnlessShutdown](
          String255.create(darName, Some("DAR file name"))
        )
        .leftMap(PackageServiceErrors.Reading.InvalidDarFileName.Error(_))
      dar <- catchUpstreamErrors(DarParser.readArchive(darName, stream))
        .mapK(FutureUnlessShutdown.outcomeK)
      // Validate the packages before storing them in the DAR store or the package store
      _ <- validateArchives(dar).mapK(FutureUnlessShutdown.outcomeK)
      _ <- storeValidatedPackagesAndSyncEvent(
        dar.all,
        lengthValidatedName.asString1GB,
        LedgerSubmissionId.assertFromString(UUID.randomUUID().toString),
        Some(
          PackageService.Dar(DarDescriptor(hash, lengthValidatedName), payload.toByteArray)
        ),
        vetAllPackages = vetAllPackages,
        synchronizeVetting = synchronizeVetting,
      )

    } yield hash
    ret.transform { res =>
      stream.close()
      res
    }
  }

  override def getDar(hash: Hash)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageService.Dar]] = {
    packagesDarsStore.getDar(hash)
  }

  override def listDars(limit: Option[Int])(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageService.DarDescriptor]] = packagesDarsStore.listDars(limit)

  def listDarContents(darId: Hash)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, (DarDescriptor, archive.Dar[DamlLf.Archive])] =
    EitherT(
      packagesDarsStore
        .getDar(darId)
        .map(_.toRight(s"No such dar ${darId}").flatMap(PackageService.darToLf))
    )

  private def validateArchives(archives: archive.Dar[DamlLf.Archive])(implicit
      traceContext: TraceContext
  ): EitherT[Future, DamlError, Unit] =
    for {
      mainPackage <- catchUpstreamErrors(Decode.decodeArchive(archives.main))
      dependencies <- archives.dependencies
        .parTraverse(archive => catchUpstreamErrors(Decode.decodeArchive(archive)))
      _ <- EitherT.fromEither[Future](
        engine
          .validatePackages((mainPackage :: dependencies).toMap)
          .leftMap(
            PackageServiceErrors.Validation.handleLfEnginePackageError(_): DamlError
          )
      )
      _ <-
        if (disableUpgradeValidation) {
          logger.info(s"Skipping upgrade validation for package ${mainPackage._1}.")
          EitherT.rightT[Future, DamlError](())
        } else
          validateUpgrade(mainPackage._1, mainPackage._2)
    } yield ()

  private def validateUpgrade(upgradingPackageId: LfPackageId, upgradingPackage: Package)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DamlError, Unit] =
    upgradingPackage.metadata.upgradedPackageId match {
      case Some(upgradedPackageId) =>
        logger.info(s"Package $upgradingPackageId claims to upgrade package id $upgradedPackageId")
        for {
          upgradedArchiveMb <- EitherT.right(getLfArchive(upgradedPackageId))
          upgradedPackageMb <-
            upgradedArchiveMb
              .traverse(Decode.decodeArchive(_))
              .leftMap(Validation.handleLfArchiveError)
              .toEitherT[Future]
          upgradeCheckResult = TypecheckUpgrades.typecheckUpgrades(
            upgradingPackageId -> upgradingPackage,
            upgradedPackageId,
            upgradedPackageMb.map(_._2),
          )
          _ <- upgradeCheckResult match {
            case Success(()) =>
              logger.info(s"Typechecking upgrades for $upgradingPackageId succeeded.")
              EitherT.rightT[Future, DamlError](())
            case Failure(err: UpgradeError) =>
              EitherT
                .leftT[Future, Unit](
                  Validation.Upgradeability.Error(upgradingPackageId, upgradedPackageId, err)
                )
                .leftWiden[DamlError]

            case Failure(err: Throwable) =>
              EitherT
                .leftT[Future, Unit](
                  PackageServiceErrors.InternalError
                    .Unhandled(
                      err,
                      Some(
                        s"Typechecking upgrades for $upgradingPackageId failed with unknown error."
                      ),
                    )
                )
                .leftWiden[DamlError]
          }
        } yield ()
      case None =>
        logger.info(s"Package $upgradingPackageId does not upgrade anything.")
        EitherT.rightT[Future, DamlError](())
    }

  def vetPackages(packages: Seq[PackageId], syncVetting: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DamlError, Unit] =
    packageOps
      .vetPackages(packages, syncVetting)
      .leftMap[DamlError] { err =>
        implicit val code = err.code
        CantonPackageServiceError.IdentityManagerParentError(err)
      }

  /** Stores archives in the store and sends package upload event to participant event log for inclusion in ledger
    * sync event stream. This allows the ledger api server to update its package store accordingly and unblock
    * synchronous upload request if the request originated in the ledger api.
    * @param archives The archives to store. They must have been decoded and package-validated before.
    * @param sourceDescription description of the source of the package
    * @param submissionId upstream submissionId for ledger api server to recognize previous package upload requests
    * @param vetAllPackages if true, then the packages will be vetted automatically
    * @param synchronizeVetting if true, the future will terminate once the participant observed the package vetting on all connected domains
    * @return future holding whether storing and/or event sending failed (relevant to upstream caller)
    */
  def storeValidatedPackagesAndSyncEvent(
      archives: List[DamlLf.Archive],
      sourceDescription: String256M,
      submissionId: LedgerSubmissionId,
      dar: Option[Dar],
      vetAllPackages: Boolean,
      synchronizeVetting: Boolean,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, DamlError, Unit] = {

    EitherT
      .right(
        packagesDarsStore
          .append(archives, sourceDescription, dar)
          .map { _ =>
            // update our dependency cache
            // we need to do this due to an issue we can hit if we have pre-populated the cache
            // with the information about the package not being present (with a None)
            // now, that the package is loaded, we need to get rid of this None.
            dependencyResolver.clearPackagesNotPreviouslyFound()
          }
          .transformWith {
            case Success(_) =>
              logger.debug(
                s"Managed to upload one or more archives in submissionId $submissionId and sourceDescription $sourceDescription"
              )
              eventPublisher.publish(
                LedgerSyncEvent.PublicPackageUpload(
                  archives = archives,
                  sourceDescription = Some(sourceDescription.unwrap),
                  recordTime = ParticipantEventPublisher.now.toLf,
                  submissionId = Some(submissionId),
                )
              )
            case Failure(e) =>
              logger.warn(
                s"Failed to upload one or more archives in submissionId $submissionId and sourceDescription $sourceDescription",
                e,
              )
              eventPublisher.publish(
                LedgerSyncEvent.PublicPackageUploadRejected(
                  rejectionReason = e.getMessage,
                  recordTime = ParticipantEventPublisher.now.toLf,
                  submissionId = submissionId,
                )
              )
          }
      )
      .flatMap { _ =>
        if (vetAllPackages)
          vetPackages(archives.map(DamlPackageStore.readPackageId), synchronizeVetting)
        else
          EitherT.rightT(())
      }
  }

  override def onClosed(): Unit = Lifecycle.close(packagesDarsStore)(logger)

}

object PackageService {
  final case class DarDescriptor(hash: Hash, name: DarName)

  object DarDescriptor {
    implicit val getResult: GetResult[DarDescriptor] =
      GetResult(r => DarDescriptor(r.<<, String255.tryCreate(r.<<)))
  }

  final case class Dar(descriptor: DarDescriptor, bytes: Array[Byte]) {
    override def equals(other: Any): Boolean = other match {
      case that: Dar =>
        // Array equality only returns true when both are the same instance.
        // So by using sameElements to compare the bytes, we ensure that we compare the data, not the instance.
        (bytes sameElements that.bytes) && descriptor == that.descriptor
      case _ => false
    }
  }

  object Dar {
    implicit def getResult(implicit getResultByteArray: GetResult[Array[Byte]]): GetResult[Dar] =
      GetResult(r => Dar(r.<<, r.<<))
  }

  private def darToLf(
      dar: Dar
  ): Either[String, (DarDescriptor, archive.Dar[DamlLf.Archive])] = {
    val bytes = dar.bytes
    val payload = ByteString.copyFrom(bytes)
    val stream = new ZipInputStream(payload.newInput())
    DarParser
      .readArchive(dar.descriptor.name.str, stream)
      .fold(
        { _ =>
          Left(s"Cannot parse stored dar $dar")
        },
        x => Right(dar.descriptor -> x),
      )
  }

}
