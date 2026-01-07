// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.{EitherT, OptionT}
import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.ledger.api.{
  EnrichedVettedPackage,
  EnrichedVettedPackages,
  ListVettedPackagesOpts,
  ParticipantVettedPackages,
  SinglePackageTargetVetting,
  UpdateVettedPackagesOpts,
  VettedPackagesRef,
}
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.digitalasset.canton.ledger.participant.state.PackageDescription
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.{
  CannotRemoveOnlyDarForPackage,
  MainPackageInUse,
  PackageRemovalError,
  PackageVetted,
}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.Vetting.{
  VettingReferenceEmpty,
  VettingReferenceMoreThanOne,
}
import com.digitalasset.canton.participant.admin.PackageService.*
import com.digitalasset.canton.participant.admin.data.UploadDarData
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.DamlPackageStore.readPackageId
import com.digitalasset.canton.participant.store.memory.{
  MutablePackageMetadataView,
  PackageMetadataView,
}
import com.digitalasset.canton.participant.topology.PackageOps
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.{LedgerSubmissionId, LfPackageId, ProtoDeserializationError}
import com.digitalasset.daml.lf.archive
import com.digitalasset.daml.lf.archive.{DamlLf, DarParser, Error as LfArchiveError}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.language.Ast.Package
import com.google.protobuf.ByteString
import slick.jdbc.GetResult

import java.util.UUID
import java.util.zip.ZipInputStream
import scala.concurrent.ExecutionContext

trait DarService {
  def upload(
      dars: Seq[UploadDarData],
      submissionIdO: Option[LedgerSubmissionId],
      vettingInfo: Option[(PhysicalSynchronizerId, PackageVettingSynchronization)],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Seq[DarMainPackageId]]

  def validateDar(
      payload: ByteString,
      filename: String,
      synchronizerId: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, DarMainPackageId]

  def getDar(mainPackageId: DarMainPackageId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, PackageService.Dar]

  def getDarContents(mainPackageId: DarMainPackageId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, Seq[PackageDescription]]

  def listDars(limit: Option[Int])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[PackageService.DarDescription]]
}

class PackageService(
    protected val loggerFactory: NamedLoggerFactory,
    metrics: ParticipantMetrics,
    packageOps: PackageOps,
    packageUploader: PackageUploader,
    protected val timeouts: ProcessingTimeout,
)(implicit ec: ExecutionContext)
    extends DarService
    with NamedLogging
    with FlagCloseable {

  private val packageLoader = new DeduplicatingPackageLoader()
  private val packageDarStore = packageUploader.packageMetadataView.packageStore

  def getPackageMetadataView: PackageMetadataView = packageUploader.packageMetadataView

  def getLfArchive(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[DamlLf.Archive]] =
    packageDarStore.getPackage(packageId)

  def listPackages(limit: Option[Int] = None)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[PackageDescription]] =
    packageDarStore.listPackages(limit)

  def getPackageDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, PackageDescription] =
    packageDarStore.getPackageDescription(packageId)

  def getPackage(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Package]] =
    FutureUnlessShutdown.recoverFromAbortException(
      packageLoader.loadPackage(
        packageId,
        packageId => getLfArchive(packageId).failOnShutdownToAbortException("getLfArchive"),
        metrics.ledgerApiServer.execution.getLfPackage,
      )
    )

  def removePackage(
      packageId: PackageId,
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    if (force) {
      logger.info(s"Forced removal of package $packageId")
      EitherT.right(packageDarStore.removePackage(packageId))
    } else {
      val checkUnused =
        packageOps.checkPackageUnused(packageId)

      val checkNotVetted =
        packageOps
          .hasVettedPackageEntry(packageId)
          .flatMap[RpcError, Unit] {
            case true => EitherT.leftT(new PackageVetted(packageId))
            case false => EitherT.rightT(())
          }

      for {
        _ <- neededForAdminWorkflow(packageId)
        _ <- checkUnused
        _ <- checkNotVetted
        _ = logger.debug(s"Removing package $packageId")
        _ <- EitherT.right(packageDarStore.removePackage(packageId))
      } yield ()
    }

  def removeDar(mainPackageId: DarMainPackageId, psids: Set[PhysicalSynchronizerId])(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    ifDarExists(mainPackageId)(removeDarLf(_, _, psids))(ifNotExistsOperationFailed =
      "DAR archive removal"
    )

  def vetDar(
      mainPackageId: DarMainPackageId,
      synchronizeVetting: PackageVettingSynchronization,
      psid: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    ifDarExists(mainPackageId) { (_, darLf) =>
      packageOps
        .vetPackages(darLf.all.map(readPackageId), synchronizeVetting, psid)
        .leftWiden[RpcError]
    }(ifNotExistsOperationFailed = "DAR archive vetting")

  def unvetDar(mainPackageId: DarMainPackageId, psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    ifDarExists(mainPackageId) { (descriptor, lfArchive) =>
      val packages = lfArchive.all.map(readPackageId)
      val mainPkg = readPackageId(lfArchive.main)
      revokeVettingForDar(mainPkg, packages, descriptor, psid)
    }(ifNotExistsOperationFailed = "DAR archive unvetting")

  def resolveTargetVettingReferences(
      targetState: SinglePackageTargetVetting[VettedPackagesRef],
      snapshot: PackageMetadata,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    RpcError,
    List[SinglePackageTargetVetting[PackageId]],
  ] =
    targetState.ref.findMatchingPackages(snapshot) match {
      // When vetting, we expect every reference to give exactly one package.
      // With unvetting we are more lenient to allow the use case where a
      // app developer provides a script that unvets an old, deprecated package
      // "just in case".
      case Left(msg) =>
        val err = VettingReferenceEmpty.Reject(msg, targetState.ref)
        if (targetState.isUnvetting) {
          logger.debug(err.cause)
          EitherT.rightT[FutureUnlessShutdown, RpcError](List())
        } else {
          EitherT.leftT[FutureUnlessShutdown, List[SinglePackageTargetVetting[PackageId]]](
            err
          )
        }

      // When vetting, we expect every reference to give exactly one package in
      // order to rule out the case where two versions of the same package are
      // vetted. On the other hand, when unvetting, it is safe to unvet all
      // versions of a package.
      case Right(matchingPackages) =>
        if (targetState.isVetting && matchingPackages.sizeIs >= 2) {
          EitherT.leftT[FutureUnlessShutdown, List[SinglePackageTargetVetting[PackageId]]](
            VettingReferenceMoreThanOne.Reject(targetState.ref, matchingPackages)
          )
        } else {
          EitherT.rightT[FutureUnlessShutdown, RpcError](
            matchingPackages.toList.map(SinglePackageTargetVetting(_, targetState.bounds))
          )
        }
    }

  def updateVettedPackages(
      opts: UpdateVettedPackagesOpts,
      synchronizerId: PhysicalSynchronizerId,
      synchronizeVetting: PackageVettingSynchronization,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    RpcError,
    (Option[EnrichedVettedPackages], Option[EnrichedVettedPackages]),
  ] = {
    val snapshot = getPackageMetadataView.getSnapshot
    val targetStates = opts.toTargetStates
    val dryRunSnapshot = Option.when(opts.dryRun)(snapshot)
    for {
      resolvedTargetStates <- targetStates.parTraverse(resolveTargetVettingReferences(_, snapshot))
      preAndPost <- packageOps
        .updateVettedPackages(
          resolvedTargetStates.flatten,
          synchronizerId,
          synchronizeVetting,
          dryRunSnapshot,
          opts.expectedTopologySerial,
          updateForceFlags = Some(opts.forceFlags),
        )
        .leftWiden[RpcError]
    } yield {
      val (pre, post) = preAndPost
      (pre.map(enrichVettedPackages), post.map(enrichVettedPackages))
    }
  }

  def listVettedPackages(
      opts: ListVettedPackagesOpts
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Seq[EnrichedVettedPackages]] = {
    val snapshot = getPackageMetadataView.getSnapshot
    val packagePredicate = opts.toPackagePredicate(snapshot)
    packageOps
      .getVettedPackages(opts)
      .leftWiden[RpcError]
      .map(_.flatMap(pkgs => filterAndEnrich(pkgs, predicate = packagePredicate)))
  }

  private def enrichVettedPackages(vetted: ParticipantVettedPackages)(implicit
      traceContext: TraceContext
  ): EnrichedVettedPackages =
    EnrichedVettedPackages(
      vetted.packages.map(enrichVettedPackage),
      vetted.participantId,
      vetted.synchronizerId,
      vetted.serial,
    )

  private def filterAndEnrich(vetted: ParticipantVettedPackages, predicate: LfPackageId => Boolean)(
      implicit traceContext: TraceContext
  ): Option[EnrichedVettedPackages] = {
    val filteredPackages = vetted.packages.filter(pkg => predicate(pkg.packageId))
    Option.when(filteredPackages.nonEmpty)(
      EnrichedVettedPackages(
        filteredPackages.map(enrichVettedPackage),
        vetted.participantId,
        vetted.synchronizerId,
        vetted.serial,
      )
    )
  }

  private def enrichVettedPackage(vetted: VettedPackage)(implicit
      traceContext: TraceContext
  ): EnrichedVettedPackage =
    getPackageMetadataView.getSnapshot.packageIdVersionMap
      .get(vetted.packageId)
      .fold(EnrichedVettedPackage(vetted, None, None)) { case (name, version) =>
        EnrichedVettedPackage(
          vetted,
          Some(name),
          Some(version),
        )
      }

  private def ifDarExists(mainPackageId: DarMainPackageId)(
      action: (
          DarDescription,
          archive.Dar[DamlLf.Archive],
      ) => EitherT[FutureUnlessShutdown, RpcError, Unit]
  )(ifNotExistsOperationFailed: => String)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    for {
      dar <- packageDarStore
        .getDar(mainPackageId)
        .toRight(
          CantonPackageServiceError.Fetching.DarNotFound
            .Reject(
              operation = ifNotExistsOperationFailed,
              mainPackageId = mainPackageId.unwrap,
            )
        )
      darLfE <- EitherT.fromEither[FutureUnlessShutdown](
        PackageService.darToLf(dar).leftMap { err =>
          CantonPackageServiceError.InternalError.Error(err)
        }
      )
      (descriptor, lfArchive) =
        darLfE
      _ <- action(descriptor, lfArchive)
    } yield ()

  private def neededForAdminWorkflow(packageId: PackageId)(implicit
      elc: ErrorLoggingContext
  ): EitherT[FutureUnlessShutdown, PackageRemovalError, Unit] =
    EitherTUtil.condUnitET(
      !AdminWorkflowServices.AllBuiltInPackages.keySet.contains(packageId),
      new PackageRemovalErrorCode.CannotRemoveAdminWorkflowPackage(packageId),
    )

  private def removeDarLf(
      darDescriptor: DarDescription,
      dar: archive.Dar[DamlLf.Archive],
      psids: Set[PhysicalSynchronizerId],
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] = {
    // Can remove the DAR if:
    // 1. The main package of the dar is unused
    // 2. For all dependencies of the DAR, either:
    //     - They are unused
    //     - Or they are contained in another vetted DAR
    // 3. The main package of the dar is either:
    //     - Already un-vetted
    //     - Or can be automatically un-vetted, by revoking a vetting transaction corresponding to all packages in the DAR

    val packages = {
      import scalaz.syntax.traverse.*
      dar.map(readPackageId)
    }

    val mainPkg = readPackageId(dar.main)
    for {
      _notAdminWf <- neededForAdminWorkflow(mainPkg)

      _mainUnused <- packageOps
        .checkPackageUnused(mainPkg)
        .leftMap(err =>
          new MainPackageInUse(err.pkg, darDescriptor, err.contract, err.synchronizerId)
        )

      packageUsed <- EitherT
        .liftF(packages.dependencies.parTraverse(p => packageOps.checkPackageUnused(p).value))

      usedPackages = packageUsed.mapFilter {
        case Left(packageInUse: PackageRemovalErrorCode.PackageInUse) => Some(packageInUse.pkg)
        case Right(()) => None
      }

      _unit <- packageDarStore
        .anyPackagePreventsDarRemoval(usedPackages, darDescriptor)
        .toLeft(())
        .leftMap(p => new CannotRemoveOnlyDarForPackage(p, darDescriptor))

      packagesThatCanBeRemoved_ <- EitherT
        .liftF(
          packageDarStore
            .determinePackagesExclusivelyInDar(packages.all, darDescriptor)
        )

      packagesThatCanBeRemoved = packagesThatCanBeRemoved_.toList

      _ <- MonadUtil.sequentialTraverse(psids.toSeq)(psid =>
        revokeVettingForDar(
          mainPkg,
          packagesThatCanBeRemoved,
          darDescriptor,
          psid,
        )
      )

      // TODO(#26078): update documentation to reflect main package dependency removal changes
      _unit <- EitherT.liftF(
        packagesThatCanBeRemoved.parTraverse(packageDarStore.removePackage(_))
      )

      _removed <- {
        logger.info(s"Removing dar ${darDescriptor.mainPackageId}")
        EitherT
          .liftF[FutureUnlessShutdown, RpcError, Unit](
            packageDarStore.removeDar(darDescriptor.mainPackageId)
          )
      }
    } yield ()
  }

  private def revokeVettingForDar(
      mainPkg: PackageId,
      packages: List[PackageId],
      darDescriptor: DarDescription,
      psid: PhysicalSynchronizerId,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    packageOps
      .hasVettedPackageEntry(mainPkg)
      .flatMap { isVetted =>
        if (!isVetted)
          EitherT.pure[FutureUnlessShutdown, RpcError](
            logger.info(
              s"Package with id $mainPkg is already unvetted. Doing nothing for the unvet operation"
            )
          )
        else
          packageOps
            .revokeVettingForPackages(
              mainPkg,
              packages,
              darDescriptor,
              psid,
              // Unvetting a DAR requires AllowUnvettedDependencies because it is going to unvet all
              // packages from the DAR, even the utility packages. UnvetDar is an experimental
              // operation that requires expert-level knowledge.
              ForceFlags(ForceFlag.AllowUnvettedDependencies),
            )
            .leftWiden
      }

  /** Performs the upload DAR flow:
    *   1. Decodes the provided DAR payload
    *   1. Validates the resulting Daml packages
    *   1. Persists the DAR and decoded archives in the DARs and package stores
    *   1. Dispatches the package upload event for inclusion in the ledger sync event stream
    *   1. Updates the
    *      [[com.digitalasset.canton.participant.store.memory.MutablePackageMetadataView]] which is
    *      used for subsequent DAR upload validations and incoming Ledger API queries
    *   1. Issues a package vetting topology transaction for all uploaded packages (if `vettingInfo`
    *      is nonempty) and waits for for its completion using the synchronization provided by the
    *      `PackageVettingSynchronization` instance in `vettingInfo`)
    *
    * @param darBytes
    *   The DAR payload to store.
    * @param description
    *   A description of the DAR.
    * @param submissionIdO
    *   upstream submissionId for ledger api server to recognize previous package upload requests
    * @param vettingInfo
    *   If set, checks that the packages have been vetted on the specified synchronizer. The Future
    *   returned by the check will complete once the synchronizer has observed the vetting for the
    *   new packages. The caller may also pass be a no-op implementation that immediately returns,
    *   depending no the caller's needs for synchronization.
    */
  final def upload(
      darBytes: ByteString,
      description: Option[String],
      submissionIdO: Option[LedgerSubmissionId],
      vettingInfo: Option[(PhysicalSynchronizerId, PackageVettingSynchronization)],
      expectedMainPackageId: Option[LfPackageId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, DarMainPackageId] =
    upload(
      Seq(UploadDarData(darBytes, description, expectedMainPackageId)),
      submissionIdO,
      vettingInfo,
    ).subflatMap {
      case Seq(mainPackageId) => Right(mainPackageId)
      case Seq() =>
        Left(
          PackageServiceErrors.InternalError.Generic(
            "Uploading the DAR unexpectedly did not return a DAR package ID"
          )
        )
      case many =>
        Left(
          PackageServiceErrors.InternalError.Generic(
            s"Uploading the DAR unexpectedly returned multiple DAR package IDs: $many"
          )
        )
    }

  /** Performs the upload DAR flow:
    *   1. Decodes the provided DAR payloads
    *   1. Validates the resulting Daml packages
    *   1. Persists the DARs and decoded archives in the DARs and package stores
    *   1. Dispatches the package upload event for inclusion in the ledger sync event stream
    *   1. Updates the
    *      [[com.digitalasset.canton.participant.store.memory.MutablePackageMetadataView]] which is
    *      used for subsequent DAR upload validations and incoming Ledger API queries
    *   1. Issues a package vetting topology transaction for all uploaded packages (if `vettingInfo`
    *      is set).
    *
    * @param dars
    *   The DARs (bytes, description, expected main package) to upload.
    * @param submissionIdO
    *   upstream submissionId for ledger api server to recognize previous package upload requests
    * @param vettingInfo
    *   If set, checks that the packages have been vetted on the specified synchronizer. The Future
    *   returned by the check will complete once the synchronizer has observed the vetting for the
    *   new packages. The caller may also pass be a no-op implementation that immediately returns,
    *   depending no the caller's needs for synchronization.
    */
  def upload(
      dars: Seq[UploadDarData],
      submissionIdO: Option[LedgerSubmissionId],
      vettingInfo: Option[(PhysicalSynchronizerId, PackageVettingSynchronization)],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Seq[DarMainPackageId]] = {
    val submissionId =
      submissionIdO.getOrElse(LedgerSubmissionId.assertFromString(UUID.randomUUID().toString))
    for {
      uploadResult <- MonadUtil.sequentialTraverse(dars) {
        case UploadDarData(darBytes, description, expectedMainPackageId) =>
          packageUploader.upload(
            darBytes,
            description,
            submissionId,
            expectedMainPackageId,
          )
      }
      (mainPkgs, allPackages) = uploadResult.foldMap { case (mainPkg, dependencies) =>
        (List(DarMainPackageId.tryCreate(mainPkg)), mainPkg +: dependencies)
      }
      _ <- vettingInfo.traverse_ { case (synchronizerId, synchronizeVetting) =>
        vetPackages(allPackages, synchronizeVetting, synchronizerId)
      }

    } yield mainPkgs // try is okay as we get this package-id from the uploader
  }

  /** Returns all dars that reference a certain package id */
  def getPackageReferences(packageId: LfPackageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DarDescription]] = packageDarStore.getPackageReferences(packageId)

  def validateDar(
      payload: ByteString,
      darName: String,
      synchronizerId: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, DarMainPackageId] = {
    import cats.implicits.catsSyntaxSemigroup
    import PackageMetadata.Implicits.packageMetadataSemigroup
    for {
      darPkgs <- packageUploader.validateDar(payload, darName)
      (mainPackageId, allPackages) = darPkgs
      targetVettingState = allPackages.map { case (packageId, _) =>
        SinglePackageTargetVetting(packageId, bounds = Some((None, None)))
      }
      dryRunSnapshot =
        allPackages
          .map { case (packageId, packageAst) => PackageMetadata.from(packageId, packageAst) }
          .foldLeft(getPackageMetadataView.getSnapshot)(_ |+| _)

      _ <- packageOps
        .updateVettedPackages(
          targetVettingState,
          synchronizerId,
          PackageVettingSynchronization.NoSync,
          Some(dryRunSnapshot),
          expectedTopologySerial = None,
        )
        .leftWiden[RpcError]
    } yield DarMainPackageId.tryCreate(mainPackageId)
  }

  override def getDar(mainPackageId: DarMainPackageId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, PackageService.Dar] =
    packageDarStore.getDar(mainPackageId)

  override def listDars(limit: Option[Int])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[PackageService.DarDescription]] = packageDarStore.listDars(limit)

  override def getDarContents(mainPackageId: DarMainPackageId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, Seq[PackageDescription]] =
    packageDarStore
      .getPackageDescriptionsOfDar(mainPackageId)

  def vetPackages(
      packages: Seq[PackageId],
      synchronizeVetting: PackageVettingSynchronization,
      psid: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    packageOps
      .vetPackages(packages, synchronizeVetting, psid)
      .leftMap { err =>
        implicit val code = err.code
        CantonPackageServiceError.IdentityManagerParentError(err)
      }

  override def onClosed(): Unit = LifeCycle.close(packageUploader)(logger)

}

object PackageService {
  def apply(
      clock: Clock,
      engine: Engine,
      mutablePackageMetadataView: MutablePackageMetadataView,
      enableStrictDarValidation: Boolean,
      loggerFactory: NamedLoggerFactory,
      metrics: ParticipantMetrics,
      packageOps: PackageOps,
      timeouts: ProcessingTimeout,
  )(implicit
      ec: ExecutionContext
  ): PackageService = {
    val packageUploader = new PackageUploader(
      clock,
      mutablePackageMetadataView.packageStore,
      engine,
      enableStrictDarValidation,
      mutablePackageMetadataView,
      timeouts,
      loggerFactory,
    )

    new PackageService(
      loggerFactory,
      metrics,
      packageOps,
      packageUploader,
      timeouts,
    )
  }

  // Opaque type for the main package id of a DAR
  final case class DarMainPackageId private (value: String255) extends AnyVal {
    def unwrap: String = value.unwrap
    def toProtoPrimitive: String = value.toProtoPrimitive
    def str: String = value.str
  }

  object DarMainPackageId {
    def create(str: String): Either[String, DarMainPackageId] =
      for {
        _ <- LfPackageId.fromString(str)
        lengthLimited <- String255.create(str)
      } yield DarMainPackageId(lengthLimited)

    def tryCreate(str: String): DarMainPackageId =
      create(str)
        .leftMap(err =>
          throw new IllegalArgumentException(
            s"Cannot convert '$str' to ${classOf[DarMainPackageId].getSimpleName}: $err"
          )
        )
        .merge

    def fromProtoPrimitive(str: String): Either[ProtoDeserializationError, DarMainPackageId] =
      create(str).leftMap(err => ProtoDeserializationError.StringConversionError(err))
  }

  final case class DarDescription(
      mainPackageId: DarMainPackageId,
      description: String255,
      name: String255,
      version: String255,
  ) extends PrettyPrinting {

    override protected def pretty: Pretty[DarDescription] = prettyOfClass(
      param("dar-id", _.mainPackageId.unwrap.readableHash),
      param("name", _.name.str.unquoted),
      param("version", _.version.str.unquoted),
      param("description", _.description.str.unquoted),
    )

  }

  object DarDescription {
    implicit val getResult: GetResult[DarDescription] =
      GetResult(r =>
        DarDescription(
          mainPackageId = DarMainPackageId.tryCreate(r.<<),
          description = String255.tryCreate(r.<<),
          name = String255.tryCreate(r.<<),
          version = String255.tryCreate(r.<<),
        )
      )
  }

  final case class Dar(descriptor: DarDescription, bytes: Array[Byte]) {
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
  ): Either[String, (DarDescription, archive.Dar[DamlLf.Archive])] = {
    val bytes = dar.bytes
    val payload = ByteString.copyFrom(bytes)
    val stream = new ZipInputStream(payload.newInput())
    DarParser
      .readArchive(dar.descriptor.description.str, stream)
      .fold(
        _ => Left(s"Cannot parse stored dar $dar"),
        x => Right(dar.descriptor -> x),
      )
  }

  def catchUpstreamErrors[E](
      attempt: Either[LfArchiveError, E]
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, RpcError, E] =
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

}
