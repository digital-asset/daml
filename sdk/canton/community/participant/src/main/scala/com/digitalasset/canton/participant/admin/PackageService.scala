// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.Eval
import cats.data.EitherT
import cats.implicits.toBifunctorOps
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.{ContextualizedErrorLogger, DamlError}
import com.daml.lf.archive
import com.daml.lf.archive.{DarParser, Error as LfArchiveError}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast.Package
import com.digitalasset.canton.LedgerSubmissionId
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.DarName
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.ledger.error.PackageServiceErrors
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
import com.digitalasset.canton.participant.store.DamlPackageStore.readPackageId
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.protocol.{PackageDescription, PackageInfoService}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, PathUtils}
import com.google.protobuf.ByteString
import slick.jdbc.GetResult

import java.nio.file.Paths
import java.util.UUID
import java.util.zip.ZipInputStream
import scala.concurrent.{ExecutionContext, Future}

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
    val dependencyResolver: PackageDependencyResolver,
    packageUploader: Eval[PackageUploader],
    hashOps: HashOps,
    packageOps: PackageOps,
    metrics: ParticipantMetrics,
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
      metrics.ledgerApiServer.daml.execution.getLfPackage,
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

      _unit <- revokeVettingForDar(mainPkg, packages, darDescriptor)

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
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, DamlError, Hash] = {
    val hash = hashOps.digest(HashPurpose.DarIdentifier, payload)
    val darName = PathUtils.getFilenameWithoutExtension(Paths.get(filename).getFileName)

    upload(
      darBytes = payload,
      fileNameO = Some(darName),
      sourceDescriptionO = None,
      submissionId = LedgerSubmissionId.assertFromString(UUID.randomUUID().toString),
      vetAllPackages = vetAllPackages,
      synchronizeVetting = synchronizeVetting,
    ).map(_ => hash)
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

  def vetPackages(packages: Seq[PackageId], syncVetting: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DamlError, Unit] =
    packageOps
      .vetPackages(packages, syncVetting)
      .leftMap[DamlError] { err =>
        implicit val code = err.code
        CantonPackageServiceError.IdentityManagerParentError(err)
      }

  /** Performs the upload DAR flow:
    *   1. Decodes the provided DAR payload
    *   2. Validates the resulting Daml packages
    *   3. Persists the DAR and decoded archives in the DARs and package stores
    *   4. Dispatches the package upload event for inclusion in the ledger sync event stream
    *   5. Updates the in-memory package-id resolution state used for subsequent validations
    *   6. Issues a package vetting topology transaction for all uploaded packages (if `vetAllPackages` is enabled) and waits for
    *      for its completion (if `synchronizeVetting` is enabled).
    * @param darBytes The DAR payload to store.
    * @param fileNameO The DAR filename, present if uploaded via the Admin API.
    * @param sourceDescriptionO description of the source of the package
    * @param submissionId upstream submissionId for ledger api server to recognize previous package upload requests
    * @param vetAllPackages if true, then the packages will be vetted automatically
    * @param synchronizeVetting if true, the future will terminate once the participant observed the package vetting on all connected domains
    */
  def upload(
      darBytes: ByteString,
      fileNameO: Option[String],
      sourceDescriptionO: Option[String],
      submissionId: LedgerSubmissionId,
      vetAllPackages: Boolean,
      synchronizeVetting: Boolean,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, DamlError, Unit] =
    for {
      uploadedPackageIds <- packageUploader.value.validateAndStorePackages(
        darPayload = darBytes,
        fileNameO = fileNameO,
        sourceDescriptionO = sourceDescriptionO,
        submissionId = submissionId,
      )
      _ <- EitherTUtil.ifThenET(vetAllPackages)(
        vetPackages(uploadedPackageIds, synchronizeVetting)
      )
    } yield ()

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

  def catchUpstreamErrors[T](
      attempt: Either[LfArchiveError, T]
  )(implicit
      executionContext: ExecutionContext,
      contextualizedErrorLogger: ContextualizedErrorLogger,
  ): EitherT[FutureUnlessShutdown, DamlError, T] = {

    EitherT.fromEither {
      attempt.leftMap {
        case LfArchiveError.InvalidDar(entries, cause) =>
          PackageServiceErrors.Reading.InvalidDar.Error(entries.entries.keys.toSeq, cause)
        case LfArchiveError.InvalidZipEntry(name, entries) =>
          PackageServiceErrors.Reading.InvalidZipEntry.Error(name, entries.entries.keys.toSeq)
        case LfArchiveError.InvalidLegacyDar(entries) =>
          PackageServiceErrors.Reading.InvalidLegacyDar.Error(entries.entries.keys.toSeq)
        case LfArchiveError.ZipBomb =>
          PackageServiceErrors.Reading.ZipBomb.Error(LfArchiveError.ZipBomb.getMessage)
        case e: LfArchiveError => PackageServiceErrors.Reading.ParseError.Error(e.msg)
        case e => PackageServiceErrors.InternalError.Unhandled(e)
      }
    }
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
