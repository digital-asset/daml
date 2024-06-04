// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.DamlError
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.language.Util.dependenciesInTopologicalOrder
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.validation.{TypecheckUpgrades, UpgradeError}
import com.daml.logging.entries.LoggingValue.OfString
import com.digitalasset.canton.ledger.error.PackageServiceErrors.{InternalError, Validation}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageUpgradeValidator.PackageMap
import com.digitalasset.canton.participant.admin.PackageUploader.ErrorValidations
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import scalaz.std.either._
import scalaz.std.option._
import scalaz.std.scalaFuture.futureInstance
import scalaz.syntax.traverse._

import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.infixOrderingOps

object PackageUpgradeValidator {
  type PackageMap = Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]
}

class PackageUpgradeValidator(
    getPackageMap: TraceContext => PackageMap,
    getLfArchive: TraceContext => Ref.PackageId => Future[Option[Archive]],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  def validateUpgrade(
      upgradingPackages: List[(Ref.PackageId, Ast.Package)]
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] = {
    val upgradingPackagesMap = upgradingPackages.toMap
    val packagesInTopologicalOrder = dependenciesInTopologicalOrder(upgradingPackages.map(_._1), upgradingPackagesMap)
    val packageMap = getPackageMap(loggingContext.traceContext)

    def go(
        packageMap: PackageMap,
        deps: List[Ref.PackageId],
    ): EitherT[Future, DamlError, PackageMap] = deps match {
      case Nil => EitherT.pure[Future, DamlError](packageMap)
      case pkgId :: rest =>
        val pkg = upgradingPackagesMap(pkgId)
        val supportsUpgrades = pkg.languageVersion >= LanguageVersion.Features.packageUpgrades
        pkg.metadata match {
          case Some(pkgMetadata) =>
            for {
              _ <- EitherTUtil.ifThenET(supportsUpgrades)(
                // This check will look for the closest neighbors of pkgId for the package versioning ordering and
                // will load them from the DB and decode them. If one were to upload many packages that upgrade each
                // other, we will end up decoding the same package many times. Some of these cases could be sped up
                // by a cache depending on the order in which the packages are uploaded.
                validatePackageUpgrade((pkgId, pkg), pkgMetadata, packageMap)
              )
              res <- go(packageMap + ((pkgId, (pkgMetadata.name, pkgMetadata.version))), rest)
            } yield res
          case None =>
            logger.debug(
              s"Package metadata is not defined for ${pkgId}. Skipping upgrade validation."
            )
            go(packageMap, rest)
        }
    }
    go(packageMap, packagesInTopologicalOrder).map(_ => ())
  }

  private def validatePackageUpgrade(
      upgradingPackage: (Ref.PackageId, Ast.Package),
      upgradingPackageMetadata: Ast.PackageMetadata,
      packageMap: PackageMap,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] = {
    val upgradingPackageId = upgradingPackage._1
    val optUpgradingDar = Some(upgradingPackage)
    logger.info(s"Uploading DAR file for $upgradingPackageId.")
    existingVersionedPackageId(upgradingPackageMetadata, packageMap) match {
      case Some(uploadedPackageId) =>
        if (uploadedPackageId == upgradingPackageId)
          EitherT.rightT[Future, DamlError](
            logger.info(
              s"Ignoring upload of package $upgradingPackageId as it has been previously uploaded"
            )
          )
        else
          EitherT.leftT[Future, Unit](
            Validation.UpgradeVersion
              .Error(
                uploadedPackageId,
                upgradingPackageId,
                upgradingPackageMetadata.version,
              ): DamlError
          )

      case None =>
        for {
          optMaximalDar <- EitherT.right[DamlError](
            maximalVersionedDar(upgradingPackageMetadata, packageMap)
          )
          _ <- typecheckUpgrades(
            TypecheckUpgrades.MaximalDarCheck,
            optUpgradingDar,
            optMaximalDar,
          )
          optMinimalDar <- EitherT.right[DamlError](
            minimalVersionedDar(upgradingPackageMetadata, packageMap)
          )
          r <- typecheckUpgrades(
            TypecheckUpgrades.MinimalDarCheck,
            optMinimalDar,
            optUpgradingDar,
          )
          _ = logger.info(s"Typechecking upgrades for $upgradingPackageId succeeded.")
        } yield r
    }
  }

  private def lookupDar(pkgId: Ref.PackageId)(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] =
    for {
      optArchive <- getLfArchive(loggingContextWithTrace.traceContext)(pkgId)
      optPackage <- Future.fromTry {
        optArchive
          .traverse(Decode.decodeArchive(_))
          .handleError(Validation.handleLfArchiveError)
      }
    } yield optPackage

  private def existingVersionedPackageId(
      packageMetadata: Ast.PackageMetadata,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
  ): Option[Ref.PackageId] = {
    val pkgName = packageMetadata.name
    val pkgVersion = packageMetadata.version
    packageMap.collectFirst { case (pkgId, (`pkgName`, `pkgVersion`)) => pkgId }
  }

  private def minimalVersionedDar(
      packageMetadata: Ast.PackageMetadata,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val pkgName = packageMetadata.name
    val pkgVersion = packageMetadata.version
    packageMap
      .collect { case (pkgId, (`pkgName`, pkgVersion)) =>
        (pkgId, pkgVersion)
      }
      .filter { case (_, version) => pkgVersion < version }
      .minByOption { case (_, version) => version }
      .traverse { case (pId, _) => lookupDar(pId) }
      .map(_.flatten)
  }

  private def maximalVersionedDar(
      packageMetadata: Ast.PackageMetadata,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] = {
    val pkgName = packageMetadata.name
    val pkgVersion = packageMetadata.version
    packageMap
      .collect { case (pkgId, (`pkgName`, pkgVersion)) =>
        (pkgId, pkgVersion)
      }
      .filter { case (_, version) => pkgVersion > version }
      .maxByOption { case (_, version) => version }
      .traverse { case (pId, _) => lookupDar(pId) }
      .map(_.flatten)
  }

  private def strictTypecheckUpgrades(
      phase: TypecheckUpgrades.UploadPhaseCheck,
      optNewDar1: Option[(Ref.PackageId, Ast.Package)],
      oldPkgId2: Ref.PackageId,
      optOldPkg2: Option[Ast.Package],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] = {
    LoggingContextWithTrace.withEnrichedLoggingContext(
      "upgradeTypecheckPhase" -> OfString(phase.toString)
    ) { implicit loggingContext =>
      optNewDar1 match {
        case None => EitherT.rightT(())

        case Some((newPkgId1, newPkg1)) =>
          logger.info(s"Package $newPkgId1 claims to upgrade package id $oldPkgId2")
          EitherT(
            Future(
              TypecheckUpgrades
                .typecheckUpgrades((newPkgId1, newPkg1), oldPkgId2, optOldPkg2)
                .toEither
            )
          ).leftMap[DamlError] {
            case err: UpgradeError => Validation.Upgradeability.Error(newPkgId1, oldPkgId2, err)
            case unhandledErr =>
              InternalError.Unhandled(
                unhandledErr,
                Some(s"Typechecking upgrades for $oldPkgId2 failed with unknown error."),
              )
          }
      }
    }
  }

  private def typecheckUpgrades(
      typecheckPhase: TypecheckUpgrades.UploadPhaseCheck,
      optNewDar1: Option[(Ref.PackageId, Ast.Package)],
      optOldDar2: Option[(Ref.PackageId, Ast.Package)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] =
    (optNewDar1, optOldDar2) match {
      case (None, _) | (_, None) => EitherT.rightT(())

      case (Some((newPkgId1, newPkg1)), Some((oldPkgId2, oldPkg2))) =>
        strictTypecheckUpgrades(
          typecheckPhase,
          Some((newPkgId1, newPkg1)),
          oldPkgId2,
          Some(oldPkg2),
        )
    }
}
