// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.DamlError
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.language.Util.{PkgIdWithNameAndVersion, dependenciesInTopologicalOrder}
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
import scalaz.std.either.*
import scalaz.std.option.*
import scalaz.std.scalaFuture.futureInstance
import scalaz.syntax.traverse.*

import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.infixOrderingOps

object PackageUpgradeValidator {
  type PackageMap = Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion, Option[LanguageVersion])]
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
    val packagesInTopologicalOrder =
      dependenciesInTopologicalOrder(upgradingPackages.map(_._1), upgradingPackagesMap)
    val packageMap = getPackageMap(loggingContext.traceContext)

    def go(
        packageMap: PackageMap,
        deps: List[Ref.PackageId],
    ): EitherT[Future, DamlError, PackageMap] = deps match {
      case Nil => EitherT.pure[Future, DamlError](packageMap)
      case pkgId :: rest =>
        val pkg = upgradingPackagesMap(pkgId)
        pkg.metadata match {
          case Some(pkgMetadata) =>
            for {
              _ <- EitherTUtil.ifThenET(supportsUpgrades(pkg))(
                // This check will look for the closest neighbors of pkgId for the package versioning ordering and
                // will load them from the DB and decode them. If one were to upload many packages that upgrade each
                // other, we will end up decoding the same package many times. Some of these cases could be sped up
                // by a cache depending on the order in which the packages are uploaded.
                validatePackageUpgrade((pkgId, pkg), pkgMetadata, packageMap, upgradingPackagesMap)
              )
              res <- go(if (supportsUpgrades(pkg)) {
                packageMap + ((pkgId, (pkgMetadata.name, pkgMetadata.version, Some(pkg.languageVersion))))
              } else {
                packageMap
              }, rest)
            } yield res
          case None =>
            logger.debug(
              s"Package metadata is not defined for $pkgId. Skipping upgrade validation."
            )
            go(packageMap, rest)
        }
    }
    go(packageMap, packagesInTopologicalOrder).map(_ => ())
  }

  private def validatePackageUpgrade(
      uploadedPackage: (Ref.PackageId, Ast.Package),
      uploadedPackageMetadata: Ast.PackageMetadata,
      packageMap: PackageMap,
      upgradingPackagesMap: Map[Ref.PackageId, Ast.Package],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] = {
    val (uploadedPackageId, uploadedPackageAst) = uploadedPackage
    val uploadedPackageIdWithMeta: PkgIdWithNameAndVersion =
      PkgIdWithNameAndVersion(uploadedPackageId, uploadedPackageMetadata)
    val optUploadedDar = Some((uploadedPackageIdWithMeta, uploadedPackageAst))
    logger.info(
      s"Uploading DAR file for $uploadedPackageIdWithMeta in submission ID ${loggingContext.serializeFiltered("submissionId")}."
    )
    existingVersionedPackageId(uploadedPackageMetadata, packageMap) match {
      case Some(existingPackageId) =>
        if (existingPackageId == uploadedPackageId)
          EitherT.rightT[Future, DamlError](
            logger.info(
              s"Ignoring upload of package $uploadedPackageIdWithMeta as it has been previously uploaded"
            )
          )
        else
          EitherT.leftT[Future, Unit](
            Validation.UpgradeVersion
              .Error(
                uploadedPackage = uploadedPackageIdWithMeta,
                existingPackage = existingPackageId,
              ): DamlError
          )

      case None =>
        for {
          _ <- typecheckUpgrades(
            TypecheckUpgrades.StandaloneDarCheck(
              optUploadedDar,
            ),
            packageMap,
          )
          optMaximalDar <- EitherT.right[DamlError](
            maximalVersionedDar(uploadedPackageMetadata, packageMap, upgradingPackagesMap)
          )
          _ <- typecheckUpgrades(
            TypecheckUpgrades.MaximalDarCheck(
              optMaximalDar,
              optUploadedDar,
            ),
            packageMap,
          )
          optMinimalDar <- EitherT.right[DamlError](
            minimalVersionedDar(uploadedPackageMetadata, packageMap, upgradingPackagesMap)
          )
          r <- typecheckUpgrades(
            TypecheckUpgrades.MinimalDarCheck(
              optUploadedDar,
              optMinimalDar,
            ),
            packageMap,
          )
          _ = logger.info(s"Typechecking upgrades for $uploadedPackageIdWithMeta succeeded.")
        } yield r
    }
  }

  /** Looks up [[pkgId]], first in the [[upgradingPackagesMap]] and then in the package store.
    */
  private def lookupDar(
      pkgId: Ref.PackageId,
      upgradingPackagesMap: Map[Ref.PackageId, Ast.Package],
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Future[Option[(Ref.PackageId, Ast.Package)]] =
    upgradingPackagesMap.get(pkgId) match {
      case Some(pkg) => Future.successful(Some((pkgId, pkg)))
      case None =>
        for {
          optArchive <- getLfArchive(loggingContextWithTrace.traceContext)(pkgId)
          optPackage <- Future.fromTry {
            optArchive
              .traverse(Decode.decodeArchive(_))
              .handleError(Validation.handleLfArchiveError)
          }
        } yield optPackage
    }

  private def existingVersionedPackageId(
      packageMetadata: Ast.PackageMetadata,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion, Option[LanguageVersion])],
  ): Option[Ref.PackageId] = {
    val pkgName = packageMetadata.name
    val pkgVersion = packageMetadata.version
    packageMap.collectFirst { case (pkgId, (`pkgName`, `pkgVersion`, _)) => pkgId }
  }

  private def minimalVersionedDar(
      packageMetadata: Ast.PackageMetadata,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion, Option[LanguageVersion])],
      upgradingPackagesMap: Map[Ref.PackageId, Ast.Package],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(PkgIdWithNameAndVersion, Ast.Package)]] = {
    val pkgName = packageMetadata.name
    val pkgVersion = packageMetadata.version
    packageMap
      .collect { case (pkgId, (`pkgName`, pkgVersion, _)) =>
        (pkgId, pkgVersion)
      }
      .filter { case (_, version) => pkgVersion < version }
      .minByOption { case (_, version) => version }
      .traverse { case (pId, version) =>
        lookupDar(pId, upgradingPackagesMap).map(
          _.map { case (pId, pkg) => (PkgIdWithNameAndVersion(pId, pkgName, version), pkg) }
            .getOrElse {
              val errorMessage = s"failed to look up dar of package $pId present in package map"
              logger.error(errorMessage)
              throw new IllegalStateException(errorMessage)
            }
        )
      }
  }

  private def maximalVersionedDar(
      packageMetadata: Ast.PackageMetadata,
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion, Option[LanguageVersion])],
      upgradingPackagesMap: Map[Ref.PackageId, Ast.Package],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[(PkgIdWithNameAndVersion, Ast.Package)]] = {
    val pkgName = packageMetadata.name
    val pkgVersion = packageMetadata.version
    packageMap
      .collect { case (pkgId, (`pkgName`, pkgVersion, _)) =>
        (pkgId, pkgVersion)
      }
      .filter { case (_, version) => pkgVersion > version }
      .maxByOption { case (_, version) => version }
      .traverse { case (pId, version) =>
        lookupDar(pId, upgradingPackagesMap)(loggingContext).map(
          _.map { case (pId, pkg) => (PkgIdWithNameAndVersion(pId, pkgName, version), pkg) }
            .getOrElse {
              val errorMessage = s"failed to look up dar of package $pId present in package map"
              logger.error(errorMessage)
              throw new IllegalStateException(errorMessage)
            }
        )
      }
  }

  private def strictTypecheckUpgrades(
      phase: TypecheckUpgrades.UploadPhaseCheck[(PkgIdWithNameAndVersion, Ast.Package)],
      packageMap: PackageMap,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] =
    LoggingContextWithTrace
      .withEnrichedLoggingContext("upgradeTypecheckPhase" -> OfString(phase.toString)) {
        implicit loggingContext =>
          phase match {
            case TypecheckUpgrades.MaximalDarCheck((oldPkgId, _), (newPkgId, _)) => logger.info(s"Package $newPkgId claims to upgrade package id $oldPkgId")
            case TypecheckUpgrades.MinimalDarCheck((oldPkgId, _), (newPkgId, _)) => logger.info(s"Package $newPkgId claims to upgrade package id $oldPkgId")
            case _ => ()
          }
          EitherT(
            Future(
              TypecheckUpgrades(loggerFactory)
                .typecheckUpgrades(
                  packageMap,
                  phase,
                )
                .toEither
            )
          ).leftMap[DamlError] {
            case err: UpgradeError =>
              Validation.Upgradeability.Error(
                upgradeError = err,
                phase = phase.map(_._1),
              )
            case unhandledErr =>
              InternalError.Unhandled(
                unhandledErr,
                Some(s"Typechecking upgrades for ${phase.uploadedPackage._1} failed with unknown error."),
              )
          }
      }

  private def typecheckUpgrades(
      typecheckPhase: TypecheckUpgrades.UploadPhaseCheck[Option[(PkgIdWithNameAndVersion, Ast.Package)]],
      packageMap: PackageMap,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[Future, DamlError, Unit] =
    typecheckPhase.sequenceOptional match {
      case None => EitherT.rightT(())
      case Some(phase) =>
        strictTypecheckUpgrades(
          phase,
          packageMap,
        )
    }

  private def lfVersionSupportsUpgrades(pkg: Ast.Package): Boolean =
    pkg.languageVersion >= LanguageVersion.Features.smartContractUpgrade

  private def supportsUpgrades(pkg: Ast.Package): Boolean =
    lfVersionSupportsUpgrades(pkg) && !pkg.isUtilityPackage

  def warnDamlScriptUpload(
      mainPackage: (Ref.PackageId, Ast.Package),
      allPackages: List[(Ref.PackageId, Ast.Package)],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Unit = {
    def isDamlScript(pkgMeta: Ast.PackageMetadata): Boolean = pkgMeta.name match {
      case "daml-script" | "daml3-script" | "daml-script-lts" | "daml-script-lts-stable" => true
      case _ => false
    }
    val mainPackageSupportsUpgrades = lfVersionSupportsUpgrades(mainPackage._2)

    allPackages.foreach { case (pkgId, pkg) =>
      pkg.metadata match {
        case None =>
        case Some(pkgMetadata) =>
          val pkgSupportsUpgrades = lfVersionSupportsUpgrades(pkg)
          if ((pkgSupportsUpgrades || mainPackageSupportsUpgrades) && isDamlScript(pkgMetadata)) {
            val mainPackageLabel =
              s"${if (mainPackageSupportsUpgrades) s"LF${mainPackage._2.languageVersion.pretty} "
                else ""}package ${mainPackage._1}"
            val damlScriptPackageLabel =
              s"${if (pkgSupportsUpgrades) s"LF${pkg.languageVersion.pretty} "
                else ""}${pkgMetadata.name} ($pkgId)"
            logger.warn(
              s"Upload of $mainPackageLabel contains $damlScriptPackageLabel as a dependency. " +
                "Avoid uploading daml-script to the ledger when using Smart Contract Upgrades"
            )
          }
      }
    }
  }
}
