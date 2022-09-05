// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.packages

import java.io.File
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.lf.archive
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Time.Timestamp
import org.slf4j.LoggerFactory
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

import scala.collection.immutable.Map
import scala.concurrent.Future

private[platform] object InMemoryPackageStore {
  def empty: InMemoryPackageStore = new InMemoryPackageStore(Map.empty, Map.empty, Map.empty)
}

private[platform] case class InMemoryPackageStore(
    packageInfos: Map[PackageId, PackageDetails],
    packages: Map[PackageId, Ast.Package],
    archives: Map[PackageId, DamlLf.Archive],
) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    Future.successful(listLfPackagesSync())

  def listLfPackagesSync(): Map[PackageId, PackageDetails] =
    packageInfos

  def getLfArchive(packageId: PackageId): Future[Option[DamlLf.Archive]] =
    Future.successful(getLfArchiveSync(packageId))

  def getLfArchiveSync(packageId: PackageId): Option[DamlLf.Archive] =
    archives.get(packageId)

  def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    Future.successful(getLfPackageSync(packageId))

  def getLfPackageSync(packageId: PackageId): Option[Ast.Package] =
    packages.get(packageId)

  def withPackages(
      knownSince: Timestamp,
      sourceDescription: Option[String],
      packages: List[DamlLf.Archive],
  ): Either[String, InMemoryPackageStore] =
    addArchives(knownSince, sourceDescription, packages)

  def withDarFile(
      knownSince: Timestamp,
      sourceDescription: Option[String],
      file: File,
  ): Either[String, InMemoryPackageStore] =
    for {
      dar <- archive.DarParser
        .readArchiveFromFile(file)
        .left
        .map(t => s"Failed to parse DAR from $file: $t")
      packages <- addArchives(knownSince, sourceDescription, dar.all)
    } yield packages

  private[InMemoryPackageStore] def addPackage(
      pkgId: PackageId,
      details: PackageDetails,
      archive: DamlLf.Archive,
      pkg: Ast.Package,
  ): InMemoryPackageStore =
    packageInfos.get(pkgId) match {
      case None =>
        new InMemoryPackageStore(
          packageInfos + (pkgId -> details),
          packages + (pkgId -> pkg),
          archives + (pkgId -> archive),
        )
      case Some(oldDetails) =>
        // Note: we are discarding the new metadata (size, known since, source description)
        logger.debug(
          s"Ignoring duplicate upload of package $pkgId. Existing package: $oldDetails, new package: $details"
        )
        this
    }

  private def addArchives(
      knownSince: Timestamp,
      sourceDescription: Option[String],
      archives: List[DamlLf.Archive],
  ): Either[String, InMemoryPackageStore] =
    archives
      .traverse(proto =>
        try {
          Right((proto, archive.Decode.assertDecodeArchive(proto)._2))
        } catch {
          case err: archive.Error => Left(s"Could not parse archive ${proto.getHash}: $err")
        }
      )
      .map(pkgs =>
        pkgs.foldLeft(this) { case (store, (archive, pkg)) =>
          val pkgId = PackageId.assertFromString(archive.getHash)
          val details =
            PackageDetails(archive.getPayload.size.toLong, knownSince, sourceDescription)
          store.addPackage(pkgId, details, archive, pkg)
        }
      )

}
